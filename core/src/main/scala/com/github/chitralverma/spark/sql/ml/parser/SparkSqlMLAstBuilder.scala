/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 *   Copyright (2021) Chitral Verma
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.chitralverma.spark.sql.ml.parser

import com.github.chitralverma.spark.sql.ml._
import com.github.chitralverma.spark.sql.ml.execution.plans.Training
import com.github.chitralverma.spark.sql.ml.execution.utils.TrainingUtils.getTrainEstimatorClass
import com.github.chitralverma.spark.sql.ml.parser.SparkSqlMLBaseParser._
import io.netty.util.internal.StringUtil
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSqlUtils
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.types._

import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * @groupname estimator_creation Visitor Functions for Estimator Creation
 * @param delegate original SparkSql parser to fallback on for non-ML queries
 */
class SparkSqlMLAstBuilder(val delegate: ParserInterface)
    extends SparkSqlMLBaseBaseVisitor[AnyRef]
    with Logging {

  protected def typedVisit[T](ctx: ParseTree): T = ctx.accept(this).as[T]

  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Handling Delegation to default SparkSqlParser
  //////////////////////////////////////////////////////////////////////////////////////////////
  /**
   * Override the default behavior for all visit methods. This will only return a non-null result
   * when the context has only one child. This is done because there is no generic method to
   * combine the results of the context children. In all other cases null is returned.
   */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }

  /**
   * Enables support of CTEs with SparkSQLML queries and the provided dataset queries.
   * Create [[With]] logical plan(s) which can alias the results of whole ML Query(-ies) or
   * it can allow alias data sets for one or more ML queries.
   *
   * Some Examples:
   * @example
   * {{{ WITH DATA AS (<data set query>) FIT ... TO (SELECT * FROM DATA) ... }}}
   * {{{ FIT ... TO (WITH DATA AS (<data set query>) SELECT * FROM DATA) ... }}}
   * {{{ WITH MODEL AS ( FIT ... TO (SELECT * FROM DATA) ... ) SELECT * FROM MODEL }}}
   */
  override def visitMlQuery(ctx: MlQueryContext): LogicalPlan =
    withOrigin(ctx) {
      val query = delegate.parsePlan(ctx.statement().getOriginalString)

      // Apply CTEs
      query.optional(ctx.mlCtes()) {
        val ctes = ctx.mlCtes.mlNamedQuery().asScala.map { nCtx =>
          val namedQuery = visitMlNamedQuery(nCtx)

          (namedQuery.alias, namedQuery)
        }

        // Check for duplicate names.
        checkDuplicateKeys(ctes, ctx)
        With(query, ctes)
      }
    }

  override def visitNamedQuery(ctx: NamedQueryContext): SubqueryAlias = withOrigin(ctx) {
    SubqueryAlias(ctx.name.getText, delegate.parsePlan(ctx.query().getOriginalString))
  }

  /**
   * Creates a [[SubqueryAlias]] for a given aliased (named) query.
   */
  override def visitMlNamedQuery(ctx: MlNamedQueryContext): SubqueryAlias =
    withOrigin(ctx) {
      SubqueryAlias(ctx.name.getText, plan(ctx.mlStatement()))
    }

  /**
   * Create an [[ExplainCommand]] logical plan.
   * The syntax of using this command in SQL is:
   * {{{ EXPLAIN (EXTENDED | CODEGEN) <SparkSQLML Statement> }}}
   */
  override def visitExplainMLStatement(ctx: ExplainMLStatementContext): LogicalPlan =
    withOrigin(ctx) {
      Option(ctx.FORMATTED).foreach(_ => operationNotAllowed("EXPLAIN FORMATTED", ctx))
      Option(ctx.LOGICAL).foreach(_ => operationNotAllowed("EXPLAIN LOGICAL", ctx))

      Option(plan(ctx.mlStatement()))
        .map(stmt =>
          ExplainCommand(
            logicalPlan = stmt,
            extended = ctx.EXTENDED != null,
            codegen = ctx.CODEGEN != null,
            cost = ctx.COST != null))
        .orNull
    }

  /**
   * A table property key can either be String or a collection of dot separated elements. This
   * function extracts the property key based on whether its a string literal or a table property
   * identifier.
   */
  override def visitTablePropertyKey(key: TablePropertyKeyContext): String = {
    key.STRING() match {
      case null => key.getText
      case node => string(node)
    }
  }

  /**
   * A table property value can be String, Integer, Boolean or Decimal. This function extracts
   * the property value based on whether its a string, integer, boolean or decimal literal.
   */
  override def visitTablePropertyValue(value: TablePropertyValueContext): String = {
    value match {
      case null => null
      case v if Option(v.STRING()).isDefined => string(value.STRING)
      case v if Option(v.booleanValue()).isDefined =>
        value.getText.toLowerCase(Locale.ROOT)
      case v => v.getText
    }
  }

  /**
   * Convert a table property list into a key-value map.
   */
  override def visitTablePropertyList(
    ctx: TablePropertyListContext): Map[String, String] =
    withOrigin(ctx) {
      val properties = ctx.tableProperty.asScala.map { property =>
        val key = visitTablePropertyKey(property.key)
        val value = visitTablePropertyValue(property.value)
        key -> value
      }
      // Check for duplicate property names.
      checkDuplicateKeys(properties, ctx)
      properties.toMap
    }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  private def visitPropertyKeyValues(
    ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.collect { case (key, null) => key }
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}",
        ctx)
    }
    props
  }

  /**
   * Create a logical plan for a regular (no-insert) query
   */
  override def visitQueryNoInsert(queryCtx: QueryNoInsertContext): LogicalPlan = {
    val dataSetQueryStr = Try(queryCtx.getOriginalString) match {
      case Failure(exception) =>
        operationNotAllowed(
          s"Invalid SQL query provided. Failed with message '${exception.getMessage}'",
          queryCtx.getParent)
      case Success(value) if StringUtil.isNullOrEmpty(value) =>
        operationNotAllowed(
          "Invalid SQL query provided. Null or Empty values are not allowed.",
          queryCtx.getParent)
      case Success(value) if Option(value).isDefined => value
    }

    logDebug("Delegating the provided dataset query to SparkSQLParser.")
    delegate.parsePlan(dataSetQueryStr)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Handling SparkSql-ML queries
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Visitor method to create a ML estimator with default hyper parameters.
   *
   * @groupname estimator_creation
   * @param ctx the parse tree
   */
  override def visitFitEstimator(ctx: FitEstimatorContext): LogicalPlan =
    withOrigin(ctx) {
      // Get estimator type as string
      val estimatorType = Try(string(ctx.estimator)).toOption match {
        case null | Some(StringUtil.EMPTY_STRING) | None =>
          operationNotAllowed(
            "Malformed input provided for estimator. Null or Empty values are not allowed.",
            ctx)
        case Some(value) => value
      }

      // Get class for provided estimator type string
      val estimatorCls = Try(getTrainEstimatorClass(estimatorType)) match {
        case Success(cls) => cls
        case Failure(exception) => throw exception
      }

      // Get provided query as logical plan with CTEs
      val dataset = visitQueryNoInsert(ctx.dataSetQuery).optionalMap(ctx.ctes()) {
        (ctesContext: CtesContext, datasetQuery: LogicalPlan) =>
          val ctes = ctesContext.namedQuery().asScala.map { nCtx =>
            val namedQuery = visitNamedQuery(nCtx)
            (namedQuery.alias, namedQuery)
          }

          // Check for duplicate names.
          checkDuplicateKeys(ctes, ctx)
          With(datasetQuery, ctes)
      }

      // Get any parameters or options if provided
      val params = Option(ctx.params).map(visitPropertyKeyValues).getOrElse(Map.empty)

      // Get write-specs (should overwrite, write location etc.) if defined

      val writeSpecsOpt = if (Option(ctx.writeAtLocation).isDefined) {
        // Check if save mode is overwrite
        val shouldOverwrite = Try(
          ctx.writeAtLocation.writeMode.getText.toLowerCase(Locale.ROOT)).toOption match {
          case Some(value) if "overwrite".equalsIgnoreCase(value) => true
          case _ => false
        }

        // Get value for location
        val location = Try(string(ctx.writeAtLocation.locationSpec().STRING())) match {
          case Failure(exception) =>
            operationNotAllowed(
              "Malformed input provided for location. " +
                s"Failed with message '${exception.getMessage}'",
              ctx)
          case Success(value) if StringUtil.isNullOrEmpty(value) =>
            operationNotAllowed(
              "Malformed input provided for location. Null or Empty values are not allowed.",
              ctx)
          case Success(value) => value
        }

        Some((shouldOverwrite, location))
      } else None

      // Fixed output schema for Training queries
      val schema = new StructType()
        .add("estimator", MLEstimatorType, nullable = false)
        .add("model", MLModelType, nullable = false)
        .add("model_location", StringType, nullable = true)
        .add("model_params", MapType(StringType, StringType), nullable = false)

      val outputAttributes = SparkSqlUtils.schemaToAttributes(schema)
      Training(estimatorCls, dataset, writeSpecsOpt, params, outputAttributes)
    }

  implicit class ParserRuleContextImplicits(ctx: ParserRuleContext) {

    def getOriginalString: String = {
      val parentInputStream = ctx.getParent.start.getInputStream
      val startIndex = ctx.start.getStartIndex
      val stopIndex = ctx.stop.getStopIndex

      parentInputStream.getText(Interval.of(startIndex, stopIndex))
    }

  }

}
