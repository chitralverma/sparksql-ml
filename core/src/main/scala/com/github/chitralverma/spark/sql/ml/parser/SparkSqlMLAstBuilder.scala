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

import com.github.chitralverma.spark.sql.ml.parser.SparkSqlMLBaseParser._
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.misc.Interval
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.util.Locale
import scala.collection.JavaConverters._

/**
 * @groupname estimator_creation Visitor Functions for Estimator Creation
 * @param delegate original SparkSql parser to fallback on for non-ML queries
 */
class SparkSqlMLAstBuilder(val delegate: ParserInterface)
    extends SparkSqlMLBaseBaseVisitor[AnyRef]
    with Logging {

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Handling Delegation to default SparkSqlParser
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Visit a parse tree produced by a SparkSQL query.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  override def visitSparkSQLStatement(ctx: SparkSQLStatementContext): LogicalPlan =
    visit(ctx.statement()).asInstanceOf[LogicalPlan]

  /**
   * Visit a parse tree produced by a SparkSQL ML query.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  override def visitSparkSQLMLStatement(ctx: SparkSQLMLStatementContext): LogicalPlan =
    visit(ctx.mlStatement()).asInstanceOf[LogicalPlan]

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

  //////////////////////////////////////////////////////////////////////////////////////////////
  // Handling SparkSql-ML queries
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Visit a parse tree produced by `SparkSqlMLBaseParser.fitEstimatorHeader`.
   * Deals with provided values for `org.apache.spark.ml.Estimator` and `SaveMode`
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  override def visitFitEstimatorHeader(
    ctx: FitEstimatorHeaderContext): (Boolean, String) = {
    val shouldOverwrite = Option(ctx.overwrite).isDefined
    val estimatorName = Option(ctx.estimator) match {
      case None =>
        operationNotAllowed(
          "Malformed input provided for estimator name. Null or Empty values are not allowed.",
          ctx)
      case Some(value) => string(value)
    }

    (shouldOverwrite, estimatorName)
  }

  /**
   * Visitor method to create a ML estimator with default hyper parameters.
   *
   * @groupname estimator_creation
   * @param ctx the parse tree
   */
  override def visitFitEstimator(ctx: FitEstimatorContext): LogicalPlan =
    withOrigin(ctx)(operationNotAllowed("Not Implemented", ctx))

  implicit class ParserRuleContextImplicits(ctx: ParserRuleContext) {

    def getOriginalString: String = {
      val parentInputStream = ctx.getParent.start.getInputStream
      val startIndex = ctx.start.getStartIndex
      val stopIndex = ctx.stop.getStopIndex

      parentInputStream.getText(Interval.of(startIndex, stopIndex))
    }

  }

}
