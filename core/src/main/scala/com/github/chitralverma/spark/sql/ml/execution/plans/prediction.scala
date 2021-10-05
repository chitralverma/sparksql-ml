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

package com.github.chitralverma.spark.sql.ml.execution.plans

import com.github.chitralverma.spark.sql.ml._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSqlUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.functions.col

import scala.language.existentials

/**
 * Logical plan to generate predictions for a given input data set.
 *
 * @param dataSetQuery [[LogicalPlan]] for the input data set
 * @param model instance of an [[MLModel]] which is used to generate predictions
 * @param modelAttrs Extra attributes that the [[MLModel]] adds to the input data set as predictions
 */
final case class Prediction(
  dataSetQuery: LogicalPlan,
  model: MLModel,
  modelAttrs: Seq[Attribute])
    extends UnaryNode {

  override def maxRows: Option[Long] = child.maxRows

  override def child: LogicalPlan = dataSetQuery

  override def references: AttributeSet = AttributeSet(dataSetQuery.flatMap(_.references))

  override def output: Seq[Attribute] = child.output ++ modelAttrs

}

/**
 * Physical plan to generate predictions for a given input data set.
 *
 * @param cmd [[LogicalPlan]] to generate predictions for a given input data set
 * @param child Physical Plan for the input data set
 */
final case class PredictionExec(cmd: Prediction, child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute"

  override protected def doExecute(): RDD[InternalRow] = {
    // Get data frame from Spark plan
    val dataSet = SparkSqlUtils.getDataFrameFromSparkPlan(
      sqlContext.sparkSession,
      child,
      cmd.dataSetQuery.isStreaming)
    output.map(_.dataType)

    val (colNames, outputTypes) = output.map(a => (col(a.name), a.dataType)).unzip
    val predictions = cmd.model.transform(dataSet)

    // Align columns correctly as per output attributes and convert RDD
    RDDConversions.rowToRowRdd(predictions.select(colNames: _*).rdd, outputTypes)
  }

}
