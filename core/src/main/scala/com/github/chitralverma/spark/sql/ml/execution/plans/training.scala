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

import io.netty.util.internal.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.metric.SQLMetric

final case class Training(
  estimatorStr: String,
  dataSetQuery: LogicalPlan,
  location: String,
  shouldOverwrite: Boolean,
  options: Map[String, String] = Map.empty,
  hyperParams: Map[String, String] = Map.empty,
  output: Seq[Attribute])
    extends UnaryNode {

  lazy val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics

  override def references: AttributeSet = AttributeSet(dataSetQuery.flatMap(_.references))

  override def child: LogicalPlan = dataSetQuery

  assert(dataSetQuery != null, "Provided data set query could not be parsed.")

  assert(options.nonEmpty, "No options (featuresCol, labelCol, etc.) were provided.")

  assert(
    !StringUtil.isNullOrEmpty(estimatorStr),
    s"Invalid string '$estimatorStr' provided for Estimator.")

  def run(): Seq[Row] = Row(estimatorStr, location) :: Nil

}

final case class TrainingExec(cmd: Training, child: SparkPlan) extends UnaryExecNode {

  override lazy val metrics: Map[String, SQLMetric] = cmd.metrics

  protected lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run()

    rows.map(converter(_).asInstanceOf[InternalRow])
  }

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = "Execute"

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator: Iterator[InternalRow] = sideEffectResult.toIterator

  override def executeTake(limit: Int): Array[InternalRow] =
    sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] =
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
}
