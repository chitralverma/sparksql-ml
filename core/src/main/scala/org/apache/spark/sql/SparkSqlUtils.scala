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

package org.apache.spark.sql

import com.github.chitralverma.spark.sql.ml._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.types.{StructType, UDTRegistration}
import org.apache.spark.sql.udt.{MLEstimatorUDT, MLModelUDT}
import org.apache.spark.util.Utils

object SparkSqlUtils {
  val sparkUtils: Utils.type = Utils

  def registerUDTs(): Unit = {
    UDTRegistration.register(MLModelCls.getName, classOf[MLModelUDT].getName)
    UDTRegistration.register(MLEstimatorCls.getName, classOf[MLEstimatorUDT].getName)
  }

  def schemaToAttributes(schema: StructType): Seq[AttributeReference] =
    schema.toAttributes

  def getDataFrameFromLogicalPlan(
    sparkSession: SparkSession,
    plan: LogicalPlan): DataFrame =
    Dataset.ofRows(sparkSession, plan)

  def getDataFrameFromSparkPlan(
    sparkSession: SparkSession,
    plan: SparkPlan,
    isStreaming: Boolean): DataFrame =
    getDataFrameFromLogicalPlan(
      sparkSession,
      LogicalRDD(
        plan.output,
        plan.execute(),
        plan.outputPartitioning,
        plan.outputOrdering,
        isStreaming)(sparkSession))

}
