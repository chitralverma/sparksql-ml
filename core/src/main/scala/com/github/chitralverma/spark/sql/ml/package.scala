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

package com.github.chitralverma.spark.sql

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{SparkSession, SparkSqlUtils}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.udt.{MLEstimatorUDT, MLModelUDT}

import scala.language.existentials
import scala.reflect.{classTag, ClassTag}

package object ml {

  final val SparkMLPackage = "org.apache.spark.ml"

  // Type Alias

  // scalastyle:off structural.type
  type MLModel = Model[T] with MLWritable forSome { type T }
  type MLEstimator = Estimator[T] forSome { type T <: MLModel }
  // scalastyle:on structural.type

  // Runtime Class
  val MLModelCls: Class[MLModel] = classFor[MLModel]
  val MLEstimatorCls: Class[MLEstimator] = classFor[MLEstimator]

  // Spark-SQL Data Type
  val MLModelType: DataType = new MLModelUDT
  val MLEstimatorType: DataType = new MLEstimatorUDT

  implicit class SparkSessionImplicits(sparkBuilder: SparkSession.Builder) {

    def withSqlMLExtensions(): SparkSession.Builder = {
      SparkSqlUtils.registerUDTs()

      sparkBuilder.withExtensions(new SparkSqlMLExtensions)
    }

  }

  implicit class ObjectImplicits(obj: Any) {

    // Alias for obj.asInstanceOf[...]
    @SuppressWarnings(Array("AsInstanceOf"))
    def as[T]: T = obj.asInstanceOf[T]

  }

  def classFor[T: ClassTag]: Class[T] = classTag.runtimeClass.as[Class[T]]

}
