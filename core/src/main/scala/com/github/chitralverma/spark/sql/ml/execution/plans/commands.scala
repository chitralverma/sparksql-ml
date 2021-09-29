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

import com.fasterxml.jackson.databind.`type`.TypeFactory
import com.github.chitralverma.spark.sql.ml.execution.utils.TrainingUtils
import com.github.chitralverma.spark.sql.ml.MLEstimator
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{BooleanType, StringType}

import java.util.Locale
import scala.util.{Failure, Success, Try}

/**
 * A command for users to get a list of all available/ supported estimators.
 * If the command is run with optional keyword EXTENDED, some extended information
 * regarding the estimator is also returned.
 *
 * The syntax of using this command in SQL is: SHOW ESTIMATOR LIST [EXTENDED];
 * @example
 * {{{
 *   SHOW ESTIMATOR LIST [EXTENDED];
 * }}}
 */
final case class ShowEstimatorListCommand(isExtended: Boolean) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val extendedInfo = if (isExtended) {
      AttributeReference("estimator_class", StringType, nullable = false)() ::
        AttributeReference("type", StringType, nullable = false)() ::
        AttributeReference("is_internal", BooleanType, nullable = false)() :: Nil
    } else {
      Nil
    }

    AttributeReference("estimator", StringType, nullable = false)() :: extendedInfo
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    TrainingUtils.trainEstimatorClasses.map { c =>
      val estimator = c.className
      val estimatorClass = c.cls.getCanonicalName
      val estimatorType = c.classType.getOrElse("others")
      val isInternal = c.isInternal

      if (isExtended) {
        Row(estimator, estimatorClass, estimatorType, isInternal)
      } else {
        Row(estimator)
      }
    }.toSeq
  }
}

/**
 * A command for users to get params for a given estimator class.
 * Estimator class is a mandatory input and must not be null or empty and
 * the class must be available on classpath.
 *
 * The syntax of using this command in SQL is: SHOW PARAMS FOR ESTIMATOR '<estimator>';
 * @example
 * {{{
 *   SHOW PARAMS FOR ESTIMATOR 'LinearRegression';
 *   SHOW PARAMS FOR ESTIMATOR 'LDA';
 *   SHOW PARAMS FOR ESTIMATOR 'ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier';
 * }}}
 */
final case class ShowEstimatorParamsCommand(estimatorClass: Class[MLEstimator])
    extends RunnableCommand {

  override val output: Seq[Attribute] = {
    AttributeReference("param_name", StringType, nullable = false)() ::
      AttributeReference("param_type", StringType, nullable = false)() ::
      AttributeReference("param_description", StringType, nullable = true)() ::
      AttributeReference("default_value", StringType, nullable = true)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val estimator: MLEstimator =
      TrainingUtils.getEstimatorToFit(estimatorClass, Map.empty[String, String])

    val params: Map[String, Param[_]] =
      estimator.params.map(p => s"get${p.name}".toLowerCase(Locale.ROOT) -> p).toMap

    estimator.getClass.getMethods
      .map(m => (m, params.get(m.getName.toLowerCase(Locale.ROOT))))
      .flatMap(m => m._2.map(x => (x, m._1)))
      .flatMap {
        case (param, method) =>
          val paramValue = estimator.getDefault(param).map(_.toString).orNull
          val paramType =
            Try(TypeFactory.rawClass(method.getGenericReturnType).getSimpleName) match {
              case Failure(_) => "Object"
              case Success(value) => value
            }

          Some(Row(param.name, paramType, param.doc, paramValue))

        case _ => None
      }
  }
}
