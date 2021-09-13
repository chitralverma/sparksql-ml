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

package com.github.chitralverma.spark.sql.ml.execution.utils

import com.github.chitralverma.spark.sql.ml._
import com.github.chitralverma.spark.sql.ml.execution.utils.ReflectionUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param._

import scala.util.{Failure, Success, Try}

object TrainingUtils extends Logging {

  private val trainEstimators: Set[MLClassRecord] = allEstimators.filter(_.isTrain)

  def getTrainEstimatorClass(className: String): Class[MLEstimator] =
    getClassOf(className, fromSet = trainEstimators)

  def getEstimatorToFit(className: String, params: Map[String, String]): MLEstimator = {
    val cls = getTrainEstimatorClass(className)
    val estimator = cls.getConstructor().newInstance()

    val estimatorParams: Map[String, ParamPair[_]] = estimator
      .extractParamMap()
      .toSeq
      .map(paramPair => paramPair.param.name -> paramPair)
      .toMap

    val paramsToSet = params
      .map(kv => (estimatorParams.get(kv._1), kv))
      .flatMap {
        case (Some(paramPair), (_, valueToSet)) =>
          Some(updateParamPair(paramPair, valueToSet))

        case (None, (key, _)) =>
          logWarning(
            s"Omitting parameter with name '$key' as it is not applicable " +
              s"for estimator '${cls.getSimpleName}'.")
          Option.empty[ParamPair[_]]
      }
      .toSeq

    val estimatorWithParams =
      if (paramsToSet.isEmpty) estimator else estimator.copy(ParamMap(paramsToSet: _*))

    estimatorWithParams
  }

  private def updateParamPair(
    initialParamPair: ParamPair[_],
    valueToSet: String): ParamPair[_] = {
    val param: Param[_] = initialParamPair.param
    val defaultValue = initialParamPair.value

    val updatedParamPair: ParamPair[_] = Try(param match {
      case p: DoubleParam => p -> valueToSet.toDouble
      case p: IntParam => p -> valueToSet.toInt
      case p: FloatParam => p -> valueToSet.toFloat
      case p: LongParam => p -> valueToSet.toLong
      case p: BooleanParam => p -> valueToSet.toBoolean
      case p: IntArrayParam => p -> valueToSet.split(",").map(_.toInt)
      case p: DoubleArrayParam => p -> valueToSet.split(",").map(_.toDouble)
      case p: StringArrayParam => p -> valueToSet.split(",")
      case p => p.asInstanceOf[Param[String]] -> valueToSet
    }) match {
      case Success(value) =>
        logDebug(
          s"Setting new value '$valueToSet' for parameter with name '${param.name}' " +
            s"and type '${param.getClass.getSimpleName}'.")
        value
      case Failure(exception: ClassCastException) =>
        logWarning(
          s"Unable to cast the provided value '$valueToSet' for parameter " +
            s"with name '${param.name}' and type '${param.getClass.getSimpleName}'." +
            s" Using parameter's default value: $defaultValue.",
          exception)
        initialParamPair
      case Failure(exception) =>
        logWarning(
          s"Unable to set the provided value '$valueToSet' for parameter " +
            s"with name '${param.name}' and type '${param.getClass.getSimpleName}'." +
            s" Using parameter's default value: $defaultValue.",
          exception)
        initialParamPair
    }

    updatedParamPair
  }

}
