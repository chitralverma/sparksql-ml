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
import io.github.classgraph.{ClassGraph, ScanResult}
import io.netty.util.internal.StringUtil
import org.apache.spark.sql.SparkSqlUtils

import scala.collection.JavaConverters._
import scala.reflect._
import scala.util._

object ReflectionUtils {

  private val scannedClasses: ScanResult = new ClassGraph().enableClassInfo().scan()

  val allEstimators: Set[MLClassRecord] = scannedClasses
    .getSubclasses(MLEstimatorCls)
    .asScala
    .filterNot(_.isAbstract)
    .map(x => MLClassRecord(x.loadClass()))
    .toSet

  val allModels: Set[MLClassRecord] = scannedClasses
    .getSubclasses(MLModelCls)
    .asScala
    .filterNot(_.isAbstract)
    .map(x => MLClassRecord(x.loadClass()))
    .toSet

  def getClassOf[T: ClassTag](
    className: String,
    fromSet: Set[MLClassRecord]): Class[T] = {
    Try {
      assert(
        !StringUtil.isNullOrEmpty(className),
        s"Invalid value '$className' provided as class name.")

      val cls = fromSet.find(_.className.equalsIgnoreCase(className)) match {
        case Some(ed) => ed.cls
        case None => SparkSqlUtils.sparkUtils.classForName(className)
      }

      cls.asInstanceOf[Class[T]]
    } match {
      case Success(cls) => cls
      case Failure(e: ClassCastException) =>
        throw new IllegalStateException(
          s"Provided class '$className' could not be cast as " +
            s"${classFor[T].getSimpleName}.",
          e)
      case Failure(e: ClassNotFoundException) =>
        throw new IllegalArgumentException(
          s"Provided value for class '$className' was not found on the classpath.",
          e)
      case Failure(e) =>
        throw new IllegalArgumentException(
          s"Unable to get class for provided value '$className'.",
          e)
    }
  }

}
