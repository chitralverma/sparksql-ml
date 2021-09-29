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

import com.github.chitralverma.spark.sql.ml.SparkMLPackage

object MLEnums extends Enumeration {
  type MLEnum = Value

  val train, predict, tuning, feature, other = Value
}

final case class MLClassRecord(cls: Class[_]) {
  private val pkg = cls.getPackage.getName

  val className: String = cls.getSimpleName
  val (classType, classCategory) = getTypeAndCategory
  val isInternal: Boolean = pkg.startsWith(SparkMLPackage)
  val isTrain: Boolean = classCategory match {
    case MLEnums.train | MLEnums.other => true
    case _ => false
  }

  private def getTypeAndCategory = pkg.split("\\.").last match {
    case "feature" => (None, MLEnums.feature)
    case "tuning" => (None, MLEnums.tuning)
    case t @ ("recommendation" | "regression" | "classification" | "clustering") =>
      (Some(t), MLEnums.train)
    case _ if className.endsWith("Classifier") || className.endsWith("Classification") =>
      (Some("classification"), MLEnums.train)
    case _ if className.endsWith("Regressor") || className.endsWith("Regression") =>
      (Some("regression"), MLEnums.train)
    case _ => (None, MLEnums.other)
  }

}
