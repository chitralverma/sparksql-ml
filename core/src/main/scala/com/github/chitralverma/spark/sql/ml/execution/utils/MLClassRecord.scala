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
  val classType: MLEnums.MLEnum = getType
  val isInternal: Boolean = pkg.startsWith(SparkMLPackage)
  val isTrain: Boolean = classType match {
    case MLEnums.train | MLEnums.other => true
    case _ => false
  }

  private def getType = pkg.split("\\.").last match {
    case "recommendation" | "regression" | "classification" | "clustering" =>
      MLEnums.train
    case s if s.endsWith("Classifier") || s.endsWith("Regressor") => MLEnums.train
    case "feature" => MLEnums.feature
    case "tuning" => MLEnums.tuning
    case _ => MLEnums.other
  }
}
