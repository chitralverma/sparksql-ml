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

package org.apache.spark.sql.udt

import com.github.chitralverma.spark.sql.ml._
import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.sql.types._

import java.io._

sealed trait AbstractMLUDT[T >: Null] extends UserDefinedType[T] {

  override def sqlType: DataType = BinaryType

  override def serialize(obj: T): Any = {
    val serializable = obj.as[Serializable]
    SerializationUtils.serialize(serializable)
  }

  override def deserialize(datum: Any): T = {
    datum match {
      case bytes: Array[Byte] => SerializationUtils.deserialize[T](bytes)
    }
  }

}

class MLModelUDT extends AbstractMLUDT[MLModel] {

  override def typeName: String = "ml_model"

  override def userClass: Class[MLModel] = MLModelCls

}

class MLEstimatorUDT extends AbstractMLUDT[MLEstimator] {

  override def typeName: String = "ml_estimator"

  override def userClass: Class[MLEstimator] = MLEstimatorCls

}
