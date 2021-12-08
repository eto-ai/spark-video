/*
 * Copyright 2021 Rikai authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rikai.udf

import org.apache.spark.ml.image.ImageSchema

case class MLImage(
    origin: String,
    height: Int,
    width: Int,
    nChannels: Int,
    mode: Int,
    data: Array[Byte]
)

object MLImage {
  def evaluate(bytes: Array[Byte]): MLImage = {
    ImageSchema.decode("", bytes) match {
      case Some(row) =>
        val inner = row.getStruct(0)
        MLImage(
          origin = inner.getString(0),
          height = inner.getInt(1),
          width = inner.getInt(2),
          nChannels = inner.getInt(3),
          mode = inner.getInt(4),
          data = inner.getAs[Array[Byte]](5)
        )
      case None =>
        null
    }
  }
}
