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

package org.apache.spark.sql.rikai.expressions

import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  GenericInternalRow,
  ImplicitCastInputTypes,
  NullIntolerant,
  UnaryExpression
}
import org.apache.spark.sql.types.{BinaryType, DataType}

case class ToMLImage(child: Expression)
    extends UnaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes
    with NullIntolerant {

  override def inputTypes = Seq(BinaryType)

  override def nullable: Boolean = true

  override def nullSafeEval(input: Any): Any = {
    ImageSchema.decode("", input.asInstanceOf[Array[Byte]]) match {
      case Some(row) =>
        val encoder = RowEncoder(ImageSchema.columnSchema)
        encoder.createSerializer().apply(row.getStruct(0)).copy()
      case None =>
        null
    }
  }

  override def dataType: DataType = ImageSchema.columnSchema

  override def prettyName: String = "thumbnail"

  /** no explicit override here to keep it working for Apache Spark < 3.2.0
    */
  def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
