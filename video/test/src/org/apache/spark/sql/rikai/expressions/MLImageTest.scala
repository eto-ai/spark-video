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

import ai.eto.rikai.sql.spark.datasources.SparkSessionSuite
import org.apache.spark.ml.image.ImageSchema

class MLImageTest extends SparkSessionSuite {
  test("udf: thumbnail") {
    val df = spark.read
      .format("video")
      .option("fps", 5)
      .load(localVideo)
    df.createOrReplaceTempView("frames")
    assert(df.count() === 50)
    val showImage =
      spark.sql("select thumbnail(image_data) as image from frames limit 3")
    assert(showImage.schema("image").dataType === ImageSchema.columnSchema)
    showImage
      .selectExpr("image.origin", "image.height", "image.width")
      .show()
  }
}
