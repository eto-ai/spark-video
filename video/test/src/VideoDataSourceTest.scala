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

package ai.eto.rikai.sql.spark.datasources

import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class VideoDataSourceTest extends FunSuite {
  val localVideo = "video/test/resources/big_buck_bunny_short.mp4"
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .config(
      "spark.sql.extensions",
      "ai.eto.rikai.sql.spark.SparkVideoExtensions"
    )
    .getOrCreate()

  test("schema of video data source") {
    val schema = spark.read
      .format("video")
      .load(localVideo)
      .schema
    assert(
      schema === StructType(
        Seq(
          StructField("video_uri", StringType),
          StructField("frame_id", LongType),
          StructField("ts", TimestampType),
          StructField("image_data", BinaryType)
        )
      )
    )
  }

  test("count of video data") {
    val df = spark.read
      .format("video")
      .option("fps", 5)
      .load(localVideo)
    assert(df.count() === 50)
  }

  test("udf: ml_image") {
    val df = spark.read
      .format("video")
      .option("fps", 5)
      .load(localVideo)
    df.createOrReplaceTempView("frames")
    spark.sqlContext.cacheTable("frames")
    assert(df.count() === 50)
    val showImage =
      spark.sql("select ml_image(image_data) as show_image from frames limit 3")
    assert(showImage.schema("show_image").dataType === ImageSchema.imageSchema)
    assert(showImage.count() == 3)
  }
}
