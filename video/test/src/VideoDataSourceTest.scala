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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class VideoDataSourceTest extends FunSuite {
  val localVideo = "video/test/resources/big_buck_bunny_short.mp4"
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  test("schema of video data source") {
    val schema = spark.read
      .format("video")
      .load(localVideo)
      .schema
    assert(
      schema === StructType(
        Seq(
          StructField("frame_id", LongType),
          StructField("image_data", BinaryType)
        )
      )
    )
  }

  test("count of video data") {
    val df = spark.read
      .format("video")
      .load(localVideo)
    assert(df.count() === 300)
  }
}
