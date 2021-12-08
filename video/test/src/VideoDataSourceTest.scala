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

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


class VideoDataSourceTest extends FunSuite {
  val root = LoggerFactory.getLogger("root").asInstanceOf[Logger]
  root.setLevel(Level.INFO)

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
}
