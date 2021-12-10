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

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.image.ImageSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.util.Properties

class VideoDataSourceTest extends FunSuite with LazyLogging {
  private val level = Level.valueOf {
    Properties.envOrElse("LOG_LEVEL", Level.ERROR.levelStr)
  }
  LoggerFactory
    .getLogger("root")
    .asInstanceOf[Logger]
    .setLevel(level)
  LoggerFactory
    .getLogger("ai.eto.rikai")
    .asInstanceOf[Logger]
    .setLevel(Level.DEBUG)

  private val localVideo = "video/test/resources/big_buck_bunny_short.mp4"
  private val spark = SparkSession
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

  test("option: fps") {
    val df = spark.read
      .format("video")
      .load(localVideo)
      .where("date_format(ts, 'mm:ss') = '00:01'")
    assert(df.count() === 1)

    (1 to 6).foreach { fps =>
      val df = spark.read
        .format("video")
        .option("fps", fps)
        .load(localVideo)
        .where("date_format(ts, 'mm:ss') = '00:00'")
      df.select("frame_id").show()
      assert(df.count() === fps)
    }
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
    assert(showImage.schema("show_image").dataType === ImageSchema.columnSchema)
    spark
      .sql("""
        |from (
        |  from frames
        |  select ml_image(image_data) as result
        |)
        |select
        |  result.origin,
        |  result.height,
        |  result.width
        |limit 3
        |""".stripMargin)
      .show()
  }
}
