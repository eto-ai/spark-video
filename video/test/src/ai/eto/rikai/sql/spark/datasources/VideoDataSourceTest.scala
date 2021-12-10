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

import org.apache.spark.sql.{DataFrame, Row}

class VideoDataSourceTest extends SparkSessionSuite {

  ignore("schema of video data source") {
    // TODO: figure out why nullable turned from false to true
    assert(
      rabbitFrames.schema === VideoSchema.columnSchema
    )
  }

  test("option: fps") {
    val df = rabbitFrames.where("date_format(ts, 'mm:ss') = '00:01'")
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

  test("option: imageWidth, imageHeight") {
    def firstRow(df: DataFrame): Row = {
      import spark.implicits._
      df.selectExpr("ml_image(image_data) as image")
        .withColumn("width", $"image.width")
        .withColumn("height", $"image.height")
        .head()
    }
    val originalRow = firstRow(rabbitFrames)
    assert(originalRow.getAs[Int]("width") == 640)
    assert(originalRow.getAs[Int]("height") == 360)

    val widthScaledRow = firstRow {
      spark.read
        .format("video")
        .option("imageWidth", 320)
        .load(localVideo)
    }
    assert(widthScaledRow.getAs[Int]("height") === 360)

    val heightScaledRow = firstRow {
      spark.read
        .format("video")
        .option("imageHeight", 180)
        .load(localVideo)
    }
    assert(heightScaledRow.getAs[Int]("width") === 640)
  }
}
