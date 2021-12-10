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

class VideoDataSourceTest extends SparkSessionSuite {

  ignore("schema of video data source") {
    val schema = spark.read
      .format("video")
      .load(localVideo)
      .schema
    // TODO: figure out why nullable turned from false to true
    assert(
      schema === VideoSchema.columnSchema
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
}
