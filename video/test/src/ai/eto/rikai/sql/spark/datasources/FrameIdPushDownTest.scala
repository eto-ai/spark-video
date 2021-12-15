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

class FrameIdPushDownTest extends SparkSessionSuite {
  def generateRows(expr: String): (Array[Row], Array[Row]) = {
    val df1 = spark
      .sql(s"""
              |select *
              |from video.`${localVideo}`
              |where ${expr}
              |""".stripMargin)
    val df2 = spark.read
      .format("video")
      .load(localVideo)
      .filter(expr)
    (df1.collect(), df2.collect())
  }

  def assertRows(rows1: Array[Row], rows2: Array[Row], size: Int): Unit = {
    assert(rows1 === rows2)
    assert(rows1.length === size)
  }

  test("frame_id = 3") {
    val (rows1, rows2) = generateRows("frame_id = 3")
    assertRows(rows1, rows2, 1)
    assert(rows1.head.getAs[Long]("frame_id") === 3)
  }

  test("frame_id < 3") {
    val (rows1, rows2) = generateRows("frame_id < 3")
    assertRows(rows1, rows2, 3)
  }

  test("frame_id <= 3") {
    val (rows1, rows2) = generateRows("frame_id <= 3")
    assertRows(rows1, rows2, 4)
  }

  test("frame_id > 296") {
    val (rows1, rows2) = generateRows("frame_id > 296")
    assertRows(rows1, rows2, 3)
  }

  test("frame_id >= 296") {
    val (rows1, rows2) = generateRows("frame_id >= 296")
    assertRows(rows1, rows2, 4)
  }

  test("between 10 and 13") {
    val (rows1, rows2) = generateRows("frame_id > 10 and frame_id < 13")
    assertRows(rows1, rows2, 2)
  }

  test("in (10, 20 ,30)") {
    val (rows1, rows2) = generateRows("frame_id in (10, 20, 30)")
    assertRows(rows1, rows2, 3)
  }

  test("using two ranges") {
    val (rows1, rows2) = generateRows(
      "(frame_id > 10 and frame_id < 13) or (frame_id > 15 and frame_id < 18)"
    )
    assertRows(rows1, rows2, 4)
  }

  test("frame_id push down with customized stepSize and stepOffset") {
    val df = spark.read
      .format("video")
      .option("frameStepSize", 2)
      .option("frameStepOffset", 1)
      .load(localVideo)
      .filter("frame_id > 10 and frame_id <= 16")
    assert(df.count() === 3)

    spark.sql(s"""
        |create or replace temporary view test_view
        |using video
        |options (
        |  path "$localVideo",
        |  frameStepSize 2
        |)
        |""".stripMargin)

    val df2 = spark.sql("""
          |select * from test_view where frame_id > 10 and frame_id <= 16
          |""".stripMargin)
    assert(df2.count() == 3)
  }
}
