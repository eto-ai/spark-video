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
}
