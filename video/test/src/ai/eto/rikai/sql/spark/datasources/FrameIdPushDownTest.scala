package ai.eto.rikai.sql.spark.datasources

class FrameIdPushDownTest extends SparkSessionSuite {
  test("frame_id = 10") {
    val rows = spark
      .sql(s"""
        |select *
        |from video.`${localVideo}`
        |where frame_id = 10
        |""".stripMargin)
      .collect()
    assert(rows.length === 1)
    assert(rows.head.getAs[Long]("frame_id") === 10)

    val rows2 = spark.read
      .format("video")
      .load(localVideo)
      .filter("frame_id = 10")
      .collect()
    assert(rows2.length === 1)
    assert(rows2.head.getAs[Long]("frame_id") === 10)

    assert(rows.head === rows2.head)
  }
}
