package ai.eto.rikai.sql.spark.datasources

import org.apache.spark.sql.types.{BinaryType, LongType, StringType, StructField, StructType, TimestampType}

object VideoSchema {
  val VIDEO_URI = "video_uri"
  val FRAME_ID = "frame_id"
  val TS = "ts"
  val IMAGE_DATA = "image_data"

  def columnSchema: StructType =
    StructType(
      Seq(
        StructField(VIDEO_URI, StringType, nullable = false),
        StructField(FRAME_ID, LongType, nullable = false),
        StructField(TS, TimestampType, nullable = false),
        StructField(IMAGE_DATA, BinaryType, nullable = false)
      )
    )
}
