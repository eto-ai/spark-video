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

import org.apache.spark.sql.types.{
  BinaryType,
  LongType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

object VideoSchema {

  /** Supported video_uri:
    * + s3: `s3://bucket_name/path/to/video.mp4`
    * + local: `file:///path/to/video.mp4`
    */
  val VIDEO_URI = "video_uri"

  /** frame_id starts from 1, if a video consists of 10 frames, here are all the
    * frame_ids: 1 2 3 4 5 6 7 8 9 10
    */
  val FRAME_ID = "frame_id"

  /** Using 'hh:mm:ss' part of TimestampType for now
    *
    * TODO: A UDT based on TimestampType which displays 'hh:mm:ss' by default
    */
  val TS = "ts"

  /** Images are saved as BinaryType in PNG format
    */
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
