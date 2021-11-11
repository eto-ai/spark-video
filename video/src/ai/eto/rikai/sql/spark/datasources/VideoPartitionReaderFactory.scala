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

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.bytedeco.javacv.{
  FFmpegFrameGrabber,
  Frame,
  FrameGrabber,
  Java2DFrameConverter
}

import java.net.URI
import javax.imageio.ImageIO

case class VideoPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: VideoOptions,
    filters: Seq[Filter]
) extends FilePartitionReaderFactory {

  private def getNumberOfFps(grabber: FrameGrabber): Int = {
    val fps = Math.floor {
      grabber.getLengthInFrames / Math.floor(
        grabber.getLengthInTime / 1000000.0
      )
    }
    val numFps = Math.floor(fps / options.fps).toInt
    if (numFps == 0) 1 else numFps
  }

  override def buildReader(
      file: PartitionedFile
  ): PartitionReader[InternalRow] = {
    val uri = new URI(file.filePath)
    val extension = FilenameUtils.getExtension(file.filePath)
    val tmpPath = uri.getScheme match {
      case "s3" =>
        val region = SQLConf.get.getConfString("spark.hadoop.aws.region")
        val tmpFileName = String.valueOf(
          DigestUtils.getMd5Digest.digest(file.toString().getBytes())
        )
        val s3Utils = new S3Utils(region)
        val fullName = s"/tmp/${tmpFileName}.${extension}"
        s3Utils.getObject(uri, fullName)
        fullName
      case _ =>
        file.filePath
    }
    val grabber = new FFmpegFrameGrabber(tmpPath)
    val converter = new Java2DFrameConverter
    grabber.start()
    val numOfFps = getNumberOfFps(grabber)
    grabber.setFrameNumber(file.start.toInt)
    var frame: Frame = null
    var frame_id = file.start

    new PartitionReader[InternalRow] {
      override def next(): Boolean = {
        val targetFrameId = frame_id + numOfFps
        while (frame_id < targetFrameId - 1) {
          frame_id = frame_id + 1
          grabber.grabImage()
        }
        frame_id = targetFrameId
        frame = grabber.grabImage()
        frame != null && (frame_id < file.start + file.length)
      }

      override def get(): InternalRow = {
        val row = new GenericInternalRow(4)
        row.update(0, UTF8String.fromString(uri.toString))
        row.setLong(1, frame_id)
        row.update(2, frame.timestamp)
        val javaImage = converter.convert(frame)
        val bos = new ByteArrayOutputStream()
        ImageIO.write(javaImage, "png", bos)
        row.update(3, bos.toByteArray)
        row
      }

      override def close(): Unit = {
        grabber.stop()
        converter.close()
      }
    }
  }
}
