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

import java.net.URI
import javax.imageio.ImageIO

import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{
  FilePartitionReaderFactory,
  PartitionReaderWithPartitionValues
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.bytedeco.javacv.{FFmpegFrameGrabber, Frame, Java2DFrameConverter}

case class VideoPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    options: VideoOptions,
    filters: Seq[Filter]
) extends FilePartitionReaderFactory {

  private def resolveFilePath(file: PartitionedFile): String = {
    val uri = new URI(file.filePath)
    val extension = FilenameUtils.getExtension(file.filePath)
    uri.getScheme match {
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
  }

  private def buildGrabber(
      options: VideoOptions,
      file: PartitionedFile
  ): FFmpegFrameGrabber = {
    val path = resolveFilePath(file)
    val grabber = new FFmpegFrameGrabber(path)
    grabber.setImageWidth(options.imageWidth)
    grabber.setImageHeight(options.imageHeight)
    grabber.setImageScalingFlags(options.getImageScalingFlags())
    grabber.start()
    grabber.setFrameNumber(file.start.toInt)
    grabber
  }

  private def buildRow(
      converter: Java2DFrameConverter,
      readDataSchema: StructType,
      uri: URI,
      frameId: Long,
      frame: Frame
  ): InternalRow = {
    val size = readDataSchema.size
    val row = new GenericInternalRow(size)
    (0 until size).foreach { index =>
      readDataSchema(index).name match {
        case VideoSchema.VIDEO_URI =>
          row.update(index, UTF8String.fromString(uri.toString))
        case VideoSchema.FRAME_ID =>
          row.setLong(index, frameId)
        case VideoSchema.TS =>
          row.update(index, frame.timestamp)
        case VideoSchema.IMAGE_DATA =>
          val javaImage = converter.convert(frame)
          val bos = new ByteArrayOutputStream()
          ImageIO.write(javaImage, "png", bos)
          row.update(index, bos.toByteArray)
      }
    }
    row
  }

  def buildIterator(file: PartitionedFile): Iterator[InternalRow] = {
    val converter = new Java2DFrameConverter
    val uri = new URI(file.filePath)
    val grabber = buildGrabber(options, file)
    val (frameStep, offset) = options.getFrameStep(grabber)
    var frame: Frame = null
    var frameId = file.start - frameStep + offset

    // Grab the first frame
    val targetFrameId = frameId + frameStep
    while (frameId < targetFrameId - 1) {
      frameId = frameId + 1
      grabber.grabImage()
    }
    frameId = targetFrameId
    frame = grabber.grabImage()

    new Iterator[InternalRow] {
      override def hasNext: Boolean = {
        frame != null
      }

      override def next(): InternalRow = {
        val currentRow =
          buildRow(converter, readDataSchema, uri, frameId, frame)

        // Grab the next frame
        val targetFrameId = frameId + frameStep
        while (frameId < targetFrameId - 1) {
          frameId = frameId + 1
          grabber.grabImage()
        }
        frameId = targetFrameId
        frame = grabber.grabImage()

        currentRow
      }
    }
  }

  override def buildReader(
      file: PartitionedFile
  ): PartitionReader[InternalRow] = {
    val converter = new Java2DFrameConverter
    val uri = new URI(file.filePath)
    val grabber = buildGrabber(options, file)
    val (frameStep, offset) = options.getFrameStep(grabber)

    var frame: Frame = null
    var frameId = file.start - frameStep + offset

    val videoReader = new PartitionReader[InternalRow] {
      override def next(): Boolean = {
        val targetFrameId = frameId + frameStep
        while (frameId < targetFrameId - 1) {
          frameId = frameId + 1
          grabber.grabImage()
        }
        frameId = targetFrameId
        frame = grabber.grabImage()
        frame != null && (frameId < file.start + file.length)
      }

      override def get(): InternalRow = {
        buildRow(converter, readDataSchema, uri, frameId, frame)
      }

      override def close(): Unit = {
        grabber.stop()
        converter.close()
      }
    }
    new PartitionReaderWithPartitionValues(
      videoReader,
      readDataSchema,
      partitionSchema,
      file.partitionValues
    )
  }
}
