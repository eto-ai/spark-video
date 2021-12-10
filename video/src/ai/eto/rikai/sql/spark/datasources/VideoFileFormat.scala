package ai.eto.rikai.sql.spark.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{
  FileFormat,
  OutputWriterFactory,
  PartitionedFile
}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class VideoFileFormat extends FileFormat with DataSourceRegister {
  override def shortName(): String = "video"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]
  ): Option[StructType] = {
    Some(VideoSchema.columnSchema)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType
  ): OutputWriterFactory = {
    ???
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf)
    )
    val factory = VideoPartitionReaderFactory(
      sparkSession.sessionState.conf,
      broadcastedConf,
      dataSchema,
      requiredSchema,
      partitionSchema,
      new VideoOptions(options),
      filters
    )
    file => factory.buildIterator(file)
  }
}
