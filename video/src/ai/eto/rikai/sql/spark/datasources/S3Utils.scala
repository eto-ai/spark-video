package ai.eto.rikai.sql.spark.datasources

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import com.amazonaws.services.s3.model.{GetObjectRequest, ObjectMetadata}

import java.io.File
import java.net.URI

class S3Utils(val region: String) extends AutoCloseable {
  import S3Utils.uri2request

  val s3 =
    AmazonS3ClientBuilder
      .standard()
      .withRegion(region)
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .build()

  def getObject(uri: URI, path: String): ObjectMetadata = {
    s3.getObject(uri2request(uri), new File(path))
  }

  override def close(): Unit = {
    s3.shutdown()
  }
}

object S3Utils {
  def uri2request(uri: URI): GetObjectRequest = {
    val aUri = new AmazonS3URI(uri)
    new GetObjectRequest(aUri.getBucket, aUri.getKey)
  }
}
