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
