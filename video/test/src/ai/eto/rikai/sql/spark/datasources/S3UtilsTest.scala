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

import org.scalatest.FunSuite

import java.net.URI

class S3UtilsTest extends FunSuite {
  test("uri2request") {
    import S3Utils.uri2request

    val r1 = uri2request(new URI("s3://bucket_name/key_name"))
    assert(r1.getKey === "key_name")
    assert(r1.getBucketName === "bucket_name")
  }
}
