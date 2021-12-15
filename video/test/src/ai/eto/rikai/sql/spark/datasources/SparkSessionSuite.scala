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

import ch.qos.logback.classic.{Level, Logger}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.util.Properties

class SparkSessionSuite extends FunSuite with LazyLogging {
  private val level = Level.valueOf {
    Properties.envOrElse("LOG_LEVEL", Level.ERROR.levelStr)
  }
  LoggerFactory
    .getLogger("root")
    .asInstanceOf[Logger]
    .setLevel(level)
  LoggerFactory
    .getLogger("ai.eto.rikai")
    .asInstanceOf[Logger]
    .setLevel(Level.DEBUG)

  protected val localVideo = "video/test/resources/big_buck_bunny_short.mp4"
  protected val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .config(
      "spark.sql.extensions",
      "ai.eto.rikai.sql.spark.SparkVideoExtensions"
    )
    .getOrCreate()
  protected val rabbitFrames = spark.read
    .format("video")
    .option("fps", 1)
    .load(localVideo)
}
