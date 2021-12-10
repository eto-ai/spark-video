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
}
