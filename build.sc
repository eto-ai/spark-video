import mill._
import mill.scalalib._
import mill.scalalib.publish._
import mill.scalalib.scalafmt._
import mill.modules.Assembly
import mill.modules.Assembly.Rule.ExcludePattern

import $ivy.`org.bytedeco:javacpp:1.5.6`

object video extends ScalaModule with PublishModule with ScalafmtModule {
  override def scalaVersion = "2.12.13"

  override def publishVersion = "0.0.2"

  override def artifactId = "spark-video"

  override def pomSettings = PomSettings(
    description = "My first library",
    organization = "eto.ai.rikai",
    url = "https://github.com/darcy-shen/spark-video",
    licenses = Seq(License.MIT),
    versionControl = VersionControl.github("darcy-shen", "spark-video"),
    developers = Seq(
      Developer("darcy-shen", "Darcy Shen", "https://github.com/darcy-shen")
    )
  )

  def javacppVersion = "1.5.6"

  override def compileIvyDeps = Agg(
    ivy"org.apache.spark::spark-mllib:3.2.0",
    ivy"com.amazonaws:aws-java-sdk-s3:1.11.173"
  )

  override def ivyDeps = Agg(
    ivy"org.bytedeco:javacv:${javacppVersion}",
    ivy"org.bytedeco:ffmpeg:4.4-${javacppVersion}"
  )

  def assemblyRules = Assembly.defaultRules ++ Seq(ExcludePattern("scala/.*"))

  object test extends Tests with TestModule.ScalaTest {
    def javacppPlatform = org.bytedeco.javacpp.Loader.Detector.getPlatform

    override def ivyDeps = Agg(
      ivy"org.bytedeco:ffmpeg:4.4-${javacppVersion};classifier=${javacppPlatform}",
      ivy"org.apache.spark::spark-mllib:3.1.2",
      ivy"com.amazonaws:aws-java-sdk-s3:1.11.173",
      ivy"org.scalatest::scalatest:3.0.8",
      ivy"ch.qos.logback:logback-classic:1.2.3"
    )
  }
}

