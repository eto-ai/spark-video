import mill._
import mill.scalalib._
import mill.scalalib.publish._
import $ivy.`org.bytedeco:javacpp:1.5.6`

object video extends ScalaModule with PublishModule {
  override def scalaVersion = "2.12.13"

  override def publishVersion = "0.0.1-SNAPSHOT"

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

  def javacppPlatform = org.bytedeco.javacpp.Loader.Detector.getPlatform

  override def ivyDeps = Agg(
    ivy"org.apache.spark::spark-sql:3.1.2",
    ivy"org.bytedeco:javacv:${javacppVersion}",
    ivy"org.bytedeco:ffmpeg:4.4-${javacppVersion};classifier=${javacppPlatform}"
  )

  object test extends Tests with TestModule.ScalaTest {
    override def ivyDeps = Agg(
      ivy"org.scalatest::scalatest:3.0.8",
      ivy"ch.qos.logback:logback-classic:1.2.3"
    )
  }
}

