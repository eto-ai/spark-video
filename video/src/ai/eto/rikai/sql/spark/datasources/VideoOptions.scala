package ai.eto.rikai.sql.spark.datasources

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.bytedeco.ffmpeg.global.swscale
import org.bytedeco.javacv.FrameGrabber

class VideoOptions(@transient val parameters: CaseInsensitiveMap[String])
    extends Serializable {
  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters))
  }

  // Number of frames per second
  val fps = parameters.get("fps").map(_.toInt).getOrElse(0)

  /** stepSize and stepOffset are used to determine how frames are selected
    * assuming the frame_ids are 1 2 3 4 5 6 7 8 9 10
    * with 3 as stepSize, 0 as stepOffset, here are the selected frames:
    * 3 6 9
    * with 3 as stepSize, 1 as stepOffset, here are the selected frames:
    * 1 4 7 10
    * with 3 as stepSize, 2 as stepOffset, here are the selected frames:
    * 2 5 8
    */
  val frameStepSize = parameters.get("frameStepSize").map(_.toInt).getOrElse(1)
  val frameStepOffset =
    parameters.get("frameStepOffset").map(_.toInt).getOrElse(0)

  /** Setting width and height when loading the videos into frames
    * Default to 0, means that keep the original width or height
    */
  val imageWidth = parameters.get("imageWidth").map(_.toInt).getOrElse(0)
  val imageHeight = parameters.get("imageHeight").map(_.toInt).getOrElse(0)

  /** A list of scaler flags from
    * http://www.ffmpeg.org/ffmpeg-scaler.html#toc-Scaler-Options
    */
  val scalerFlag = parameters.get("scalerFlag").getOrElse("bilinear")

  def getFrameStep(grabber: FrameGrabber): (Int, Int) = {
    if (fps <= 0) {
      val startId = if (frameStepOffset == 0) frameStepSize else frameStepOffset
      (frameStepSize, startId)
    } else {
      val realFps = Math.floor {
        grabber.getLengthInFrames / Math.floor(
          grabber.getLengthInTime / 1000000.0
        )
      }
      if (fps == 1) {
        val stepSize = Math.floor(realFps / fps).toInt
        (stepSize, stepSize / 2)
      } else {
        val stepSize = Math.floor(Math.ceil(realFps) / fps).toInt
        (stepSize, 1)
      }
    }
  }

  def getImageScalingFlags(): Int = {
    scalerFlag match {
      case "fast_bilinear" => swscale.SWS_FAST_BILINEAR
      case "bilinear"      => swscale.SWS_BILINEAR
      case "bicubic"       => swscale.SWS_BICUBIC
      case "experimental"  => swscale.SWS_X
      case "area"          => swscale.SWS_AREA
      case "bicublin"      => swscale.SWS_BICUBLIN
      case "gauss"         => swscale.SWS_GAUSS
      case "sinc"          => swscale.SWS_SINC
      case "lanczos"       => swscale.SWS_LANCZOS
      case "spline"        => swscale.SWS_SPLINE
      case unknown =>
        throw new IllegalArgumentException(
          s"Unsupported scaler flag: ${unknown}"
        )
    }
  }
}
