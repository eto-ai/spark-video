package ai.eto.rikai.sql.spark.datasources

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class VideoOptions(@transient val parameters: CaseInsensitiveMap[String])
    extends Serializable {
  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters))
  }

  val fps = parameters.get("fps").map(_.toInt).getOrElse(1)

  val imageWidth = parameters.get("imageWidth").map(_.toInt).getOrElse(0)

  val imageHeight = parameters.get("imageHeight").map(_.toInt).getOrElse(0)

  /** A list of scaler flags from
    * http://www.ffmpeg.org/ffmpeg-scaler.html#toc-Scaler-Options
    */
  val scalerFlag = parameters.get("scalerFlag").getOrElse("bilinear")
}
