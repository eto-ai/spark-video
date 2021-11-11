package ai.eto.rikai.sql.spark.datasources

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class VideoOptions(@transient val parameters: CaseInsensitiveMap[String])
    extends Serializable {
  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap(parameters))
  }

  val fps =
    parameters.get("fps").map(_.toInt).getOrElse(1)
}
