package ai.eto.rikai.sql.spark.datasources

import org.apache.spark.sql.sources.{
  EqualNullSafe,
  EqualTo,
  Filter,
  GreaterThan,
  GreaterThanOrEqual,
  LessThan,
  LessThanOrEqual
}

/** Push down by frame_id using lowest and highest
  * @param lowest
  * @param highest
  */
case class FrameIdFilterV1(lowest: Option[Long], highest: Option[Long])

object FrameIdFilterV1 {
  val DEFAULT_LOWEST_ID = 1L
  val DEFAULT_HIGHEST_ID = Long.MaxValue

  def apply(filters: Seq[Filter]): FrameIdFilterV1 = {
    var lowest = DEFAULT_HIGHEST_ID
    var highest = DEFAULT_LOWEST_ID
    filters.foreach {
      case EqualTo(_, value) =>
        lowest = lowest.min(value.asInstanceOf[Long])
        highest = highest.max(value.asInstanceOf[Long])
      case EqualNullSafe(_, value) =>
        lowest = lowest.min(value.asInstanceOf[Long])
        highest = highest.max(value.asInstanceOf[Long])
      case GreaterThanOrEqual(_, value) =>
        lowest = lowest.min(value.asInstanceOf[Long])
      case GreaterThan(_, value) =>
        lowest = lowest.min(value.asInstanceOf[Long])
      case LessThan(_, value) =>
        highest = highest.max(value.asInstanceOf[Long])
      case LessThanOrEqual(_, value) =>
        highest = highest.max(value.asInstanceOf[Long])
      case _ =>
    }

    if (lowest == DEFAULT_HIGHEST_ID && highest == DEFAULT_LOWEST_ID) {
      FrameIdFilterV1(None, None)
    } else if (lowest == DEFAULT_HIGHEST_ID) {
      FrameIdFilterV1(None, Some(highest))
    } else if (highest == DEFAULT_LOWEST_ID) {
      FrameIdFilterV1(Some(lowest), None)
    } else {
      FrameIdFilterV1(Some(lowest), Some(highest))
    }
  }
}
