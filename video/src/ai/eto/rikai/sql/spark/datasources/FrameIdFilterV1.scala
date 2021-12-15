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

import org.apache.spark.sql.sources.{
  EqualNullSafe,
  EqualTo,
  Filter,
  GreaterThan,
  GreaterThanOrEqual,
  LessThan,
  LessThanOrEqual
}

/** Push down by frame_id using the lowest and highest
  * This is a very naive implementation. For example,
  *     select * from video.`xyz.mp4` where frame_id > 10 and frame_id > 20
  * It will only skip the first 10 frames.
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
