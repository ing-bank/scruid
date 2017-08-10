/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ing.wbaa.druid.definitions

sealed trait Filter {
  val kind: String
}

object FilterOperators {

  implicit class LogicalOperations(lhs: Filter) {
    private def buildFilter(predicate: String, args: List[Filter]): Filter = {
      val newFilters = (lhs :: args).flatMap {
        case FilterEmpty => None
        case filter: Filter => Some(filter)
      }

      if (newFilters.size > 1) {
        FilterCompose(predicate, newFilters)
      } else if (newFilters.nonEmpty) {
        newFilters.head
      } else {
        FilterEmpty
      }
    }

    def &&(otherFilter: Filter*): Filter = buildFilter("and", otherFilter.toList)

    def ||(otherFilter: Filter*): Filter = buildFilter("or", otherFilter.toList)

    def unary_!(): Filter = FilterNot(kind = "not", lhs)
  }

}

case class FilterSelect(kind: String = "selector", dimension: String, value: Any) extends Filter

case class FilterRegex(kind: String = "regex", dimension: String, pattern: String) extends Filter

case class FilterCompose(kind: String, fields: List[Filter]) extends Filter

case class FilterNot(kind: String = "not", field: Filter) extends Filter

case object FilterEmpty extends Filter {
  override val kind: String = "empty"
}
