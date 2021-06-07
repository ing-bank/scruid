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

package com.ing.wbaa.druid.definitions

import io.circe.{ Encoder, Json }

object QueryContext {

  type QueryContextParam = String

  sealed trait QueryContextValue {
    def toJson: Json
  }

  object QueryContextValue {
    implicit val encoder: Encoder[QueryContextValue] = new Encoder[QueryContextValue] {
      override def apply(a: QueryContextValue): Json = a.toJson
    }
  }

  implicit class QueryContextValueString(v: String) extends QueryContextValue {
    override def toJson: Json = Json.fromString(v)
  }

  implicit class QueryContextValueInt(v: Int) extends QueryContextValue {
    override def toJson: Json = Json.fromInt(v)
  }

  implicit class QueryContextValueLong(v: Long) extends QueryContextValue {
    override def toJson: Json = Json.fromLong(v)
  }

  implicit class QueryContextValueBoolean(v: Boolean) extends QueryContextValue {
    override def toJson: Json = Json.fromBoolean(v)
  }

  implicit class QueryContextValueFloat(v: Float) extends QueryContextValue {
    override def toJson: Json = Json.fromFloatOrNull(v)
  }

  implicit class QueryContextValueDouble(v: Double) extends QueryContextValue {
    override def toJson: Json = Json.fromDoubleOrNull(v)
  }

  // the following apply only to all queries
  final val Timeout                  = "timeout"
  final val Priority                 = "priority"
  final val Lane                     = "lane"
  final val QueryId                  = "queryId"
  final val UseCache                 = "useCache"
  final val PopulateCache            = "populateCache"
  final val UseResultLevelCache      = "useResultLevelCache"
  final val PopulateResultLevelCache = "populateResultLevelCache"
  final val BySegment                = "bySegment"
  final val Finalize                 = "finalize"
  @deprecated("deprecated context parameter in Druid", since = "2.3.0")
  final val ChunkPeriod                   = "chunkPeriod"
  final val MaxScatterGatherBytes         = "maxScatterGatherBytes"
  final val MaxQueuedBytes                = "maxQueuedBytes"
  final val SerializeDateTimeAsLong       = "serializeDateTimeAsLong"
  final val SerializeDateTimeAsLongInner  = "serializeDateTimeAsLongInner"
  final val EnableParallelMerge           = "enableParallelMerge"
  final val ParallelMergeParallelism      = "parallelMergeParallelism"
  final val ParallelMergeInitialYieldRows = "parallelMergeInitialYieldRows"
  final val ParallelMergeSmallBatchRows   = "parallelMergeSmallBatchRows"
  final val UseFilterCNF                  = "useFilterCNF"
  final val SecondaryPartitionPruning     = "secondaryPartitionPruning"

  final val Vectorize               = "vectorize"
  final val VectorSize              = "vectorizeSize"
  final val VectorizeVirtualColumns = "vectorizeVirtualColumns"

  // the following apply only to time-series queries
  final val SkipEmptyBuckets = "skipEmptyBuckets"
  final val GrandTotal       = "grandTotal"

  // the following apply only to top-n queries
  final val MinTopNThreshold = "minTopNThreshold"

  // the following apply only to group-by queries
  final val GroupByStrategy         = "groupByStrategy"
  final val GroupByIsSingleThreaded = "groupByIsSingleThreaded"

  // group-by V2 parameters
  final val MaxMergingDictionarySize    = "maxMergingDictionarySize"
  final val MaxOnDiskStorage            = "maxOnDiskStorage"
  final val BufferGrouperInitialBuckets = "bufferGrouperInitialBuckets"
  final val BufferGrouperMaxLoadFactor  = "bufferGrouperMaxLoadFactor"
  final val ForceHashAggregation        = "forceHashAggregation"
  final val IntermediateCombineDegree   = "intermediateCombineDegree"
  final val NumParallelCombineThreads   = "numParallelCombineThreads"

  final val SortByDimsFirst             = "sortByDimsFirst"
  final val ApplyLimitPushDownToSegment = "applyLimitPushDownToSegment"
  final val ForceLimitPushDown          = "forceLimitPushDown"

  // group-by V1 parameters
  final val MaxIntermediateRows = "maxIntermediateRows"
  final val MaxResults          = "maxResults"
  final val UseOffheap          = "useOffheap"

  // sql parameters
  final val SqlQueryId                  = "sqlQueryId"
  final val SqlTimeZone                 = "sqlTimeZone"
  final val UseApproximateCountDistinct = "useApproximateCountDistinct"
  final val UseApproximateTopN          = "useApproximateTopN"
}
