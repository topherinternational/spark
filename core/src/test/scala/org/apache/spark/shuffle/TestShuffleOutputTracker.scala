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

package org.apache.spark.shuffle

import java.util.{List => JList, Optional}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.java8.OptionConverters._

import org.apache.spark.shuffle.api.{MapOutputMetadata, ShuffleBlockMetadata, ShuffleMetadata, ShuffleOutputTracker}

case class ShuffleDataAttemptId(shuffleId: Int, mapId: Int, mapAttemptId: Long)

case object LocalOnlyOutput extends MapOutputMetadata

case object ExternalOutput extends MapOutputMetadata

case class ExternalShuffleMetadata(externalOutputs: Set[ShuffleDataAttemptId])
  extends ShuffleMetadata

class TestShuffleOutputTracker extends ShuffleOutputTracker {

  private val externalOutputs =
    new ConcurrentHashMap[Int, mutable.Set[ShuffleDataAttemptId]]().asScala

  override def registerShuffle(shuffleId: Int, numMaps: Int): Unit = {
    externalOutputs.putIfAbsent(
      shuffleId, ConcurrentHashMap.newKeySet[ShuffleDataAttemptId]().asScala)
  }

  override def registerMapOutput(
      shuffleId: Int,
      mapId: Int,
      mapAttemptId: Long,
      metadata: Optional[MapOutputMetadata]): Unit = {
    metadata.asScala.getOrElse(LocalOnlyOutput) match {
      case ExternalOutput =>
        externalOutputs(shuffleId) += ShuffleDataAttemptId(shuffleId, mapId, mapAttemptId)
      case _ =>
    }
  }

  override def handleFetchFailure(
      shuffleId: Int,
      mapId: Int,
      mapAttemptId: Long,
      partitionId: Long,
      block: Optional[ShuffleBlockMetadata]): Unit = {}

  override def invalidateShuffle(shuffleId: Int): Unit = {
    externalOutputs(shuffleId).clear()
  }

  override def unregisterShuffle(shuffleId: Int): Unit = {
    externalOutputs -= shuffleId
  }

  override def shuffleMetadata(shuffleId: Int): Optional[ShuffleMetadata] = {
    externalOutputs.get(shuffleId).map(
      dataAttemptIds =>
        ExternalShuffleMetadata(dataAttemptIds.toSet)
          .asInstanceOf[ShuffleMetadata])
      .asJava
  }

  override def areAllPartitionsAvailableExternally(
      shuffleId: Int, mapId: Int, mapAttemptId: Long): Boolean = {
    externalOutputs.get(shuffleId).exists(
      _.contains(ShuffleDataAttemptId(shuffleId, mapId, mapAttemptId)))
  }

  override def preferredMapOutputLocations(
      shuffleId: Int, mapId: Int): JList[String] = {
    Seq.empty[String].asJava
  }

  override def preferredPartitionLocations(
      shuffleId: Int, mapId: Int): JList[String] = {
    Seq.empty[String].asJava
  }
}
