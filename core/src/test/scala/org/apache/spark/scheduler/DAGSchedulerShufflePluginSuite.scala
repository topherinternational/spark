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
package org.apache.spark.scheduler

import java.util.{Collections, Optional, List => JList, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.java8.OptionConverters._

import org.apache.spark.{FetchFailed, HashPartitioner, ShuffleDependency, SparkConf, Success}

import org.apache.spark.shuffle.api.{MapOutputMetadata, ShuffleBlockMetadata, ShuffleDriverComponents, ShuffleMetadata, ShuffleOutputTracker}
import org.apache.spark.storage.BlockManagerId

class PluginShuffleDriverComponents extends ShuffleDriverComponents {

  private val outputTracker = new TestShuffleOutputTracker()

  override def initializeApplication(): JMap[String, String] = Collections.emptyMap()

  override def unregisterOutputOnHostOnFetchFailure(): Boolean = true

  override def shuffleTracker(): Optional[ShuffleOutputTracker] =
    Some[ShuffleOutputTracker](outputTracker).asJava
}

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
      block: Optional[ShuffleBlockMetadata]): Unit = {

  }

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

class DAGSchedulerShufflePluginSuite extends DAGSchedulerSuite {

  override def loadShuffleDriverComponents(sparkConf: SparkConf): ShuffleDriverComponents = {
    new PluginShuffleDriverComponents()
  }

  def setupRdds(): (MyRDD, Int) = {
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    (reduceRdd, shuffleId)
  }

  test("Some outputs available externally - do not recompute those.") {
    val (reduceRdd, shuffleId) = setupRdds()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val taskResult1 = makeMapTaskLocalOnlyResult(null, "hostA")
    val taskResult2 = makeMapTaskExternalResult(null, "hostB")
    complete(taskSets(0), Seq((Success, taskResult1), (Success, taskResult2)))

    complete(taskSets(1), Seq(
      (FetchFailed(BlockManagerId(null, "hostA", 1234), shuffleId, 0, 0L, 0, "ignored"), null),
      (FetchFailed(BlockManagerId(null, "hostB", 1234), shuffleId, 1, 0L, 0, "ignored"), null)))
    // We keep taskResult2's map output, because we kept it externally.
    assertMapShuffleLocations(shuffleId, Seq(null, taskResult2.status))
    scheduler.resubmitFailedStages()
    assert(taskSets(2).tasks.length === 1)
    val recomputedTaskResult1 = makeMapTaskLocalOnlyResult(null, "hostC")
    complete(taskSets(2), Seq((Success, recomputedTaskResult1)))
    complete(taskSets(3), Seq((Success, 42), (Success, 43)))

    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  def makeMapTaskLocalOnlyResult(execId: String, host: String): MapTaskResult = {
    MapTaskResult(
      MapStatus(BlockManagerId(execId, host, 1234), Array.fill[Long](2)(2), 0),
      Some(LocalOnlyOutput))
  }

  def makeMapTaskExternalResult(execId: String, host: String): MapTaskResult = {
    MapTaskResult(
      MapStatus(BlockManagerId(execId, host, 1234), Array.fill[Long](2)(2), 0),
      Some(ExternalOutput))
  }

  def makeEmptyMapTaskLocalOnlyResult(): MapTaskResult = {
    MapTaskResult(
      MapStatus(null, Array.fill[Long](2)(2), 0),
      Some(LocalOnlyOutput))
  }

  def assertMapShuffleLocations(shuffleId: Int, set: Seq[MapStatus]): Unit = {
    val actualShuffleLocations = mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses
    assert(actualShuffleLocations.toSeq === set)
  }
}
