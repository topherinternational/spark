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

import scala.collection.mutable.Map
import scala.collection.mutable.Seq

import org.apache.spark.{FetchFailed, HashPartitioner, ShuffleDependency, SparkConf, Success}
import org.apache.spark.api.shuffle.{MapShuffleLocations, ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleLocation}
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.io.DefaultShuffleDataIO
import org.apache.spark.storage.BlockManagerId

class PluginShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  val defaultShuffleDataIO = new DefaultShuffleDataIO(sparkConf)
  override def driver(): ShuffleDriverComponents = defaultShuffleDataIO.driver()

  override def executor(): ShuffleExecutorComponents = defaultShuffleDataIO.executor()
}

class DAGSchedulerPluginSuite extends DAGSchedulerSuite {

  class PluginShuffleLocation(loc: String) extends ShuffleLocation {
    def getLocation(): String = loc
  }

  class PluginMapShuffleLocations(shuffleLocation: ShuffleLocation)
    extends MapShuffleLocations {
    override def getLocationForBlock(reduceId: Int): ShuffleLocation = shuffleLocation
  }

  private def setupTest(): (RDD[_], Int) = {
    afterEach()
    val conf = new SparkConf()
    // unregistering all outputs on a host is enabled for the individual file server case
    conf.set(config.SHUFFLE_IO_PLUGIN_CLASS, classOf[PluginShuffleDataIO].getName)
    init(conf)
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    (reduceRdd, shuffleId)
  }

  test("Test simple shuffle success") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeMapStatus("hostA")
    val mapStatus2 = makeMapStatus("hostB")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assertMapShuffleLocations(shuffleId,
      Seq(mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(mapStatus1.location, mapStatus2.location))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test simple shuffle success with empty MapShuffleLocations") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val blockManagerId = makeBlockManagerId("hostA")
    val mapStatus = makeEmptyMapStatus("hostA")
    complete(taskSets(0), Seq((Success, mapStatus), (Success, mapStatus)))
    assertMapShuffleLocations(shuffleId, Seq(null, null))
    assertBlockManagerIds(shuffleId, Seq(blockManagerId, blockManagerId))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test fetch failed only removes single mapper") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus = makeMapStatus("hostA", "hostA")
    complete(taskSets(0), Seq((Success, mapStatus), (Success, mapStatus)))
    assertMapShuffleLocations(shuffleId,
      Seq(mapStatus.mapShuffleLocations, mapStatus.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(mapStatus.location, mapStatus.location))

    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeBlockManagerId("hostA"), shuffleId, 0, 0, "ignored"), null)))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 1)
    assertMapShuffleLocations(shuffleId, Seq(null, mapStatus.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(null, mapStatus.location))

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus)))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 2)
    assertMapShuffleLocations(shuffleId,
      Seq(mapStatus.mapShuffleLocations, mapStatus.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(mapStatus.location, mapStatus.location))

    scheduler.resubmitFailedStages()
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Doesn't remove MapStatus if blockManagerId doesn't match") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus = makeMapStatus("hostA", "hostA")
    complete(taskSets(0), Seq((Success, mapStatus), (Success, mapStatus)))
    assertMapShuffleLocations(shuffleId,
      Seq(mapStatus.mapShuffleLocations, mapStatus.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(mapStatus.location, mapStatus.location))

    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeBlockManagerId("hostC"), shuffleId, 0, 0, "ignored"), null)))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 2)
    assertMapShuffleLocations(shuffleId,
      Seq(mapStatus.mapShuffleLocations, mapStatus.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(mapStatus.location, mapStatus.location))

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Doesn't remove MapStatus if blockManagerId is null") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus = makeMapStatus("hostA", "hostA")
    complete(taskSets(0), Seq((Success, mapStatus), (Success, mapStatus)))
    assertMapShuffleLocations(shuffleId,
      Seq(mapStatus.mapShuffleLocations, mapStatus.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(mapStatus.location, mapStatus.location))

    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(null, shuffleId, 0, 0, "ignored"), null)))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 2)
    assertMapShuffleLocations(shuffleId,
      Seq(mapStatus.mapShuffleLocations, mapStatus.mapShuffleLocations))
    assertBlockManagerIds(shuffleId, Seq(mapStatus.location, mapStatus.location))

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  def assertMapShuffleLocations(
    shuffleId: Int,
    set: Seq[MapShuffleLocations]): Unit = {
    val actualShuffleLocations = mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses
      .map(mapStatus => {
        if (mapStatus == null) {
          return null
        }
        mapStatus.mapShuffleLocations
      })
    assert(set === actualShuffleLocations.toSeq)
  }

  def assertBlockManagerIds(shuffleId: Int, set: Seq[BlockManagerId]): Unit = {
    val actualBlockManagerIds = mapOutputTracker.shuffleStatuses(shuffleId).mapStatuses
      .map(mapStatus => {
        if (mapStatus == null) {
          return null
        }
        mapStatus.location
      })
    assert(set === actualBlockManagerIds)
  }

  def makeMapStatus(host: String): MapStatus = {
    makeMapStatus(host, host)
  }

  def makeMapStatus(blockManagerIdHost: String, shuffleLocationHost: String): MapStatus = {
    MapStatus(
      makeBlockManagerId(blockManagerIdHost),
      new PluginMapShuffleLocations(new PluginShuffleLocation(shuffleLocationHost)),
      Array.fill[Long](2)(2)
    )
  }

  def makeEmptyMapStatus(host: String): MapStatus = {
    MapStatus(
      makeBlockManagerId(host),
      null,
      Array.fill[Long](2)(2)
    )
  }

  def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)
}
