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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.collection.mutable.Map
import scala.collection.mutable.Seq

import org.apache.spark.{FetchFailed, HashPartitioner, ShuffleDependency, SparkConf, Success}
import org.apache.spark.api.java.Optional
import org.apache.spark.api.shuffle.{MapShuffleLocations, ShuffleLocation}
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerId

class DAGSchedulerFileServerSuite extends DAGSchedulerSuite {

  class FileServerShuffleLocation(hostname: String, portInt: Int) extends ShuffleLocation {
    override def host(): String = hostname
    override def port(): Int = portInt
  }
  
  class FileServerMapShuffleLocations(mapShuffleLocationsInput: Buffer[Buffer[ShuffleLocation]])
    extends MapShuffleLocations {
    val mapShuffleLocations = mapShuffleLocationsInput
    override def getLocationsForBlock(reduceId: Int): util.List[ShuffleLocation] =
      mapShuffleLocations(reduceId).asJava

    override def invalidateShuffleLocation(host: String, port: Optional[Integer]): Boolean = {
      var missingPartition = false
      for ((locations, i) <- mapShuffleLocations.zipWithIndex) {
        mapShuffleLocations(i) = locations.filter(loc =>
          loc.host() != host || (port.isPresent && loc.port() != port.get()))
        if (locations.isEmpty) {
          missingPartition = true
        }
      }
      missingPartition
    }

    override def invalidateShuffleLocation(executorId: String): Boolean = {
      return false
    }
  }

  private def setupTest(): (RDD[_], Int) = {
    afterEach()
    val conf = new SparkConf()
    // unregistering all outputs on a host is enabled for the individual file server case
    conf.set(config.UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE, true)
    conf.set(config.SHUFFLE_SERVICE_ENABLED, true)
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
    val mapStatus1 = makeFileServerMapStatus("hostA", "hostB", "hostC", "hostD")
    val mapStatus2 = makeFileServerMapStatus("hostA", "hostB", "hostC", "hostE")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assertMapOutputTrackerContains(shuffleId,
      Seq(mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test one mapper failed but another has blocks elsewhere") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeFileServerMapStatus("hostA", "hostB", "hostC", "hostD")
    val mapStatus2 = makeFileServerMapStatus("hostA", "hostE", "hostC", "hostE")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assertMapOutputTrackerContains(shuffleId,
      Seq(mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (FetchFailed(
      Seq(
        shuffleLocation("hostA"),
        shuffleLocation("hostB"),
        shuffleLocation("hostC"),
        shuffleLocation("hostD")),
      shuffleId, 0, 0, "ignored"), null)))
    assert(scheduler.failedStages.size > 0)
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 1)
    assertMapOutputTrackerContains(shuffleId, Seq(null,
      new FileServerMapShuffleLocations(Buffer(
        shuffleLocationSeq("hostE"),
        shuffleLocationSeq("hostE")))))

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus1)))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 2)
    assertMapOutputTrackerContains(shuffleId, Seq(
      mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("Test one failed but other only has one partition replicated") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeFileServerMapStatus("hostA", "hostB", "hostC", "hostD")
    val mapStatus2 = makeFileServerMapStatus("hostA", "hostB", "hostC", "hostE")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assertMapOutputTrackerContains(shuffleId,
      Seq(mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (FetchFailed(
      Seq(
        shuffleLocation("hostA"),
        shuffleLocation("hostB"),
        shuffleLocation("hostC"),
        shuffleLocation("hostD")),
      shuffleId, 0, 0, "ignored"), null)))
    assert(scheduler.failedStages.size > 0)
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 0)
    assertMapOutputTrackerContains(shuffleId, Seq(null, null))

    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 2)
    assertMapOutputTrackerContains(shuffleId, Seq(
      mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }


  def assertMapOutputTrackerContains(
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

  def makeFileServerMapStatus(
    partition1Primary: String,
    partition1Secondary: String,
    partition2Primary: String,
    partition2Secondary: String): MapStatus = {
    val partition1List: Buffer[ShuffleLocation] = Buffer(
      new FileServerShuffleLocation(partition1Primary, 1234),
      new FileServerShuffleLocation(partition1Secondary, 1234))
    val partition2List: Buffer[ShuffleLocation] = Buffer(
      new FileServerShuffleLocation(partition2Primary, 1234),
      new FileServerShuffleLocation(partition2Secondary, 1234))
    makeFileServerMapStatus(partition1List, partition2List)
  }

  def makeFileServerMapStatus(partition1Loc: Buffer[ShuffleLocation],
                              partition2Loc: Buffer[ShuffleLocation]): MapStatus = {
    MapStatus(
      makeBlockManagerId("executor-host"),
      new FileServerMapShuffleLocations(Buffer(partition1Loc, partition2Loc)),
      Array.fill[Long](2)(2)
    )
  }

  def shuffleLocationSeq(hosts: String*): Buffer[ShuffleLocation] = {
    hosts.map(host =>
      shuffleLocation(host)
    ).toBuffer
  }

  def shuffleLocation(host: String): ShuffleLocation = {
    new FileServerShuffleLocation(host, 1234)
  }

  def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)
}
