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

import scala.collection.mutable.{Map, Seq}

import org.apache.spark.{FetchFailed, HashPartitioner, ShuffleDependency, SparkConf, Success}
import org.apache.spark.api.java.Optional
import org.apache.spark.api.shuffle.{MapShuffleLocations, ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleLocation, ShuffleLocationComponents}
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.io.DefaultShuffleDataIO
import org.apache.spark.storage.BlockManagerId

class FileServerShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  val defaultShuffleDataIO = new DefaultShuffleDataIO(sparkConf)
  override def driver(): ShuffleDriverComponents = defaultShuffleDataIO.driver()

  override def executor(): ShuffleExecutorComponents = defaultShuffleDataIO.executor()

  override def shuffleLocations(): Optional[ShuffleLocationComponents] = {
    Optional.of(new FileServerShuffleLocationComponents())
  }
}

class ShuffleServer(hostname: String, portNum: Int) {
  def host(): String = hostname
  def port(): Int = portNum

  override def equals(other: Any): Boolean = {
    other match {
      case other: ShuffleServer => other.host() == host() && other.port() == port()
      case _ => false
    }
  }

  override def hashCode(): Int = {
    host().hashCode + port().hashCode()
  }
}

class FileServerShuffleLocation(
    primary: ShuffleServer,
    secondary: ShuffleServer) extends ShuffleLocation {
  def locations(): Set[ShuffleServer] = Set(primary, secondary)
}

class FileServerMapShuffleLocations(mapShuffleLocationsInput: Seq[FileServerShuffleLocation])
  extends MapShuffleLocations {
  override def getLocationForBlock(reduceId: Int): ShuffleLocation =
    mapShuffleLocationsInput(reduceId)

  def shouldInvalidateMapStatus(lostLoc: FileServerShuffleLocation): Boolean = {
    val filtered = mapShuffleLocationsInput.filter(partitionLocations =>
      partitionLocations.locations() == lostLoc.locations()
    )
    filtered.nonEmpty
  }
}

class FileServerShuffleLocationComponents extends ShuffleLocationComponents {
  override def shouldRemoveMapOutputOnLostBlock(
    lostLocation: ShuffleLocation,
    mapOutputLocations: MapShuffleLocations): Boolean = {
    val lostFSLoc = lostLocation.asInstanceOf[FileServerShuffleLocation]
    val mappedFsLoc = mapOutputLocations.asInstanceOf[FileServerMapShuffleLocations]
    mappedFsLoc.shouldInvalidateMapStatus(lostFSLoc)
  }
}

class DAGSchedulerFileServerSuite extends DAGSchedulerSuite {

  private def setupTest(): (RDD[_], Int) = {
    afterEach()
    val conf = new SparkConf()
    // unregistering all outputs on a host is enabled for the individual file server case
    conf.set(config.SHUFFLE_IO_PLUGIN_CLASS, classOf[FileServerShuffleDataIO].getName)
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
    val mapStatus2 = makeFileServerMapStatus("hostA", "hostE", "hostB", "hostE")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assertMapOutputTrackerContains(shuffleId,
      Seq(mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42),
      (FetchFailed(
        makeShuffleLocation("hostA", "hostB"), shuffleId, 0, 0, None, "ignored"), null)))
    assert(scheduler.failedStages.size > 0)
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 1)
    assertMapOutputTrackerContains(shuffleId, Seq(null, mapStatus2.mapShuffleLocations))

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
    complete(taskSets(1), Seq((Success, 42),
      (FetchFailed(
        makeShuffleLocation("hostA", "hostB"), shuffleId, 0, 0, None, "ignored"), null)))
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
    val partition1 = new FileServerShuffleLocation(
      new ShuffleServer(partition1Primary, 1234), new ShuffleServer(partition1Secondary, 1234))
    val partition2 = new FileServerShuffleLocation(
      new ShuffleServer(partition2Primary, 1234), new ShuffleServer(partition2Secondary, 1234))

    makeFileServerMapStatus(partition1, partition2)
  }

  def makeFileServerMapStatus(partition1Loc: FileServerShuffleLocation,
                              partition2Loc: FileServerShuffleLocation): MapStatus = {
    MapStatus(
      makeBlockManagerId("executor-host"),
      new FileServerMapShuffleLocations(Seq(partition1Loc, partition2Loc)),
      Array.fill[Long](2)(2)
    )
  }

  def makeShuffleLocation(primary: String, secondary: String): FileServerShuffleLocation = {
    new FileServerShuffleLocation(new ShuffleServer(primary, 1234),
      new ShuffleServer(secondary, 1234))
  }

  def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)
}