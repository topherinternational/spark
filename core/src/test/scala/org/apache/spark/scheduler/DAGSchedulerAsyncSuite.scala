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

import org.apache.spark.{FetchFailed, HashPartitioner, ShuffleDependency, SparkConf, Success}
import org.apache.spark.api.java.Optional
import org.apache.spark.api.shuffle.{MapShuffleLocations, ShuffleDataIO, ShuffleDriverComponents, ShuffleExecutorComponents, ShuffleLocation, ShuffleLocationComponents}
import org.apache.spark.internal.config
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.io.DefaultShuffleDataIO
import org.apache.spark.storage.BlockManagerId

class AsyncShuffleDataIO(sparkConf: SparkConf) extends ShuffleDataIO {
  val defaultShuffleDataIO = new DefaultShuffleDataIO(sparkConf)
  override def driver(): ShuffleDriverComponents = defaultShuffleDataIO.driver()
  override def executor(): ShuffleExecutorComponents = defaultShuffleDataIO.executor()

  override def shuffleLocations(): Optional[ShuffleLocationComponents] =
    Optional.of(new AsyncShuffleLocationComponents())
}

class AsyncShuffleLocationComponents extends ShuffleLocationComponents {
  override def shouldRemoveMapOutputOnLostBlock(
    lostLocation: ShuffleLocation,
    mapOutputLocations: MapShuffleLocations): Boolean = false
}

class DAGSchedulerAsyncSuite extends DAGSchedulerSuite {

  class AsyncShuffleLocation(hostname: String, portInt: Int) extends ShuffleLocation {
    def host(): String = hostname
    def port(): Int = portInt

    override def equals(other: Any): Boolean = {
      other match {
        case other: AsyncShuffleLocation => other.host() == host() && other.port() == port()
        case _ => false
      }
    }

    override def hashCode(): Int = {
      host().hashCode + port().hashCode()
    }
  }

  class AsyncMapShuffleLocations(asyncLocation: AsyncShuffleLocation)
    extends MapShuffleLocations {

    override def getLocationForBlock(reduceId: Int): ShuffleLocation = asyncLocation
  }

  private def setupTest(): (RDD[_], Int) = {
    afterEach()
    val conf = new SparkConf()
    // unregistering all outputs on a host is disabled for async case
    conf.set(config.SHUFFLE_IO_PLUGIN_CLASS, classOf[AsyncShuffleDataIO].getName)
    init(conf)
    val shuffleMapRdd = new MyRDD(sc, 2, Nil)
    val shuffleDep = new ShuffleDependency(shuffleMapRdd, new HashPartitioner(2))
    val shuffleId = shuffleDep.shuffleId
    val reduceRdd = new MyRDD(sc, 2, List(shuffleDep), tracker = mapOutputTracker)
    (reduceRdd, shuffleId)
  }

  test("Test async simple shuffle success") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeAsyncMapStatus("hostA")
    val mapStatus2 = makeAsyncMapStatus("hostB")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))
    assertMapOutputTrackerContains(shuffleId, Seq(
      mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    // perform reduce task
    complete(taskSets(1), Seq((Success, 42), (Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("test async fetch failed - different hosts") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    // Perform map task
    val mapStatus1 = makeAsyncMapStatus("hostA")
    val mapStatus2 = makeAsyncMapStatus("hostB")
    complete(taskSets(0), Seq((Success, mapStatus1), (Success, mapStatus2)))

    // The 2nd ResultTask reduce task failed. This will remove that shuffle location,
    // but the other shuffle block is still available
    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeAsyncShuffleLocation("hostA"),
        shuffleId, 0, 0, Some(makeBlockManagerId("hostA")), "ignored"), null)))
    assert(scheduler.failedStages.size > 0)
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 1)
    assertMapOutputTrackerContains(shuffleId, Seq(null, mapStatus2.mapShuffleLocations))

    // submit the mapper once more
    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus1)))
    assertMapOutputTrackerContains(shuffleId,
      Seq(mapStatus1.mapShuffleLocations, mapStatus2.mapShuffleLocations))

    // submit last reduce task
    complete(taskSets(3), Seq((Success, 43)))
    assert(results === Map(0 -> 42, 1 -> 43))
    assertDataStructuresEmpty()
  }

  test("test async fetch failed - same host, same exec") {
    val (reduceRdd, shuffleId) = setupTest()
    submit(reduceRdd, Array(0, 1))

    val mapStatus = makeAsyncMapStatus("hostA")
    complete(taskSets(0), Seq((Success, mapStatus), (Success, mapStatus)))

    // the 2nd ResultTask failed. This removes the first executor, but the
    // other task is still intact because it was uploaded to the remove dfs
    complete(taskSets(1), Seq(
      (Success, 42),
      (FetchFailed(makeAsyncShuffleLocation("hostA"),
        shuffleId, 0, 0, Some(makeBlockManagerId("hostA")), "ignored"), null)))
    assert(scheduler.failedStages.size > 0)
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 1)
    assertMapOutputTrackerContains(shuffleId, Seq(null, new AsyncMapShuffleLocations(null)))

    // submit both mappers once more
    scheduler.resubmitFailedStages()
    complete(taskSets(2), Seq((Success, mapStatus)))
    assert(mapOutputTracker.getNumAvailableOutputs(shuffleId) == 2)
    assertMapOutputTrackerContains(shuffleId, Seq(
      mapStatus.mapShuffleLocations, mapStatus.mapShuffleLocations))

    // submit last reduce task
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

  def makeAsyncMapStatus(
                          host: String,
                          reduces: Int = 2,
                          execId: Optional[String] = Optional.empty(),
                          sizes: Byte = 2): MapStatus = {
    MapStatus(makeBlockManagerId(host),
      new AsyncMapShuffleLocations(makeAsyncShuffleLocation(host)),
      Array.fill[Long](reduces)(sizes))
  }

  def makeAsyncShuffleLocation(host: String): AsyncShuffleLocation = {
    new AsyncShuffleLocation(host, 12345)
  }

  def makeBlockManagerId(host: String): BlockManagerId =
    BlockManagerId("exec-" + host, host, 12345)

}