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

package org.apache.spark

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.ArrayBuffer
import scala.compat.java8.OptionConverters._

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.{ExternalOutput, ExternalShuffleMetadata, LocalOnlyOutput, ShuffleDataAttemptId, TestPluginShuffleDriverComponents}
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.storage.BlockManagerId

class MapOutputTrackerShufflePluginSuite extends SparkFunSuite with BeforeAndAfterEach {
  import MapOutputTrackerShufflePluginSuite._

  private val conf = new SparkConf
  private val currentPartSize = new AtomicLong(2L)
  private var shuffleDriverComponents: ShuffleDriverComponents = _
  private var master: MapOutputTrackerMaster = _
  private var worker: MapOutputTrackerWorker = _
  private var rpcEnv: RpcEnv = _

  override def beforeEach() {
    currentPartSize.set(2L)
    shuffleDriverComponents = new TestPluginShuffleDriverComponents()
    rpcEnv = newRpcEnv("test")
    master = newTrackerMaster(rpcEnv, shuffleDriverComponents)
    worker = newTrackerWorker(rpcEnv)
  }

  override def afterEach(): Unit = {
    worker.stop()
    master.stop()
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
  }

  test("Serializing shuffle metadata if present.") {
    master.registerShuffle(SHUFFLE_ID, NUM_MAPS)
    val stat1 = createMapStatus(0L)
    val stat2 = createMapStatus(1L)
    master.registerMapOutput(SHUFFLE_ID, 0, stat1, Some(ExternalOutput))
    master.registerMapOutput(SHUFFLE_ID, 1, stat2, Some(LocalOnlyOutput))
    val metadata = worker.getPartitionMetadata(
      0, 0, 2)
    assert(metadata.shuffleMetadata.isDefined)
    assert(metadata.shuffleMetadata.get.isInstanceOf[ExternalShuffleMetadata])
    assert(metadata.shuffleMetadata.get.asInstanceOf[ExternalShuffleMetadata].externalOutputs
        === Set(ShuffleDataAttemptId(0, 0, stat1.mapTaskAttemptId)))
  }

  test("Caching metadata until epoch updates.") {
    master.registerShuffle(SHUFFLE_ID, NUM_MAPS)
    val stat1 = createMapStatus(0L)
    val stat2 = createMapStatus(1L)
    master.registerMapOutput(SHUFFLE_ID, 0, stat1, Some(ExternalOutput))
    master.registerMapOutput(SHUFFLE_ID, 1, stat2, Some(LocalOnlyOutput))
    var metadata: PartitionMetadata = worker.getPartitionMetadata(0, 0, 2)
    master.registerMapOutput(SHUFFLE_ID, 1, stat2, Some(ExternalOutput))
    metadata = worker.getPartitionMetadata(
      0, 0, 2)
    assert(metadata.shuffleMetadata.get.asInstanceOf[ExternalShuffleMetadata].externalOutputs
      === Set(ShuffleDataAttemptId(0, 0, stat1.mapTaskAttemptId)))
    master.incrementEpoch()
    worker.updateEpoch(master.getEpoch)
    metadata = worker.getPartitionMetadata(0, 0, 2)
    assert(metadata.shuffleMetadata.get.asInstanceOf[ExternalShuffleMetadata].externalOutputs
      === Set(
      ShuffleDataAttemptId(0, 0, stat1.mapTaskAttemptId),
      ShuffleDataAttemptId(0, 1, stat2.mapTaskAttemptId)))
  }

  private def newTrackerMaster(env: RpcEnv, driverComponents: ShuffleDriverComponents) = {
    val broadcastManager = new BroadcastManager(true, conf,
      new SecurityManager(conf))
    val master = new MapOutputTrackerMaster(
      conf,
      broadcastManager,
      true)
    master.setShuffleOutputTracker(driverComponents.shuffleTracker().asScala)
    val endpoint = env.setupEndpoint(
      MASTER_ENDPOINT_NAME, new MapOutputTrackerMasterEndpoint(env, master, conf))
    master.trackerEndpoint = endpoint
    master
  }

  private def newTrackerWorker(rpcEnv: RpcEnv): MapOutputTrackerWorker = {
    val worker = new MapOutputTrackerWorker(conf)
    val endpointRef = rpcEnv.setupEndpointRef(rpcEnv.address, MASTER_ENDPOINT_NAME)
    worker.trackerEndpoint = endpointRef
    worker
  }

  private def newRpcEnv(name: String, host: String = "localhost", port: Int = 0,
      securityManager: SecurityManager = new SecurityManager(conf)): RpcEnv = {
    RpcEnv.create(name, host, port, conf, securityManager)
  }

  private def createMapStatus(attemptId: Long): MapStatus = {
    val sizes = ArrayBuffer[Long]()
    for (index <- 0 until NUM_PARTS) {
      sizes += index * currentPartSize.get()
    }
    currentPartSize.addAndGet(1L)
    MapStatus(BLOCK_MANAGER_ID, sizes.toArray, attemptId)
  }
}

object MapOutputTrackerShufflePluginSuite {
  val MASTER_ENDPOINT_NAME = "trackerTestEndpoint"
  val SHUFFLE_ID = 0
  val NUM_MAPS = 2
  val NUM_PARTS = 10
  val BLOCK_MANAGER_ID = BlockManagerId("localhost", 7077)
}
