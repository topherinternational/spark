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

package org.apache.spark.palantir.shuffle.async.metadata

import java.lang.{Integer => JInt}
import java.util.{Map => JMap}
import java.util.concurrent.atomic.AtomicLong

import scala.compat.java8.FunctionConverters._

import com.google.common.collect.{ImmutableMap, Maps}
import com.palantir.logsafe.SafeArg
import com.palantir.logsafe.exceptions.SafeIllegalStateException

import org.apache.spark.storage.BlockManagerId

class ShuffleStorageStateTracker {

  import ShuffleStorageStateTracker._

  private val states = Maps.newConcurrentMap[JInt, JMap[MapOutputId, ShuffleStorageState]]
  private val nextMergeId = new AtomicLong(0L)

  def getNextMergeId: Long = nextMergeId.getAndIncrement()

  def registerShuffle(shuffleId: Int): Unit = {
    states.putIfAbsent(shuffleId, Maps.newConcurrentMap[MapOutputId, ShuffleStorageState])
  }

  def unregisterShuffle(shuffleId: Int): Unit = {
    states.remove(shuffleId)
  }

  def registerLocallyWrittenMapOutput(
      executorLocation: BlockManagerId,
      mapOutputId: MapOutputId): Unit = {
    updateStorageState(
      mapOutputId,
      OnExecutorOnly(executorLocation),
      prev => {
        throw new SafeIllegalStateException(
          "Map output should only be registered as locally written as an initial state.",
          mapOutputIdArg(mapOutputId),
          invalidPrevStateArg(prev)
        )
      })
  }

  def registerUnmergedBackedUpMapOutput(mapOutputId: MapOutputId): Unit = {
    updateStorageState(
      mapOutputId,
      defaultStateFunc = {
        throw mapOutputNotWrittenLocallyBeforeBackedUp(mapOutputId)
      },
      nextStateFunc = {
        case OnExecutorOnly(executorLocation) => OnExecutorAndRemote(executorLocation)
        case invalidPrevState: ShuffleStorageState =>
          throw mapOutputNotWrittenLocallyBeforeBackedUp(mapOutputId, Some(invalidPrevState))
      })
  }

  def registerMergedBackedUpMapOutput(mapOutputId: MapOutputId, mergeId: Long): Unit = {
    updateStorageState(
      mapOutputId,
      defaultStateFunc = {
        throw mapOutputNotWrittenLocallyBeforeBackedUp(mapOutputId)
      },
      nextStateFunc = {
        case OnExecutorOnly(executorLocation) => OnExecutorAndRemote(executorLocation, mergeId)
        case invalidPrevState: ShuffleStorageState =>
          throw mapOutputNotWrittenLocallyBeforeBackedUp(mapOutputId, Some(invalidPrevState))
      })
  }

  def markLocalMapOutputAsDeleted(mapOutputId: MapOutputId): Unit = {
    updateStorageState(
      mapOutputId,
      defaultStateFunc = {
        throw mapOutputNotBackedUpBeforeDeletedLocally(mapOutputId)
      },
      nextStateFunc = {
        case OnExecutorAndRemote(_, mergeId) => OnRemoteOnly(mergeId)
        case invalidPrevState: ShuffleStorageState =>
          throw mapOutputNotBackedUpBeforeDeletedLocally(mapOutputId, Some(invalidPrevState))
      })
  }

  def isShuffleRegistered(shuffleId: Int): Boolean = states.containsKey(shuffleId)

  def getShuffleStorageState(mapOutputId: MapOutputId): ShuffleStorageState = {
    states.getOrDefault(mapOutputId.shuffleId, NO_MAP_OUTPUTS)
      .getOrDefault(mapOutputId, Unregistered)
  }

  def getShuffleStorageStates(shuffleId: Int): JMap[MapOutputId, ShuffleStorageState] = {
    ImmutableMap.copyOf(states.getOrDefault(shuffleId, NO_MAP_OUTPUTS))
  }

  def blacklistExecutor(blacklistedLocation: BlockManagerId): Unit = {
    states.replaceAll(
      asJavaBiFunction { (_: JInt, mapOutputs: JMap[MapOutputId, ShuffleStorageState]) =>
        mapOutputs.replaceAll(
          asJavaBiFunction { (_, state) =>
            state match {
              case storedOnExecutor: StoredOnExecutor
                  if storedOnExecutor.executorLocation == blacklistedLocation =>
                getNextStateWhenLocationRemoved(storedOnExecutor)
              case other: ShuffleStorageState => other
            }
          })
        mapOutputs
      })
  }

  private def getNextStateWhenLocationRemoved(prevState: StoredOnExecutor): ShuffleStorageState = {
    prevState match {
      case OnExecutorOnly(_) => Unregistered
      case OnExecutorAndRemote(_, mergeId) => OnRemoteOnly(mergeId)
    }
  }

  private def updateStorageState(
      mapOutputId: MapOutputId,
      defaultStateFunc: => ShuffleStorageState,
      nextStateFunc: ShuffleStorageState => ShuffleStorageState): Unit = {
    states.computeIfPresent(
      mapOutputId.shuffleId,
      asJavaBiFunction { (_, mapOutputs: JMap[MapOutputId, ShuffleStorageState]) =>
        mapOutputs.compute(
          mapOutputId,
          asJavaBiFunction { (_, prevState) =>
            Option(prevState)
              .map(nextStateFunc)
              .getOrElse(defaultStateFunc)
          })
        mapOutputs
      })
  }
}

private object ShuffleStorageStateTracker {
  private val NO_MAP_OUTPUTS = ImmutableMap.of[MapOutputId, ShuffleStorageState]()

  def mapOutputNotWrittenLocallyBeforeBackedUp(
      mapOutputId: MapOutputId,
      invalidPrevState: Option[ShuffleStorageState] = None): SafeIllegalStateException = {
    val errMsg = "Map output should not be initially marked as backed up; should be registered" +
      " as being on the executor only first."
    invalidPrevState.map { prev =>
      new SafeIllegalStateException(
        errMsg,
        mapOutputIdArg(mapOutputId),
        invalidPrevStateArg(prev))
    }.getOrElse(new SafeIllegalStateException(errMsg, mapOutputIdArg(mapOutputId)))
  }

  def mapOutputNotBackedUpBeforeDeletedLocally(
      mapOutputId: MapOutputId,
      invalidPrevState: Option[ShuffleStorageState] = None): SafeIllegalStateException = {
    val errMsg = "Map output should not be marked as deleted before it was marked as being" +
      " backed up."
    invalidPrevState.map { prev =>
      new SafeIllegalStateException(
        errMsg,
        mapOutputIdArg(mapOutputId),
        invalidPrevStateArg(prev))
    }.getOrElse(new SafeIllegalStateException(errMsg, mapOutputIdArg(mapOutputId)))
  }

  def mapOutputIdArg(mapOutputId: MapOutputId): SafeArg[MapOutputId] = {
    SafeArg.of("mapOutputId", mapOutputId)
  }

  def invalidPrevStateArg(invalidPrevState: ShuffleStorageState): SafeArg[ShuffleStorageState] = {
    SafeArg.of("invalidPrevState", invalidPrevState)
  }
}
