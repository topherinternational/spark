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

package org.apache.spark.palantir.shuffle.async.metadata;

import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.palantir.logsafe.SafeArg;

import org.apache.spark.shuffle.api.MapOutputMetadata;
import org.apache.spark.shuffle.api.ShuffleBlockMetadata;
import org.apache.spark.shuffle.api.ShuffleMetadata;
import org.apache.spark.shuffle.api.ShuffleOutputTracker;
import org.apache.spark.storage.BlockManagerId;

public final class HadoopAsyncShuffleOutputTracker implements ShuffleOutputTracker {

  private final ShuffleStorageStateTracker storageStateTracker;

  public HadoopAsyncShuffleOutputTracker(ShuffleStorageStateTracker storageStateTracker) {
    this.storageStateTracker = storageStateTracker;
  }

  @Override
  public void registerShuffle(int shuffleId, int numMaps) {
    storageStateTracker.registerShuffle(shuffleId);
  }

  @Override
  public void registerMapOutput(int shuffleId, int mapId, long mapAttemptId, Optional<MapOutputMetadata> metadata) {
    Preconditions.checkArgument(
        metadata.isPresent(),
        "Expecting the Hadoop async shuffle output tracker to receive map output metadata.",
        SafeArg.of("shuffleId", shuffleId),
        SafeArg.of("mapId", mapId),
        SafeArg.of("mapAttemptId", mapAttemptId));
    Preconditions.checkArgument(
        metadata.get() instanceof MapperLocationMetadata,
        "Shuffle map output metadata provided is of the incorrect type.",
        SafeArg.of("expectedClass", MapperLocationMetadata.class),
        SafeArg.of("actualClass", metadata.get().getClass()));
    storageStateTracker.registerLocallyWrittenMapOutput(
        ((MapperLocationMetadata) metadata.get()).mapperLocation(),
        new MapOutputId(shuffleId, mapId, mapAttemptId));
  }

  @Override
  public void handleFetchFailure(
      int shuffleId,
      int mapId,
      long mapAttemptId,
      long partitionId,
      Optional<ShuffleBlockMetadata> blockMetadata) {
    Preconditions.checkArgument(
        blockMetadata.isPresent(),
        "Expecting the Hadoop async shuffle output tracker to receive shuffle block metadata.",
        SafeArg.of("shuffleId", shuffleId),
        SafeArg.of("mapId", mapId),
        SafeArg.of("mapAttemptId", mapAttemptId));
    Preconditions.checkArgument(
        blockMetadata.get() instanceof MapperLocationMetadata,
        "Shuffle map output metadata provided is of the incorrect type.",
        SafeArg.of("expectedClass", MapperLocationMetadata.class),
        SafeArg.of("actualClass", blockMetadata.get().getClass()));
    storageStateTracker.blacklistExecutor(
        ((MapperLocationMetadata) blockMetadata.get()).mapperLocation());
  }

  @Override
  public void invalidateShuffle(int shuffleId) {}

  @Override
  public void unregisterShuffle(int shuffleId) {
    storageStateTracker.unregisterShuffle(shuffleId);
  }

  @Override
  public Optional<ShuffleMetadata> shuffleMetadata(int shuffleId) {
    return Optional.of(
        new HadoopAsyncShuffleMetadata(
            storageStateTracker.getShuffleStorageStates(shuffleId)));
  }

  @Override
  public boolean areAllPartitionsAvailableExternally(int shuffleId, int mapId, long mapAttemptId) {
    MapOutputId id = new MapOutputId(shuffleId, mapId, mapAttemptId);
    return storageStateTracker.getShuffleStorageState(id).visit(
        new ShuffleStorageStateVisitor<Boolean>() {
          @Override
          public Boolean onExecutorAndRemote(BlockManagerId executorLocation, Optional<Long> mergeId) {
            return true;
          }

          @Override
          public Boolean onExecutorOnly(BlockManagerId executorLocation) {
            return false;
          }

          @Override
          public Boolean unregistered() {
            return false;
          }

          @Override
          public Boolean onRemoteOnly(Optional<Long> mergeId) {
            return true;
          }
        });
  }

  @Override
  public List<String> preferredMapOutputLocations(int shuffleId, int mapId) {
    return ImmutableList.of();
  }

  @Override
  public List<String> preferredPartitionLocations(int shuffleId, int mapId) {
    return ImmutableList.of();
  }
}
