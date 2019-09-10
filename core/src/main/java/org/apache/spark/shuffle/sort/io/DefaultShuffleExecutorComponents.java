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

package org.apache.spark.shuffle.sort.io;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.api.ShuffleBlockInfo;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.io.DefaultShuffleReadSupport;
import org.apache.spark.storage.BlockManager;

import java.util.Map;

public class DefaultShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  // Submodule for the read side for shuffles - implemented in Scala for ease of
  // compatibility with previously written code.
  private DefaultShuffleReadSupport shuffleReadSupport;
  private BlockManager blockManager;
  private IndexShuffleBlockResolver blockResolver;

  public DefaultShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @VisibleForTesting
  public DefaultShuffleExecutorComponents(
      SparkConf sparkConf,
      BlockManager blockManager,
      MapOutputTracker mapOutputTracker,
      SerializerManager serializerManager,
      IndexShuffleBlockResolver blockResolver) {
    this.sparkConf = sparkConf;
    this.blockManager = blockManager;
    this.blockResolver = blockResolver;
    this.shuffleReadSupport = new DefaultShuffleReadSupport(
        blockManager, mapOutputTracker, serializerManager, sparkConf);
  }

  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    blockManager = SparkEnv.get().blockManager();
    MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
    SerializerManager serializerManager = SparkEnv.get().serializerManager();
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
    shuffleReadSupport = new DefaultShuffleReadSupport(
        blockManager, mapOutputTracker, serializerManager, sparkConf);
  }

  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(int shuffleId, int mapId, long mapTaskAttemptId, int numPartitions) throws IOException {
    checkInitialized();
    return new DefaultShuffleMapOutputWriter(
        shuffleId,
        mapId,
        numPartitions,
        blockManager.shuffleServerId(),
        TaskContext.get().taskMetrics().shuffleWriteMetrics(), blockResolver, sparkConf);
  }

  @Override
  public Iterable<InputStream> getPartitionReaders(Iterable<ShuffleBlockInfo> blockMetadata) throws IOException {
    return shuffleReadSupport.getPartitionReaders(blockMetadata);
  }

  @Override
  public boolean shouldWrapPartitionReaderStream() {
    return false;
  }

  private void checkInitialized() {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
  }
}
