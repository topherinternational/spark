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

package org.apache.spark.shuffle.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.spark.annotation.Experimental;

/**
 * :: Experimental ::
 * An interface for building shuffle support for Executors
 *
 * @since 3.0.0
 */
@Experimental
public interface ShuffleExecutorComponents {
  void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs);

  ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      int mapId,
      long mapTaskAttemptId,
      int numPartitions) throws IOException;

  /**
   * Returns an underlying {@link Iterable<InputStream>} that will iterate
   * through shuffle data, given an iterable for the shuffle blocks to fetch.
   */
  Iterable<InputStream> getPartitionReaders(Iterable<ShuffleBlockInfo> blockMetadata)
      throws IOException;

  default boolean shouldWrapPartitionReaderStream() {
    return true;
  }
}
