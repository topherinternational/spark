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

import org.apache.spark.annotation.Private;

import java.io.IOException;
import java.util.Map;

/**
 * :: Private ::
 * An interface for building shuffle support modules for the Driver.
 */
@Private
public interface ShuffleDriverComponents {

  /**
   * Called once in the driver to bootstrap this module that is specific
   * to this application.
   *
   * @return additional SparkConf settings necessary for initializing the executors.
   */
  Map<String, String> initializeApplication();

  /**
   * Called once at the end of the Spark application to clean up any existing shuffle state.
   */
  default void cleanupApplication() throws IOException {}

  default void registerShuffle(int shuffleId) throws IOException {}

  /**
   * Removes shuffle data associated with the given shuffle Id.
   *
   * @param shuffleId The unique identifier for the shuffle stage.
   * @param blocking Whether this call should block on the deletion of the data.
   */
  void removeShuffle(int shuffleId, boolean blocking) throws IOException;
}
