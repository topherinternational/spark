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

package org.apache.spark.api.shuffle;

import org.apache.spark.api.java.Optional;

/**
 * Marker interface representing a location of a shuffle block. Implementations of shuffle readers
 * and writers are expected to cast this down to an implementation-specific representation.
 */
public abstract class ShuffleLocation {
  /**
   * The host and port on which the shuffle block is located.
   */
  public abstract String host();
  public abstract int port();

  /**
   * The executor on which the ShuffleLocation is located. Returns {@link Optional#empty()} if
   * location is not associated with an executor.
   */
  public Optional<String> execId() {
    return Optional.empty();
  }

  @Override
  public String toString() {
    String shuffleLocation = String.format("ShuffleLocation %s:%d", host(), port());
    if (execId().isPresent()) {
      return String.format("%s (execId: %s)", shuffleLocation, execId().get());
    }
    return shuffleLocation;
  }
}
