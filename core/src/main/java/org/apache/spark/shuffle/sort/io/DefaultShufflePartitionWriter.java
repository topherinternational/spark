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

import org.apache.spark.api.shuffle.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class DefaultShufflePartitionWriter implements ShufflePartitionWriter {

  private static final Logger log =
      LoggerFactory.getLogger(DefaultShufflePartitionWriter.class);

  private final DefaultShuffleBlockOutputStream stream;

  public DefaultShufflePartitionWriter(
      DefaultShuffleBlockOutputStream stream) {
    this.stream = stream;
  }

  @Override
  public OutputStream openStream() throws IOException {
    return stream;
  }

  @Override
  public long getLength() {
    try {
      stream.close();
    } catch (Exception e) {
      log.error("Error with closing stream for partition", e);
    }
    return stream.getCount();
  }
}
