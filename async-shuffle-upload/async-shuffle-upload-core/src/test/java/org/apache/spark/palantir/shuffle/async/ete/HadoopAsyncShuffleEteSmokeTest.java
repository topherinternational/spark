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

package org.apache.spark.palantir.shuffle.async.ete;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import scala.Tuple2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.palantir.shuffle.async.AsyncShuffleDataIoSparkConfigs;
import org.apache.spark.palantir.shuffle.async.io.HadoopAsyncShuffleDataIo;

/**
 * Simple test that just uses the plugin, but doesn't attempt to do anything
 * else fancy with it.
 */
public final class HadoopAsyncShuffleEteSmokeTest implements Serializable {

  private static JavaSparkContext sc;

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static synchronized void beforeClass() throws IOException {
    File shuffleBackupFolder = tempFolder.newFolder("shuffle-backups");
    sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(
        new SparkConf()
            .setMaster("local[4]")
            .setAppName("hadoop-async-ete-smoke-test")
            .set(AsyncShuffleDataIoSparkConfigs.BASE_URI(),
                "file://" + shuffleBackupFolder.getAbsolutePath())
            .set("spark.shuffle.sort.io.storage.plugin.class.v2",
                HadoopAsyncShuffleDataIo.class.getName())));
  }

  @Test
  public void basicTest() {
    Assertions.assertThat(sc.parallelizePairs(ImmutableList.of(
        Tuple2.apply(10, 15),
        Tuple2.apply(10, 13),
        Tuple2.apply(10, 2),
        Tuple2.apply(13, 23),
        Tuple2.apply(13, 25),
        Tuple2.apply(15, 1)))
        .combineByKey(
            v -> v,
            (v1, v2) -> v1 + v2,
            (v1, v2) -> v1 + v2)
        .collectAsMap())
        .isEqualTo(
            ImmutableMap.of(
                10, 15 + 13 + 2,
                13, 23 + 25,
                15, 1));
  }
}
