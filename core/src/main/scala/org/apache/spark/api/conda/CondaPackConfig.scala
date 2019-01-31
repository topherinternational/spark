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

package org.apache.spark.api.conda

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{CONDA_PACK_COMPRESS_LEVEL, CONDA_PACK_FORMAT, CONDA_PACK_NUM_THREADS, CONDA_USE_PACK}

final case class CondaPackConfig(format: String, compressLevel: Int, numThreads: Int) {
}

object CondaPackConfig {
  def fromConf(sparkConf: SparkConf): Option[CondaPackConfig] = {

    if (!sparkConf.get(CONDA_USE_PACK)) {
      return Option.empty
    }
    val format = sparkConf.get(CONDA_PACK_FORMAT)
    val compressLevel = sparkConf.get(CONDA_PACK_COMPRESS_LEVEL)
    val numThreads = sparkConf.get(CONDA_PACK_NUM_THREADS)
    Option(new CondaPackConfig(format, compressLevel, numThreads))
  }
}
