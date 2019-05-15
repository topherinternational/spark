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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{RangeExec, UnionExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class QueryStageTest extends SharedSQLContext {
  test("Adaptive Query Execution repartitions") {
    val originalNumPartitions = 100

    val plan = {
      val leftRangeExec = RangeExec(
        org.apache.spark.sql.catalyst.plans.logical.Range(1, 1000, 1, 1))

      ShuffleExchangeExec(
        HashPartitioning(leftRangeExec.output, originalNumPartitions),
        leftRangeExec)
    }

    assert(plan.execute().getNumPartitions == originalNumPartitions)
    assert(PlanQueryStage.apply(new SQLConf)(plan).execute().getNumPartitions == 1)
  }

  test("Works on unions when children have different number of partitions") {
    val union = UnionExec(Seq(
      ShuffleExchangeExec(
        HashPartitioning(Seq(), 100),
        RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(1, 1000, 1, 1))),
      ShuffleExchangeExec(
        HashPartitioning(Seq(), 500),
        RangeExec(org.apache.spark.sql.catalyst.plans.logical.Range(1, 1000, 1, 1)))
    ))
    val rdd = PlanQueryStage.apply(new SQLConf)(union).execute()
    assert(rdd.getNumPartitions == 600)
  }
}
