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

package org.apache.spark.shuffle.sort.io

import java.io._

import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mock
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.when
import org.mockito.MockitoAnnotations
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.Utils


class DefaultShuffleMapOutputWriterSuite extends SparkFunSuite with BeforeAndAfterEach {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _

  private val NUM_PARTITIONS = 4
  private val D_LEN = 10
  private val data: Array[Array[Int]] =
    (0 until NUM_PARTITIONS).map { _ => (1 to D_LEN).toArray }.toArray
  private var mergedOutputFile: File = _
  private var tempDir: File = _
  private var partitionSizesInMergedFile: Array[Long] = _
  private var conf: SparkConf = _
  private var taskMetrics: TaskMetrics = _
  private var metricsReporter: ShuffleWriteMetricsReporter = _
  private var mapOutputWriter: DefaultShuffleMapOutputWriter = _

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  override def beforeEach(): Unit = {
    MockitoAnnotations.initMocks(this)
    tempDir = Utils.createTempDir(null, "test")
    mergedOutputFile = File.createTempFile("mergedoutput", "", tempDir)
    partitionSizesInMergedFile = null
    conf = new SparkConf()
      .set("spark.app.id", "example.spark.app")
      .set("spark.shuffle.unsafe.file.output.buffer", "16k")
    taskMetrics = new TaskMetrics
    metricsReporter = taskMetrics.shuffleWriteMetrics
    when(blockResolver.getDataFile(anyInt, anyInt)).thenReturn(mergedOutputFile)

    doAnswer(new Answer[Void] {
      def answer(invocationOnMock: InvocationOnMock): Void = {
        partitionSizesInMergedFile = invocationOnMock.getArguments()(2).asInstanceOf[Array[Long]]
        val tmp: File = invocationOnMock.getArguments()(3).asInstanceOf[File]
        if (tmp != null) {
          mergedOutputFile.delete
          tmp.renameTo(mergedOutputFile)
        }
        null
      }
    }).when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyInt, any(classOf[Array[Long]]), any(classOf[File]))
    mapOutputWriter = new DefaultShuffleMapOutputWriter(
      0, 0, NUM_PARTITIONS, metricsReporter, blockResolver, conf)
  }

  private def readRecordsFromFile(): Array[Array[Int]] = {
    val startOffset = 0L
    val result = new Array[Array[Int]](NUM_PARTITIONS)
    (0 until NUM_PARTITIONS).foreach { p =>
      val partitionSize = partitionSizesInMergedFile(p)
      val inner = new Array[Int](D_LEN)
      if (partitionSize > 0) {
        val in = new FileInputStream(mergedOutputFile)
        in.getChannel.position(startOffset)
        val lin = new LimitedInputStream(in, partitionSize)
        var nonEmpty = true
        var count = 0
        while (nonEmpty & count < partitionSize) {
          try {
            inner(count) = lin.read()
            count += 1
          } catch {
            case eof: EOFException =>
              nonEmpty = false
          }
        }
      }
      result(p) = inner
    }
    result
  }

  test("writing to outputstream") {
    (0 until NUM_PARTITIONS).foreach{ p =>
      val writer = mapOutputWriter.getNextPartitionWriter
      val stream = writer.openStream()
      data(p).foreach { i => stream.write(i)}
      stream.close()
      intercept[IllegalStateException] {
        stream.write(p)
      }
      assert(writer.getLength == D_LEN)
    }
    mapOutputWriter.commitAllPartitions()
    val partitionLengths = (0 until NUM_PARTITIONS).map { _ => D_LEN.toDouble}.toArray
    assert(partitionSizesInMergedFile === partitionLengths)
    assert(mergedOutputFile.length() === partitionLengths.sum)
    assert(data === readRecordsFromFile())
  }
}
