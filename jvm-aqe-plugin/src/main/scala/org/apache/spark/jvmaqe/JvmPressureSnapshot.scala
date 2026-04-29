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

package org.apache.spark.jvmaqe

@SerialVersionUID(1L)
case class JvmPressureSnapshot(
    executorId: String,
    host: String,
    timestampMs: Long,
    heapUsed: Long,
    heapMax: Long,
    nonHeapUsed: Long,
    nonHeapMax: Long,
    totalGcCount: Long,
    totalGcTimeMs: Long,
    recentGcTimeRatio: Double,
    lastGcName: String,
    lastGcCause: String,
    lastGcAction: String,
    lastGcDurationMs: Long,
    lastGcHeapBefore: Long,
    lastGcHeapAfter: Long,
    lastGcReclaimedBytes: Long) extends Serializable {

  def heapUsedRatio: Double = {
    if (heapMax <= 0L) 0.0d else heapUsed.toDouble / heapMax.toDouble
  }

  def heapFreeBytes: Long = {
    if (heapMax <= 0L) 0L else math.max(0L, heapMax - heapUsed)
  }

  def compactString: String = {
    f"executor=$executorId host=$host heap=${heapUsed / MiB}MB/${heapMax / MiB}MB " +
      f"heapRatio=$heapUsedRatio%.4f nonHeap=${nonHeapUsed / MiB}MB " +
      f"gcCount=$totalGcCount gcMs=$totalGcTimeMs recentGc=$recentGcTimeRatio%.4f " +
      s"lastGc=$lastGcName cause=$lastGcCause action=$lastGcAction " +
      s"lastDurationMs=$lastGcDurationMs reclaimed=${lastGcReclaimedBytes / MiB}MB"
  }

  private def MiB: Long = 1024L * 1024L
}
