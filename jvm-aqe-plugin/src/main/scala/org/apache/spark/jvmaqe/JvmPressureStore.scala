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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

object JvmPressureStore {
  private val snapshots = new ConcurrentHashMap[String, JvmPressureSnapshot]()
  private val pressureSamples = new ConcurrentHashMap[String, ConcurrentLinkedDeque[PressureSample]]()
  private val updates = new AtomicLong(0L)
  private val maxSampleRetentionMs = 300000L
  private val maxSamplesPerExecutor = 512
  private val defaultHighWatermarkWindowMs = 30000L

  def reset(): Unit = {
    snapshots.clear()
    pressureSamples.clear()
    updates.set(0L)
  }

  def update(snapshot: JvmPressureSnapshot): Unit = {
    snapshots.put(snapshot.executorId, snapshot)
    val samples = pressureSamples.computeIfAbsent(
      snapshot.executorId,
      _ => new ConcurrentLinkedDeque[PressureSample]())
    samples.addLast(PressureSample(pressureOf(snapshot), snapshot.timestampMs,
      snapshot.executorId, snapshot.heapUsed))
    trimSamples(samples, snapshot.timestampMs)
    updates.incrementAndGet()
  }

  def snapshotFor(executorId: String): Option[JvmPressureSnapshot] = {
    Option(snapshots.get(executorId))
  }

  def allSnapshots: Seq[JvmPressureSnapshot] = {
    snapshots.values().asScala.toSeq
  }

  def latestJvmPressure: Option[JvmPressureSnapshot] = {
    mostPressured(executorSnapshotsOrAll)
  }

  def isBroadcastRisky(
      expectedBytes: Long,
      expansionFactor: Double,
      heapSafetyFraction: Double): Boolean = {
    isBroadcastRisky(expectedBytes, expansionFactor, heapSafetyFraction, Long.MaxValue)
  }

  def isBroadcastRisky(
      expectedBytes: Long,
      expansionFactor: Double,
      heapSafetyFraction: Double,
      staleThresholdMs: Long): Boolean = {
    isBroadcastRisky(expectedBytes, expansionFactor, heapSafetyFraction,
      staleThresholdMs, JvmPressureStore.defaultHighWatermarkWindowMs)
  }

  // Uses sliding-window high-watermark of heapUsed for determinism across heap-valley reads.
  def isBroadcastRisky(
      expectedBytes: Long,
      expansionFactor: Double,
      heapSafetyFraction: Double,
      staleThresholdMs: Long,
      highWatermarkWindowMs: Long): Boolean = {
    val expandedBytes = safeMultiply(expectedBytes, expansionFactor)
    val safeFraction = math.max(0.0d, math.min(1.0d, heapSafetyFraction))
    val now = System.currentTimeMillis()
    val heapSnapshots = executorSnapshotsOrAll.filter(_.heapMax > 0L)
    val freshSnapshots = heapSnapshots.filterNot(isStale(_, now, staleThresholdMs))
    val candidates = if (freshSnapshots.nonEmpty) freshSnapshots else heapSnapshots
    candidates.exists { snapshot =>
      val windowMaxHeapUsed = highWatermarkHeapUsed(snapshot.executorId, now, highWatermarkWindowMs)
      val effectiveHeapUsed = math.max(snapshot.heapUsed, windowMaxHeapUsed)
      val projectedHeapUsed = effectiveHeapUsed.toDouble + expandedBytes
      projectedHeapUsed > snapshot.heapMax.toDouble * safeFraction
    }
  }

  def effectivePressure(shuffleIds: Seq[Int]): Double = {
    currentPressureView(Long.MaxValue).pressure
  }

  def summary(shuffleIds: Seq[Int]): String = summaryString

  def currentPressureView(staleThresholdMs: Long): PressureView = {
    currentPressureView(staleThresholdMs, staleThresholdMs)
  }

  def currentPressureView(staleThresholdMs: Long, highWatermarkWindowMs: Long): PressureView = {
    val now = System.currentTimeMillis()
    val current = allSnapshots
    val executorSnapshots = current.filterNot(_.executorId == "driver")
    val baseCandidates = if (executorSnapshots.nonEmpty) executorSnapshots else current
    val freshCandidates = baseCandidates.filterNot(isStale(_, now, staleThresholdMs))
    val candidates = if (freshCandidates.nonEmpty) freshCandidates else baseCandidates
    val selected = mostPressured(candidates)
    val ageMs = selected.map(snapshot => math.max(0L, now - snapshot.timestampMs))
    val stale = selected.forall(snapshot => isStale(snapshot, now, staleThresholdMs))
    val currentPressure = selected.map(pressureOf).getOrElse(0.0d)
    val highWatermark = recentHighWatermark(baseCandidates.map(_.executorId).toSet,
      now, highWatermarkWindowMs)
    val pressure = math.max(currentPressure, highWatermark.map(_.pressure).getOrElse(0.0d))

    PressureView(
      pressure = pressure,
      selected = selected,
      selectedAgeMs = ageMs,
      stale = stale,
      staleThresholdMs = staleThresholdMs,
      highWatermarkPressure = highWatermark.map(_.pressure).getOrElse(0.0d),
      highWatermarkAgeMs = highWatermark.map(sample => math.max(0L, now - sample.timestampMs)),
      highWatermarkWindowMs = highWatermarkWindowMs,
      executorSnapshots = executorSnapshots.size,
      totalSnapshots = current.size,
      updates = updates.get())
  }

  def summaryString: String = {
    val ordered = allSnapshots.sortBy(_.executorId)
    val body = ordered.take(8).map(_.compactString).mkString("; ")
    s"updates=${updates.get()}, executors=${ordered.size}, latest=${latestJvmPressure.map(
      _.executorId).getOrElse("none")}, sample=[$body]"
  }

  def updateCount: Long = updates.get()

  private def safeMultiply(bytes: Long, factor: Double): Double = {
    if (bytes <= 0L || factor <= 0.0d || factor.isNaN) {
      0.0d
    } else {
      math.min(Long.MaxValue.toDouble, bytes.toDouble * factor)
    }
  }

  private def executorSnapshotsOrAll: Seq[JvmPressureSnapshot] = {
    val current = allSnapshots
    val executors = current.filterNot(_.executorId == "driver")
    if (executors.nonEmpty) executors else current
  }

  private def mostPressured(
      current: Seq[JvmPressureSnapshot]): Option[JvmPressureSnapshot] = {
    if (current.isEmpty) {
      None
    } else {
      Some(current.maxBy(pressureSortKey))
    }
  }

  private def pressureSortKey(snapshot: JvmPressureSnapshot): Double = {
    snapshot.heapUsedRatio + clamp(snapshot.recentGcTimeRatio)
  }

  private def pressureOf(snapshot: JvmPressureSnapshot): Double = {
    math.max(snapshot.heapUsedRatio, clamp(snapshot.recentGcTimeRatio))
  }

  private def isStale(
      snapshot: JvmPressureSnapshot,
      nowMs: Long,
      staleThresholdMs: Long): Boolean = {
    staleThresholdMs >= 0L &&
      staleThresholdMs < Long.MaxValue &&
      nowMs - snapshot.timestampMs > staleThresholdMs
  }

  private def highWatermarkHeapUsed(
      executorId: String,
      nowMs: Long,
      windowMs: Long): Long = {
    if (windowMs <= 0L) {
      0L
    } else {
      val deque = pressureSamples.get(executorId)
      if (deque == null) {
        0L
      } else {
        var max = 0L
        val it = deque.iterator()
        while (it.hasNext) {
          val sample = it.next()
          val ageMs = nowMs - sample.timestampMs
          if (ageMs >= 0L && ageMs <= windowMs && sample.heapUsed > max) {
            max = sample.heapUsed
          }
        }
        max
      }
    }
  }

  private def recentHighWatermark(
      executorIds: Set[String],
      nowMs: Long,
      windowMs: Long): Option[PressureSample] = {
    if (windowMs <= 0L) {
      None
    } else {
      executorIds.toSeq.flatMap { executorId =>
        Option(pressureSamples.get(executorId)).toSeq.flatMap { samples =>
          samples.iterator().asScala.filter { sample =>
            val ageMs = nowMs - sample.timestampMs
            ageMs >= 0L && ageMs <= windowMs
          }
        }
      }.sortBy(sample => (sample.pressure, sample.timestampMs)).lastOption
    }
  }

  private def trimSamples(
      samples: ConcurrentLinkedDeque[PressureSample],
      nowMs: Long): Unit = {
    var head = samples.peekFirst()
    while (head != null &&
        (nowMs - head.timestampMs > maxSampleRetentionMs ||
          samples.size() > maxSamplesPerExecutor)) {
      samples.pollFirst()
      head = samples.peekFirst()
    }
  }

  private def clamp(value: Double): Double = {
    math.max(0.0d, math.min(1.0d, value))
  }
}

private final case class PressureSample(
    pressure: Double,
    timestampMs: Long,
    executorId: String,
    heapUsed: Long)

final case class PressureView(
    pressure: Double,
    selected: Option[JvmPressureSnapshot],
    selectedAgeMs: Option[Long],
    stale: Boolean,
    staleThresholdMs: Long,
    highWatermarkPressure: Double,
    highWatermarkAgeMs: Option[Long],
    highWatermarkWindowMs: Long,
    executorSnapshots: Int,
    totalSnapshots: Int,
    updates: Long) {

  def compactString: String = {
    val executorId = selected.map(_.executorId).getOrElse("none")
    val age = selectedAgeMs.map(_.toString).getOrElse("n/a")
    val highAge = highWatermarkAgeMs.map(_.toString).getOrElse("n/a")
    val selectedSummary = selected.map(_.compactString).getOrElse("no-snapshot")
    f"pressure=$pressure%.4f selected=$executorId ageMs=$age stale=$stale " +
      s"staleThresholdMs=$staleThresholdMs " +
      f"highWatermark=$highWatermarkPressure%.4f highWatermarkAgeMs=$highAge " +
      s"highWatermarkWindowMs=$highWatermarkWindowMs executorSnapshots=$executorSnapshots " +
      s"totalSnapshots=$totalSnapshots updates=$updates selectedSnapshot=[$selectedSummary]"
  }
}
