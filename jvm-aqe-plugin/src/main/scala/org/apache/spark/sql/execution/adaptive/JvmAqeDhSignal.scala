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

import org.apache.spark.jvmaqe.JvmPressureStore
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

private[adaptive] final case class JvmAqeDhSignal(
    bytesMax: Long,
    predictedCostMax: Double,
    predictedCostP95: Double,
    dMax: Double,
    dDisp: Double,
    d: Double,
    h: Double,
    humongousFlag: Double,
    lowReclaimFlag: Double,
    heapRatio: Double,
    snapshotsInWindow: Int,
    dThreshold: Double,
    hThreshold: Double) {

  def highD: Boolean = d > dThreshold
  def highH: Boolean = h > hThreshold

  def quadrant: String = {
    if (highD && highH) {
      "Q1"
    } else if (highD && !highH) {
      "Q2"
    } else if (!highD && highH) {
      "Q3"
    } else {
      "Q4"
    }
  }

  def controlPressure(fallbackPressure: Double): Double = {
    val hPressure = math.max(fallbackPressure, h)
    if (!highD) {
      hPressure
    } else {
      val dSeverity = clamp((d - dThreshold) / math.max(dThreshold, 0.0001d))
      val dPressure = hThreshold + dSeverity * (1.0d - hThreshold)
      math.max(hPressure, dPressure)
    }
  }

  def compactString: String = {
    f"d=$d%.4f dMax=$dMax%.4f dDisp=$dDisp%.4f " +
      f"h=$h%.4f quadrant=$quadrant " +
      f"humongousFlag=$humongousFlag%.4f lowReclaimFlag=$lowReclaimFlag%.4f " +
      f"heapRatio=$heapRatio%.4f snapshotsInWindow=$snapshotsInWindow " +
      f"bytesMax=$bytesMax predictedCostMax=$predictedCostMax%.1f " +
      f"predictedCostP95=$predictedCostP95%.1f " +
      f"dThreshold=$dThreshold%.4f hThreshold=$hThreshold%.4f"
  }

  private def clamp(value: Double): Double = math.max(0.0d, math.min(1.0d, value))
}

private[adaptive] object JvmAqeDhSignal {

  def forBytes(
      session: SparkSession,
      bytes: Seq[Long],
      op: String): JvmAqeDhSignal = {
    val kInflation = math.max(0.01d,
      session.conf.get(JvmAqeSqlConf.EntropyKInflation, "4.0").toDouble)
    val mMultiplier = opMultiplier(session, op)
    val bSafe = bSafeBytes(session)
    val predictedCosts = bytes.map { b =>
      math.max(0.0d, b.toDouble) * kInflation * mMultiplier
    }
    val costMax = if (predictedCosts.isEmpty) 0.0d else predictedCosts.max
    val costMean =
      if (predictedCosts.isEmpty) 0.0d else predictedCosts.sum / predictedCosts.length.toDouble
    val costVariance =
      if (predictedCosts.length < 2 || costMean <= 0.0d) {
        0.0d
      } else {
        predictedCosts.map(c => (c - costMean) * (c - costMean)).sum /
          predictedCosts.length.toDouble
      }
    val sortedCosts = predictedCosts.sorted
    val p95Idx = if (sortedCosts.isEmpty) {
      0
    } else {
      (sortedCosts.length * 0.95).toInt.min(sortedCosts.length - 1).max(0)
    }
    val costP95 = if (sortedCosts.isEmpty) 0.0d else sortedCosts(p95Idx)
    val dMax = if (bSafe <= 0L) Double.MaxValue else costMax / bSafe.toDouble
    val dDisp = if (costMean <= 0.0d) 0.0d else math.sqrt(costVariance) / costMean
    val d = dMax * math.max(1.0d, dDisp)

    val hWindowMs = math.max(1L,
      session.conf.get(JvmAqeSqlConf.EntropyHWindowMs, "30000").toLong)
    val lowReclaimThreshold = math.max(0L,
      session.conf.get(JvmAqeSqlConf.EntropyHLowReclaimThresholdBytes, "67108864").toLong)
    val (w1, w2, w3) = hWeights(session)
    val now = System.currentTimeMillis()
    val snapshots = JvmPressureStore.allSnapshots.filter { snapshot =>
      snapshot.executorId != "driver" && now - snapshot.timestampMs <= hWindowMs
    }
    val humongous = if (snapshots.exists(_.lastGcCause.toLowerCase(
        java.util.Locale.ROOT).contains("g1 humongous allocation"))) 1.0d else 0.0d
    val lowReclaim = if (snapshots.exists { snapshot =>
        snapshot.lastGcReclaimedBytes >= 0L &&
          snapshot.lastGcReclaimedBytes < lowReclaimThreshold
      }) 1.0d else 0.0d
    val heapRatio = if (snapshots.nonEmpty) {
      snapshots.map(_.heapUsedRatio).sum / snapshots.size.toDouble
    } else {
      val all = JvmPressureStore.allSnapshots.filter { snapshot =>
        snapshot.executorId != "driver" && snapshot.heapMax > 0L
      }
      if (all.isEmpty) 0.0d else all.map(_.heapUsedRatio).sum / all.size.toDouble
    }
    val h = clamp(w1 * humongous + w2 * lowReclaim + w3 * heapRatio)

    JvmAqeDhSignal(
      bytesMax = if (bytes.isEmpty) 0L else bytes.max,
      predictedCostMax = costMax,
      predictedCostP95 = costP95,
      dMax = dMax,
      dDisp = dDisp,
      d = d,
      h = h,
      humongousFlag = humongous,
      lowReclaimFlag = lowReclaim,
      heapRatio = heapRatio,
      snapshotsInWindow = snapshots.size,
      dThreshold = session.conf.get(JvmAqeSqlConf.EntropyDThreshold, "1.0").toDouble,
      hThreshold = session.conf.get(JvmAqeSqlConf.EntropyHThreshold, "0.5").toDouble)
  }

  def bSafeBytes(session: SparkSession): Long = {
    val execMemBytes = try {
      session.conf.getOption("spark.executor.memory")
        .map(Utils.byteStringAsBytes)
        .getOrElse(1L * 1024L * 1024L * 1024L)
    } catch {
      case _: Exception => 1L * 1024L * 1024L * 1024L
    }
    val memFraction = session.conf.get("spark.memory.fraction", "0.6").toDouble
    val storageFraction = session.conf.get("spark.memory.storageFraction", "0.5").toDouble
    val execCores = math.max(1, session.conf.get("spark.executor.cores", "1").toInt)
    math.max(1L,
      (execMemBytes.toDouble * memFraction * (1.0d - storageFraction) / execCores).toLong)
  }

  private def opMultiplier(session: SparkSession, op: String): Double = {
    op.toLowerCase(java.util.Locale.ROOT) match {
      case "sort" =>
        math.max(0.01d, session.conf.get(
          JvmAqeSqlConf.EntropyOpMultiplierSort, "2.0").toDouble)
      case "sortmergejoin" =>
        math.max(0.01d, session.conf.get(
          JvmAqeSqlConf.EntropyOpMultiplierSmj, "1.5").toDouble)
      case "hashaggregate" =>
        math.max(0.01d, session.conf.get(
          JvmAqeSqlConf.EntropyOpMultiplierHashAgg, "2.5").toDouble)
      case "broadcasthashjoin" =>
        math.max(0.01d, session.conf.get(
          JvmAqeSqlConf.EntropyOpMultiplierBhj, "3.0").toDouble)
      case _ =>
        math.max(0.01d, session.conf.get(
          JvmAqeSqlConf.EntropyOpMultiplierDefault, "1.0").toDouble)
    }
  }

  private def hWeights(session: SparkSession): (Double, Double, Double) = {
    val raw = session.conf.get(JvmAqeSqlConf.EntropyHWeights, "0.4,0.3,0.3")
    val parts = raw.split(",").map(_.trim)
    if (parts.length != 3) {
      (0.4d, 0.3d, 0.3d)
    } else {
      try {
        (parts(0).toDouble, parts(1).toDouble, parts(2).toDouble)
      } catch {
        case _: NumberFormatException => (0.4d, 0.3d, 0.3d)
      }
    }
  }

  private def clamp(value: Double): Double = math.max(0.0d, math.min(1.0d, value))
}
