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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.jvmaqe.{JvmPressureStore, PressureView}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{
  CoalescedPartitionSpec,
  ShufflePartitionSpec,
  SparkPlan,
  UnaryExecNode,
  UnionExec
}
import org.apache.spark.sql.execution.exchange.{
  ENSURE_REQUIREMENTS,
  REBALANCE_PARTITIONS_BY_COL,
  REBALANCE_PARTITIONS_BY_NONE,
  REPARTITION_BY_COL,
  ShuffleExchangeLike,
  ShuffleOrigin
}
import org.apache.spark.sql.execution.joins.{
  BroadcastHashJoinExec,
  BroadcastNestedLoopJoinExec,
  CartesianProductExec
}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Recomputes or restores coalesced AQE shuffle reads when JVM pressure is observed.
 * Dynamic rewrites are only allowed to increase the existing partition count so that the rule does
 * not increase per-task memory pressure while the JVM is already pressured.
 *
 * The rule is intentionally conservative: it only rewrites an existing coalesced
 * [[AQEShuffleReadExec]] backed by a materialized [[ShuffleQueryStageExec]]. This avoids changing
 * local or skew shuffle-read layouts created by other AQE rules.
 */
final class JvmPressureAwareCoalesceRule(session: SparkSession)
  extends AQEShuffleReadRule with Logging {

  override protected val supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REPARTITION_BY_COL, REBALANCE_PARTITIONS_BY_NONE,
      REBALANCE_PARTITIONS_BY_COL)

  private def sqlConf: SQLConf = session.sessionState.conf

  private def enabled: Boolean =
    session.conf.get(JvmAqeSqlConf.Enabled, "true").toBoolean &&
      session.conf.get(JvmAqeSqlConf.CoalesceEnabled, "true").toBoolean

  private def modifyPlans: Boolean =
    session.conf.get(JvmAqeSqlConf.CoalesceModifyPlansEnabled,
      session.conf.get(JvmAqeSqlConf.ModifyPlansEnabled, "false")).toBoolean

  private def pressureThreshold: Double =
    session.conf.get(JvmAqeSqlConf.PressureThreshold, "0.70").toDouble

  private def minTargetFactor: Double =
    clamp(session.conf.get(JvmAqeSqlConf.CoalesceMinTargetFactor, "0.25").toDouble, 0.01, 1.0)

  private def sensitivity: Double =
    math.max(0.0d, session.conf.get(JvmAqeSqlConf.CoalesceSensitivity, "1.0").toDouble)

  private def minShuffleBytes: Long =
    math.max(0L, Utils.byteStringAsBytes(
      session.conf.get(JvmAqeSqlConf.CoalesceMinShuffleBytes, "512m")))

  private def tinyStageBytes: Long =
    math.max(0L, Utils.byteStringAsBytes(
      session.conf.get(JvmAqeSqlConf.CoalesceTinyStageBytes, "256m")))

  private def tinyStageMaxPartitions: Int =
    math.max(1, session.conf.get(JvmAqeSqlConf.CoalesceTinyStageMaxPartitions, "4").toInt)

  private def maxExpansionFactor: Double =
    math.max(1.0d, session.conf.get(JvmAqeSqlConf.CoalesceMaxExpansionFactor, "4.0").toDouble)

  private def minOldAvgFactor: Double =
    math.max(0.0d, session.conf.get(JvmAqeSqlConf.CoalesceMinOldAvgFactor, "0").toDouble)

  private def minOldMaxFactor: Double =
    math.max(0.0d, session.conf.get(JvmAqeSqlConf.CoalesceMinOldMaxFactor, "0").toDouble)

  private def maxHighWatermarkAgeMsForRewrite: Long =
    session.conf.get(JvmAqeSqlConf.CoalesceMaxHighWatermarkAgeMsForRewrite, "-1").toLong

  private def benefitGuardBypassOnHumongousGc: Boolean =
    session.conf.get(JvmAqeSqlConf.CoalesceBenefitGuardBypassOnHumongousGc, "false").toBoolean

  private def benefitGuardBypassLowReclaimBytes: Long =
    math.max(0L, Utils.byteStringAsBytes(session.conf.get(
      JvmAqeSqlConf.CoalesceBenefitGuardBypassLowReclaimBytes, "0")))

  private def coalesceAction: String =
    session.conf.get(JvmAqeSqlConf.CoalesceAction, JvmAqeSqlConf.CoalesceActionDynamic)
      .trim.toLowerCase

  private def decisionLogEnabled: Boolean =
    session.conf.get(JvmAqeSqlConf.DecisionLogEnabled, "true").toBoolean

  private def snapshotStaleThresholdMs: Long =
    math.max(0L, session.conf.get(JvmAqeSqlConf.SnapshotStaleThresholdMs, "10000").toLong)

  private def pressureHighWatermarkWindowMs: Long =
    math.max(0L, session.conf.get(
      JvmAqeSqlConf.PressureHighWatermarkWindowMs, "60000").toLong)

  override protected def isSupported(shuffle: ShuffleExchangeLike): Boolean = {
    shuffle.outputPartitioning != SinglePartition && super.isSupported(shuffle)
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    // Observe-only entropy logging: independent of modifyPlans; always runs when enabled.
    if (entropyObserveEnabled) {
      collectEntropyTargets(plan, parentOp = None).foreach { case (read, stage, parentName) =>
        logEntropyObservation(read, stage, parentName)
      }
    }

    if (!enabled || !sqlConf.coalesceShufflePartitionsEnabled) {
      return plan
    }

    coalesceAction match {
      case JvmAqeSqlConf.CoalesceActionDynamic =>
        rewriteDynamicCoalesceGroups(plan)
      case _ =>
        plan.transformUp {
          case read @ AQEShuffleReadExec(stage: ShuffleQueryStageExec, specs)
              if shouldConsider(stage, specs) =>
            rewriteRead(read, stage, specs)
        }
    }
  }

  // -------------------------------------------------------------------------
  // Entropy observe mode
  // -------------------------------------------------------------------------

  private def entropyObserveEnabled: Boolean =
    session.conf.get(JvmAqeSqlConf.EntropyObserveEnabled, "false").toBoolean

  private def entropyKInflation: Double =
    math.max(0.01d,
      session.conf.get(JvmAqeSqlConf.EntropyKInflation, "4.0").toDouble)

  private def entropyOpMultiplier(op: String): Double = op.toLowerCase(java.util.Locale.ROOT) match {
    case "sort" =>
      math.max(0.01d,
        session.conf.get(JvmAqeSqlConf.EntropyOpMultiplierSort, "2.0").toDouble)
    case "sortmergejoin" =>
      math.max(0.01d,
        session.conf.get(JvmAqeSqlConf.EntropyOpMultiplierSmj, "1.5").toDouble)
    case "hashaggregate" =>
      math.max(0.01d,
        session.conf.get(JvmAqeSqlConf.EntropyOpMultiplierHashAgg, "2.5").toDouble)
    case "broadcasthashjoin" =>
      math.max(0.01d,
        session.conf.get(JvmAqeSqlConf.EntropyOpMultiplierBhj, "3.0").toDouble)
    case _ =>
      math.max(0.01d,
        session.conf.get(JvmAqeSqlConf.EntropyOpMultiplierDefault, "1.0").toDouble)
  }

  private def entropyHThreshold: Double =
    session.conf.get(JvmAqeSqlConf.EntropyHThreshold, "0.5").toDouble

  private def entropyDThreshold: Double =
    session.conf.get(JvmAqeSqlConf.EntropyDThreshold, "1.0").toDouble

  private def entropyHWindowMs: Long =
    math.max(1L,
      session.conf.get(JvmAqeSqlConf.EntropyHWindowMs, "30000").toLong)

  private def entropyHLowReclaimThresholdBytes: Long =
    math.max(0L, session.conf.get(
      JvmAqeSqlConf.EntropyHLowReclaimThresholdBytes, "67108864").toLong)

  /** Parse "w1,w2,w3" weights string; returns (wHumongous, wLowReclaim, wHeap). */
  private def entropyHWeights: (Double, Double, Double) = {
    val raw = session.conf.get(JvmAqeSqlConf.EntropyHWeights, "0.4,0.3,0.3")
    val parts = raw.split(",").map(_.trim)
    if (parts.length == 3) {
      try {
        (parts(0).toDouble, parts(1).toDouble, parts(2).toDouble)
      } catch {
        case _: NumberFormatException => (0.4d, 0.3d, 0.3d)
      }
    } else {
      (0.4d, 0.3d, 0.3d)
    }
  }

  /**
   * Compute B_safe (bytes) from executor memory config.
   * B_safe = (executorMemory × spark.memory.fraction × (1 - spark.memory.storageFraction))
   *           / executorCores
   */
  private def bSafeBytes: Long = {
    val execMemBytes: Long = try {
      session.conf.getOption("spark.executor.memory")
        .map(org.apache.spark.util.Utils.byteStringAsBytes)
        .getOrElse(1L * 1024L * 1024L * 1024L) // default 1 GB
    } catch {
      case _: Exception => 1L * 1024L * 1024L * 1024L
    }
    val memFraction = session.conf.get("spark.memory.fraction", "0.6").toDouble
    val storageFraction = session.conf.get("spark.memory.storageFraction", "0.5").toDouble
    val execCores = math.max(1,
      session.conf.get("spark.executor.cores", "1").toInt)
    math.max(1L,
      ((execMemBytes.toDouble * memFraction * (1.0d - storageFraction)) / execCores).toLong)
  }

  /**
   * Walk the plan tree tracking parent context and collect
   * (AQEShuffleReadExec, ShuffleQueryStageExec, parentClassName) tuples for entropy logging.
   * The parent class name is the direct parent of the AQEShuffleReadExec node (the "downstream"
   * consumer). Returns "Unknown" when no parent can be identified (e.g. root node).
   */
  private def collectEntropyTargets(
      plan: SparkPlan,
      parentOp: Option[String]): Seq[(AQEShuffleReadExec, ShuffleQueryStageExec, String)] = {
    plan match {
      case read @ AQEShuffleReadExec(stage: ShuffleQueryStageExec, _)
          if stage.isMaterialized && stage.mapStats.isDefined =>
        Seq((read, stage, parentOp.getOrElse("Unknown"))) ++
          plan.children.flatMap { child =>
            val childParent = plan.getClass.getSimpleName
              .replace("Exec", "").replace("$", "")
            collectEntropyTargets(child, Some(childParent))
          }
      case other =>
        val myName = other.getClass.getSimpleName.replace("Exec", "").replace("$", "")
        other.children.flatMap(child => collectEntropyTargets(child, Some(myName)))
    }
  }

  /**
   * Map a plan class simple name to a canonical operator key used for multiplier lookup.
   */
  private def canonicalOp(rawName: String): String = rawName.toLowerCase(java.util.Locale.ROOT) match {
    case n if n.contains("sort") && n.contains("merge") => "sortmergejoin"
    case n if n.contains("sort") && !n.contains("merge") => "sort"
    case n if n.contains("hashagg") || n.contains("hashaggregate") => "hashaggregate"
    case n if n.contains("broadcasthashjoin") || (n.contains("broadcast") && n.contains("hash")) =>
      "broadcasthashjoin"
    case _ => "unknown"
  }

  /**
   * Humongous-allocation flag: returns 1.0 if ANY executor snapshot in the recent window
   * has lastGcCause containing "g1 humongous allocation" (case-insensitive substring match),
   * else 0.0. Flag-based avoids the sparsity problem of rate computation.
   */
  private def humongousFlag(windowMs: Long): Double = {
    val now = System.currentTimeMillis()
    val snapshots = JvmPressureStore.allSnapshots.filter { s =>
      s.executorId != "driver" && (now - s.timestampMs) <= windowMs
    }
    if (snapshots.isEmpty) return 0.0d
    val found = snapshots.exists(
      _.lastGcCause.toLowerCase(java.util.Locale.ROOT).contains("g1 humongous allocation"))
    if (found) 1.0d else 0.0d
  }

  /**
   * Low-reclaim flag: returns 1.0 if ANY executor snapshot in the recent window has
   * lastGcReclaimedBytes < lowReclaimThresholdBytes, else 0.0.
   * Threshold configured via spark.jvmAqe.sql.entropy.h.lowReclaimThresholdBytes (default 64 MiB).
   */
  private def lowReclaimFlag(windowMs: Long, thresholdBytes: Long): Double = {
    val now = System.currentTimeMillis()
    val snapshots = JvmPressureStore.allSnapshots.filter { s =>
      s.executorId != "driver" && (now - s.timestampMs) <= windowMs
    }
    if (snapshots.isEmpty) return 0.0d
    val found = snapshots.exists { s =>
      s.lastGcReclaimedBytes >= 0L && s.lastGcReclaimedBytes < thresholdBytes
    }
    if (found) 1.0d else 0.0d
  }

  /** Mean heap-used ratio across executor snapshots. */
  private def heapUsedRatioMean(windowMs: Long): Double = {
    val now = System.currentTimeMillis()
    val snapshots = JvmPressureStore.allSnapshots.filter { s =>
      s.executorId != "driver" && s.heapMax > 0L && (now - s.timestampMs) <= windowMs
    }
    if (snapshots.isEmpty) {
      // Fall back to all executor snapshots if none in window
      val all = JvmPressureStore.allSnapshots.filter(s => s.executorId != "driver" && s.heapMax > 0L)
      if (all.isEmpty) return 0.0d
      all.map(_.heapUsedRatio).sum / all.size.toDouble
    } else {
      snapshots.map(_.heapUsedRatio).sum / snapshots.size.toDouble
    }
  }

  private def clampD(v: Double): Double = math.max(0.0d, math.min(1.0d, v))

  /**
   * Core observe-only logging method. Computes D and H per AQEShuffleReadExec,
   * classifies the quadrant, and emits a single structured log line.
   * DOES NOT MODIFY THE PLAN.
   */
  private def logEntropyObservation(
      read: AQEShuffleReadExec,
      stage: ShuffleQueryStageExec,
      parentOpName: String): Unit = {
    // --- plan ID context ---
    val appId = try {
      session.sparkContext.applicationId
    } catch {
      case _: Exception => "unknown"
    }
    val stageId = stage.id
    val qsid = stage.id  // query stage id == stage id in AQE

    val mapStats = stage.mapStats.get
    val partitionBytes: Array[Long] = mapStats.bytesByPartitionId
    val numParts = partitionBytes.length

    // --- downstream operator detection ---
    // parentOpName is the class simple name of the direct parent of this AQEShuffleReadExec
    // in the plan tree, as resolved by collectEntropyTargets above.
    val rawOp: String = parentOpName
    val canonOp = canonicalOp(rawOp)
    val opLabel = rawOp
    val kInflation = entropyKInflation
    val mMult = entropyOpMultiplier(canonOp)
    val bSafe = bSafeBytes

    // --- predicted task costs ---
    val predictedCosts: Array[Double] = partitionBytes.map { b =>
      b.toDouble * kInflation * mMult
    }
    val costMax = if (predictedCosts.isEmpty) 0.0d else predictedCosts.max
    val costMean = if (predictedCosts.isEmpty) 0.0d
      else predictedCosts.sum / predictedCosts.length.toDouble
    val costVariance = if (predictedCosts.length < 2) 0.0d
      else predictedCosts.map(c => (c - costMean) * (c - costMean)).sum / predictedCosts.length.toDouble
    val costStddev = math.sqrt(costVariance)

    // percentiles
    val sortedCosts = predictedCosts.sorted
    val p50Idx = math.max(0, (sortedCosts.length * 0.50 - 1).toInt.max(0))
    val p95Idx = math.max(0, math.min(sortedCosts.length - 1,
      (sortedCosts.length * 0.95).toInt))
    val costP50 = if (sortedCosts.nonEmpty) sortedCosts(p50Idx) else 0.0d
    val costP95 = if (sortedCosts.nonEmpty) sortedCosts(p95Idx) else 0.0d

    // bytes percentiles (from raw partition bytes)
    val sortedBytes = partitionBytes.sorted
    val bytesP50 = if (sortedBytes.nonEmpty) sortedBytes((sortedBytes.length * 0.50).toInt.min(sortedBytes.length - 1).max(0)) else 0L
    val bytesP95 = if (sortedBytes.nonEmpty) sortedBytes((sortedBytes.length * 0.95).toInt.min(sortedBytes.length - 1).max(0)) else 0L
    val bytesMax = if (sortedBytes.nonEmpty) sortedBytes.last else 0L

    // --- D computation ---
    val dMax = if (bSafe <= 0L) Double.MaxValue else costMax / bSafe.toDouble
    val dDisp = if (costMean <= 0.0d) 0.0d else costStddev / costMean
    val d = dMax * math.max(1.0d, dDisp)

    // --- H computation ---
    val windowMs = entropyHWindowMs
    val lowReclaimThreshBytes = entropyHLowReclaimThresholdBytes
    val (w1, w2, w3) = entropyHWeights
    val dThresh = entropyDThreshold
    val hThresh = entropyHThreshold

    // Collect window snapshots for diagnostics and H flags
    val now = System.currentTimeMillis()
    val allStored = JvmPressureStore.allSnapshots
    val storeSize = allStored.size
    val executorSnapshots = allStored.filter(_.executorId != "driver")
    val executorSnapshotsTotal = executorSnapshots.size
    val newestSnapshotAgeMs: Long = if (executorSnapshots.isEmpty) -1L
      else executorSnapshots.map(s => now - s.timestampMs).min
    val oldestSnapshotAgeMsAll: Long = if (executorSnapshots.isEmpty) -1L
      else executorSnapshots.map(s => now - s.timestampMs).max
    val windowSnapshots = allStored.filter { s =>
      s.executorId != "driver" && (now - s.timestampMs) <= windowMs
    }
    val snapshotsInWindow: Int = windowSnapshots.size
    val oldestSnapshotAgeMs: Long = if (windowSnapshots.isEmpty) -1L
      else windowSnapshots.map(s => now - s.timestampMs).max

    val hStr: String = if (snapshotsInWindow == 0) {
      "NA"
    } else {
      val hFlag = humongousFlag(windowMs)
      val lFlag = lowReclaimFlag(windowMs, lowReclaimThreshBytes)
      val heapRatio = heapUsedRatioMean(windowMs)
      val hRaw = w1 * hFlag + w2 * lFlag + w3 * heapRatio
      f"${clampD(hRaw)}%.4f"
    }

    val (humFlagStr, lowFlagStr, heapRatioStr) = if (snapshotsInWindow == 0) {
      ("NA", "NA", "NA")
    } else {
      val hFlag = humongousFlag(windowMs)
      val lFlag = lowReclaimFlag(windowMs, lowReclaimThreshBytes)
      val heapRatio = heapUsedRatioMean(windowMs)
      (f"$hFlag%.4f", f"$lFlag%.4f", f"$heapRatio%.4f")
    }

    // --- Quadrant classification ---
    val quadrant: String = if (hStr == "NA") {
      "QUnknown"
    } else {
      val hVal = hStr.toDouble
      if (d > dThresh && hVal > hThresh) "Q1"
      else if (d > dThresh && hVal <= hThresh) "Q2"
      else if (d <= dThresh && hVal > hThresh) "Q3"
      else "Q4"
    }

    val MiB = 1024.0d * 1024.0d

    // GUARD: this method must never modify the plan. We only emit log.
    logInfo(
      f"JVM-AQE-OBSERVE app=$appId stage=$stageId queryStage=$qsid " +
        f"numParts=$numParts " +
        f"bytesP50=${bytesP50 / MiB}%.3fMB " +
        f"bytesP95=${bytesP95 / MiB}%.3fMB " +
        f"bytesMax=${bytesMax / MiB}%.3fMB " +
        f"downstreamOp=$opLabel " +
        f"kInflation=$kInflation%.2f " +
        f"mMultiplier=$mMult%.2f " +
        f"bSafeMb=${bSafe / MiB}%.3fMB " +
        f"predictedCostMaxMb=${costMax / MiB}%.3fMB " +
        f"predictedCostP95Mb=${costP95 / MiB}%.3fMB " +
        f"dMax=$dMax%.4f " +
        f"dDisp=$dDisp%.4f " +
        f"d=$d%.4f " +
        f"humongousFlag=$humFlagStr " +
        f"lowReclaimFlag=$lowFlagStr " +
        f"heapRatio=$heapRatioStr " +
        f"h=$hStr " +
        f"quadrant=$quadrant " +
        f"storeSize=$storeSize " +
        f"executorSnapshotsTotal=$executorSnapshotsTotal " +
        f"newestSnapshotAgeMs=$newestSnapshotAgeMs " +
        f"oldestSnapshotAgeMsAll=$oldestSnapshotAgeMsAll " +
        f"snapshotsInWindow=$snapshotsInWindow " +
        f"oldestSnapshotAgeMs=$oldestSnapshotAgeMs " +
        f"action=observe-only")
  }

  private def shouldConsider(
      stage: ShuffleQueryStageExec,
      currentSpecs: Seq[ShufflePartitionSpec]): Boolean = {
    stage.isMaterialized &&
      stage.mapStats.isDefined &&
      isSupported(stage.shuffle) &&
      currentSpecs.nonEmpty &&
      currentSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])
  }

  private def rewriteRead(
      read: AQEShuffleReadExec,
      stage: ShuffleQueryStageExec,
      oldSpecs: Seq[ShufflePartitionSpec]): SparkPlan = {
    val shuffleId = stage.mapStats.map(_.shuffleId).getOrElse(-1)
    val mapStats = stage.mapStats.get
    val mapStatsTotalBytes = mapStats.bytesByPartitionId.sum
    val reducerCount = mapStats.bytesByPartitionId.length
    val maxReducerBytes =
      if (mapStats.bytesByPartitionId.isEmpty) 0L else mapStats.bytesByPartitionId.max
    val pressureView = JvmPressureStore.currentPressureView(
      snapshotStaleThresholdMs, pressureHighWatermarkWindowMs)
    val pressure = pressureView.pressure
    val oldByteSummary = coalescedBytesSummary(Seq(oldSpecs), mapStatsTotalBytes, oldSpecs.length)
    val dhSignal = JvmAqeDhSignal.forBytes(session, Seq(oldByteSummary.maxBytes), "unknown")
    val shouldRewriteBySignal = pressure >= pressureThreshold || dhSignal.highD || dhSignal.highH
    if (!shouldRewriteBySignal) {
      if (decisionLogEnabled) {
        logInfo(
          s"[JVM-AQE] considered coalesced AQEShuffleReadExec; action=keep, " +
            s"reason=dhBelowThreshold, shuffleStage=${stage.id}, shuffle=$shuffleId, " +
            s"partitions=${oldSpecs.length}, mapStatsTotalBytes=$mapStatsTotalBytes, " +
            s"reducerCount=$reducerCount, maxReducerBytes=$maxReducerBytes, " +
            f"pressure=$pressure%.4f, pressureThreshold=$pressureThreshold%.4f, " +
            s"signal=${dhSignal.compactString}, pressureView=${pressureView.compactString}")
      }
      return read
    }

    coalesceAction match {
      case JvmAqeSqlConf.CoalesceActionRestore =>
        restoreRead(read, stage, oldSpecs, shuffleId, mapStatsTotalBytes, reducerCount,
          maxReducerBytes, pressure, pressureView.compactString)
      case JvmAqeSqlConf.CoalesceActionDynamic =>
        rewriteReadWithDynamicTarget(read, stage, oldSpecs, shuffleId, mapStatsTotalBytes,
          reducerCount, maxReducerBytes, pressure, pressureView, dhSignal, oldByteSummary)
      case other =>
        logWarning(
          s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
            s"shuffle=$shuffleId, action=keep, reason=unknownCoalesceAction, " +
            s"${JvmAqeSqlConf.CoalesceAction}=$other, " +
            s"supportedActions=${JvmAqeSqlConf.CoalesceActionDynamic}," +
            s"${JvmAqeSqlConf.CoalesceActionRestore}, oldPartitions=${oldSpecs.length}, " +
            s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
            s"maxReducerBytes=$maxReducerBytes, " +
            f"pressure=$pressure%.4f, pressureView=${pressureView.compactString}")
        read
    }
  }

  private def restoreRead(
      read: AQEShuffleReadExec,
      stage: ShuffleQueryStageExec,
      oldSpecs: Seq[ShufflePartitionSpec],
      shuffleId: Int,
      mapStatsTotalBytes: Long,
      reducerCount: Int,
      maxReducerBytes: Long,
      pressure: Double,
      pressureViewSummary: String): SparkPlan = {
    val restoredSpecs = identitySpecs(stage)
    val expansionFactor = maxExpansionFactor
    val maxAllowedPartitions = cappedPartitionCount(oldSpecs.length, reducerCount, expansionFactor)
    if (restoredSpecs == oldSpecs) {
      logInfo(
        s"[JVM-AQE] restore unchanged for shuffleStage=${stage.id}, shuffle=$shuffleId, " +
          s"action=restore, reason=alreadyIdentity, partitions=${oldSpecs.length}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, " +
          f"pressure=$pressure%.4f, pressureView=$pressureViewSummary")
      read
    } else if (restoredSpecs.length > maxAllowedPartitions) {
      logWarning(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=restoreExpansionCapGuard, " +
          s"oldPartitions=${oldSpecs.length}, restoredPartitions=${restoredSpecs.length}, " +
          s"maxAllowedPartitions=$maxAllowedPartitions, maxExpansionFactor=$expansionFactor, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, " +
          f"pressure=$pressure%.4f, pressureView=$pressureViewSummary")
      read
    } else if (modifyPlans) {
      logWarning(
        s"[JVM-AQE] restoring coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=restore, oldPartitions=${oldSpecs.length}, " +
          s"newPartitions=${restoredSpecs.length}, maxAllowedPartitions=$maxAllowedPartitions, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, " +
          s"reducerCount=$reducerCount, maxReducerBytes=$maxReducerBytes, " +
          f"pressure=$pressure%.4f, pressureView=$pressureViewSummary")
      AQEShuffleReadExec(stage, restoredSpecs)
    } else {
      logWarning(
        s"[JVM-AQE] would restore coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=restore, oldPartitions=${oldSpecs.length}, " +
          s"newPartitions=${restoredSpecs.length}, " +
          s"${JvmAqeSqlConf.CoalesceModifyPlansEnabled}=false, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, " +
          f"pressure=$pressure%.4f, pressureView=$pressureViewSummary")
      read
    }
  }

  private def rewriteDynamicCoalesceGroups(plan: SparkPlan): SparkPlan = {
    val groups = collectCoalesceGroups(plan)
    if (groups.isEmpty) {
      return plan
    }
    val groupKeySets = groups.map(group => group.map(readKey).toSet)
    val sharedReadKeys = groupKeySets.flatten.groupBy(identity).collect {
      case (key, occurrences) if occurrences.size > 1 => key
    }.toSet

    val pressureView = JvmPressureStore.currentPressureView(
      snapshotStaleThresholdMs, pressureHighWatermarkWindowMs)
    val pressure = pressureView.pressure

    val specsMap = mutable.HashMap.empty[JvmCoalesceReadKey, Seq[ShufflePartitionSpec]]
    groups.foreach { group =>
      rewriteDynamicCoalesceGroup(group, sharedReadKeys, pressure, pressureView).foreach {
        candidates =>
          addGroupCandidates(specsMap, candidates, group)
      }
    }

    if (specsMap.nonEmpty) {
      updateShuffleReads(plan, specsMap.toMap)
    } else {
      plan
    }
  }

  private def rewriteDynamicCoalesceGroup(
      group: Seq[JvmCoalesceStageInfo],
      sharedReadKeys: Set[JvmCoalesceReadKey],
      pressure: Double,
      pressureView: PressureView): Option[Seq[(JvmCoalesceReadKey, Seq[ShufflePartitionSpec])]] = {
    val pressureViewSummary = pressureView.compactString
    val sharedKeysInGroup = group.map(readKey).filter(sharedReadKeys.contains).distinct
    if (sharedKeysInGroup.nonEmpty) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=readKeySharedAcrossGroups, sharedReadKeys=$sharedKeysInGroup, " +
          s"${groupSummary(group)}, " +
          f"pressure=$pressure%.4f, pressureView=$pressureViewSummary")
      return None
    }

    val invalidReason = invalidGroupReason(group)
    if (invalidReason.nonEmpty) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=${invalidReason.get}, ${groupSummary(group)}, " +
          f"pressure=$pressure%.4f, pressureView=$pressureViewSummary")
      return None
    }

    val mapStats = group.map(_.stage.mapStats.get)
    val groupTotalBytes = mapStats.map(_.bytesByPartitionId.sum).sum
    val reducerCount = mapStats.head.bytesByPartitionId.length
    val maxReducerBytes = mapStats.flatMap(_.bytesByPartitionId).foldLeft(0L)(math.max)
    val baseTarget = advisoryPartitionSize(group)
    val oldByteSummary = coalescedBytesSummary(
      group.map(_.specs), groupTotalBytes, group.map(_.oldPartitionCount).max)
    val dhSignal = JvmAqeDhSignal.forBytes(session, Seq(oldByteSummary.maxBytes), "unknown")
    val shouldRewriteBySignal = pressure >= pressureThreshold || dhSignal.highD || dhSignal.highH
    if (!shouldRewriteBySignal) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, reason=dhBelowThreshold, " +
          s"${groupSummary(group)}, groupTotalBytes=$groupTotalBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, pressureThreshold=$pressureThreshold%.4f, " +
          s"signal=${dhSignal.compactString}, maxReducerBytes=$maxReducerBytes, " +
          s"pressureView=$pressureViewSummary")
      return None
    }
    val controlPressure = dhSignal.controlPressure(pressure)
    val pressureFactor = dynamicTargetFactor(controlPressure)
    val pressureTarget = targetSize(baseTarget, pressureFactor)
    val minBytes = minShuffleBytes
    val tinyBytes = tinyStageBytes
    val tinyMaxPartitions = tinyStageMaxPartitions
    val expansionFactor = maxExpansionFactor

    if (groupTotalBytes < minBytes) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, reason=minShuffleBytesGuard, " +
          s"${groupSummary(group)}, groupTotalBytes=$groupTotalBytes, " +
          s"minShuffleBytes=$minBytes, reducerCount=$reducerCount, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    if (group.map(_.oldPartitionCount).max <= tinyMaxPartitions &&
        groupTotalBytes <= tinyBytes) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, reason=tinyLateStageGuard, " +
          s"${groupSummary(group)}, groupTotalBytes=$groupTotalBytes, " +
          s"tinyStageBytes=$tinyBytes, tinyStageMaxPartitions=$tinyMaxPartitions, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    if (group.forall(_.oldPartitionCount >= reducerCount)) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, reason=noReducerHeadroom, " +
          s"${groupSummary(group)}, groupTotalBytes=$groupTotalBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    val maxAllowedByReadKey = group.map { info =>
      readKey(info) -> cappedPartitionCount(info.oldPartitionCount, reducerCount, expansionFactor)
    }.toMap
    val groupMaxAllowedPartitions = maxAllowedByReadKey.values.min
    val capTarget = targetForMaxPartitions(groupTotalBytes, groupMaxAllowedPartitions)
    val effectiveTarget = math.max(pressureTarget, capTarget)
    val minPartitionSize = minPartitionSizeFor(effectiveTarget)
    val newSpecsByStage = ShufflePartitionsUtil.coalescePartitions(
      group.map(_.stage.mapStats),
      group.map(_ => None),
      advisoryTargetSize = effectiveTarget,
      minNumPartitions = 1,
      minPartitionSize = minPartitionSize)

    if (newSpecsByStage.isEmpty) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=shufflePartitionsUtilNoChange, ${groupSummary(group)}, " +
          s"groupTotalBytes=$groupTotalBytes, reducerCount=$reducerCount, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    val desiredStageDecisions = group.zip(newSpecsByStage).map { case (info, specs) =>
      JvmCoalesceStageDecision(info, specs, maxAllowedByReadKey(readKey(info)))
    }
    val desiredExceedsCap =
      desiredStageDecisions.exists(d => d.newPartitionCount > d.maxAllowedPartitions)
    val maybeStageDecisions = if (desiredExceedsCap) {
      capCoalescedSpecsByStage(newSpecsByStage, groupMaxAllowedPartitions).map {
        cappedSpecsByStage =>
          group.zip(cappedSpecsByStage).map { case (info, specs) =>
            JvmCoalesceStageDecision(info, specs, maxAllowedByReadKey(readKey(info)))
          }
      }
    } else {
      Some(desiredStageDecisions)
    }

    if (maybeStageDecisions.isEmpty) {
      logWarning(
        s"[JVM-AQE] keeping coalesce group; action=keep, reason=capRewriteFailed, " +
          s"${groupSummary(group)}, desired=${decisionSummary(desiredStageDecisions)}, " +
          s"groupTotalBytes=$groupTotalBytes, reducerCount=$reducerCount, " +
          s"maxExpansionFactor=$expansionFactor, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    val stageDecisions = maybeStageDecisions.get
    val newByteSummary = coalescedBytesSummary(
      stageDecisions.map(_.newSpecs), groupTotalBytes, stageDecisions.map(_.newPartitionCount).max)
    if (stageDecisions.exists(d => d.newPartitionCount < d.info.oldPartitionCount)) {
      logWarning(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=wouldReducePartitionsUnderPressure, ${groupSummary(group)}, " +
          s"${decisionSummary(stageDecisions)}, desired=${decisionSummary(desiredStageDecisions)}, " +
          s"cappedByExpansion=$desiredExceedsCap, groupTotalBytes=$groupTotalBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    if (!stageDecisions.exists(d => d.newPartitionCount > d.info.oldPartitionCount)) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=noPartitionIncrease, ${groupSummary(group)}, " +
          s"${decisionSummary(stageDecisions)}, desired=${decisionSummary(desiredStageDecisions)}, " +
          s"cappedByExpansion=$desiredExceedsCap, groupTotalBytes=$groupTotalBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    if (stageDecisions.exists(d => d.newPartitionCount > d.maxAllowedPartitions)) {
      logWarning(
        s"[JVM-AQE] keeping coalesce group; action=keep, reason=expansionCapGuard, " +
          s"${groupSummary(group)}, ${decisionSummary(stageDecisions)}, " +
          s"desired=${decisionSummary(desiredStageDecisions)}, " +
          s"groupTotalBytes=$groupTotalBytes, reducerCount=$reducerCount, " +
          s"maxExpansionFactor=$expansionFactor, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    if (benefitGuardTriggered(oldByteSummary, baseTarget, pressureView)) {
      val avgThreshold = targetBytesForFactor(baseTarget, minOldAvgFactor)
      val maxThreshold = targetBytesForFactor(baseTarget, minOldMaxFactor)
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, reason=benefitGuard, " +
          s"${groupSummary(group)}, ${decisionSummary(stageDecisions)}, " +
          s"desired=${decisionSummary(desiredStageDecisions)}, " +
          s"cappedByExpansion=$desiredExceedsCap, groupTotalBytes=$groupTotalBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          s"minOldAvgFactor=$minOldAvgFactor, minOldMaxFactor=$minOldMaxFactor, " +
          s"minOldAvgBytes=$avgThreshold, minOldMaxBytes=$maxThreshold, " +
          s"benefitGuardBypass=${benefitGuardBypassSummary(pressureView)}, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    if (highWatermarkAgeGuardTriggered(pressureView)) {
      logInfo(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=highWatermarkAgeGuard, ${groupSummary(group)}, " +
          s"${decisionSummary(stageDecisions)}, desired=${decisionSummary(desiredStageDecisions)}, " +
          s"cappedByExpansion=$desiredExceedsCap, groupTotalBytes=$groupTotalBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          s"selectedPressure=${selectedPressure(pressureView)}, " +
          s"highWatermarkAgeMs=${pressureView.highWatermarkAgeMs.getOrElse(-1L)}, " +
          s"maxHighWatermarkAgeMsForRewrite=$maxHighWatermarkAgeMsForRewrite, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return None
    }

    val candidates = stageDecisions.map(d => readKey(d.info) -> d.newSpecs)
    if (hasConflictingStageCandidates(candidates)) {
      logWarning(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=conflictingDuplicateStageIds, ${groupSummary(group)}, " +
          s"${decisionSummary(stageDecisions)}, desired=${decisionSummary(desiredStageDecisions)}, " +
          s"cappedByExpansion=$desiredExceedsCap, groupTotalBytes=$groupTotalBytes, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, pressureView=$pressureViewSummary")
      return None
    }

    if (modifyPlans) {
      logWarning(
        s"[JVM-AQE] rewriting coalesce group; action=dynamic, ${groupSummary(group)}, " +
          s"reason=${if (desiredExceedsCap) "cappedExpansionRewrite" else "pressureTargetRewrite"}, " +
          s"${decisionSummary(stageDecisions)}, desired=${decisionSummary(desiredStageDecisions)}, " +
          s"cappedByExpansion=$desiredExceedsCap, groupTotalBytes=$groupTotalBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      Some(candidates)
    } else {
      logWarning(
        s"[JVM-AQE] would rewrite coalesce group; action=dynamic, " +
          s"reason=${if (desiredExceedsCap) "cappedExpansionRewrite" else "pressureTargetRewrite"}, " +
          s"${groupSummary(group)}, ${decisionSummary(stageDecisions)}, " +
          s"desired=${decisionSummary(desiredStageDecisions)}, " +
          s"cappedByExpansion=$desiredExceedsCap, " +
          s"groupTotalBytes=$groupTotalBytes, reducerCount=$reducerCount, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"groupMaxAllowedPartitions=$groupMaxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, pressureFactor=$pressureFactor%.4f, " +
          s"${JvmAqeSqlConf.CoalesceModifyPlansEnabled}=false, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      None
    }
  }

  private def rewriteReadWithDynamicTarget(
      read: AQEShuffleReadExec,
      stage: ShuffleQueryStageExec,
      oldSpecs: Seq[ShufflePartitionSpec],
      shuffleId: Int,
      mapStatsTotalBytes: Long,
      reducerCount: Int,
      maxReducerBytes: Long,
      pressure: Double,
      pressureView: PressureView,
      dhSignal: JvmAqeDhSignal,
      oldByteSummary: JvmCoalesceByteSummary): SparkPlan = {
    val pressureViewSummary = pressureView.compactString
    val baseTarget = advisoryPartitionSize(stage)
    val controlPressure = dhSignal.controlPressure(pressure)
    val pressureFactor = dynamicTargetFactor(controlPressure)
    val pressureTarget = targetSize(baseTarget, pressureFactor)
    val minBytes = minShuffleBytes
    val tinyBytes = tinyStageBytes
    val tinyMaxPartitions = tinyStageMaxPartitions
    val expansionFactor = maxExpansionFactor

    if (mapStatsTotalBytes < minBytes) {
      logInfo(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=minShuffleBytesGuard, " +
          s"oldPartitions=${oldSpecs.length}, reducerCount=$reducerCount, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, minShuffleBytes=$minBytes, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return read
    }

    if (oldSpecs.length <= tinyMaxPartitions && mapStatsTotalBytes <= tinyBytes) {
      logInfo(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=tinyLateStageGuard, " +
          s"oldPartitions=${oldSpecs.length}, tinyStageMaxPartitions=$tinyMaxPartitions, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, tinyStageBytes=$tinyBytes, " +
          s"reducerCount=$reducerCount, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return read
    }

    if (oldSpecs.length >= reducerCount) {
      logInfo(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=noReducerHeadroom, " +
          s"oldPartitions=${oldSpecs.length}, reducerCount=$reducerCount, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return read
    }

    val maxAllowedPartitions = cappedPartitionCount(oldSpecs.length, reducerCount, expansionFactor)
    val capTarget = targetForMaxPartitions(mapStatsTotalBytes, maxAllowedPartitions)
    val effectiveTarget = math.max(pressureTarget, capTarget)
    val minPartitionSize = minPartitionSizeFor(effectiveTarget)
    val maybeNewSpecs = coalescedSpecs(stage, effectiveTarget, minPartitionSize)
    if (maybeNewSpecs.isEmpty) {
      logInfo(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=shufflePartitionsUtilNoChange, " +
          s"oldPartitions=${oldSpecs.length}, baseTarget=$baseTarget, " +
          s"pressureTarget=$pressureTarget, effectiveTarget=$effectiveTarget, " +
          s"minPartitionSize=$minPartitionSize, maxAllowedPartitions=$maxAllowedPartitions, " +
          s"capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=n/a, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      return read
    }
    val newSpecs = maybeNewSpecs.get
    val newByteSummary = coalescedBytesSummary(Seq(newSpecs), mapStatsTotalBytes, newSpecs.length)

    if (newSpecs.length < oldSpecs.length) {
      logWarning(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=wouldReducePartitionsUnderPressure, " +
          s"oldPartitions=${oldSpecs.length}, calculatedPartitions=${newSpecs.length}, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"maxAllowedPartitions=$maxAllowedPartitions, capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      read
    } else if (newSpecs.length == oldSpecs.length) {
      val reason = if (newSpecs == oldSpecs) {
        "unchanged"
      } else {
        "noPartitionIncrease"
      }
      logInfo(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, " +
          s"action=keep, reason=$reason, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"maxAllowedPartitions=$maxAllowedPartitions, capTarget=$capTarget, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          s"partitions=${oldSpecs.length}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      read
    } else if (newSpecs.length > maxAllowedPartitions) {
      logWarning(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=expansionCapGuard, " +
          s"oldPartitions=${oldSpecs.length}, calculatedPartitions=${newSpecs.length}, " +
          s"maxAllowedPartitions=$maxAllowedPartitions, maxExpansionFactor=$expansionFactor, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      read
    } else if (benefitGuardTriggered(oldByteSummary, baseTarget, pressureView)) {
      val avgThreshold = targetBytesForFactor(baseTarget, minOldAvgFactor)
      val maxThreshold = targetBytesForFactor(baseTarget, minOldMaxFactor)
      logInfo(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=benefitGuard, " +
          s"oldPartitions=${oldSpecs.length}, calculatedPartitions=${newSpecs.length}, " +
          s"maxAllowedPartitions=$maxAllowedPartitions, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"capTarget=$capTarget, ${oldByteSummary.logString}, " +
          s"newAvgBytes=${newByteSummary.avgBytes}, " +
          s"minOldAvgFactor=$minOldAvgFactor, minOldMaxFactor=$minOldMaxFactor, " +
          s"minOldAvgBytes=$avgThreshold, minOldMaxBytes=$maxThreshold, " +
          s"benefitGuardBypass=${benefitGuardBypassSummary(pressureView)}, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      read
    } else if (highWatermarkAgeGuardTriggered(pressureView)) {
      logInfo(
        s"[JVM-AQE] keeping coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, action=keep, reason=highWatermarkAgeGuard, " +
          s"oldPartitions=${oldSpecs.length}, calculatedPartitions=${newSpecs.length}, " +
          s"maxAllowedPartitions=$maxAllowedPartitions, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"capTarget=$capTarget, ${oldByteSummary.logString}, " +
          s"newAvgBytes=${newByteSummary.avgBytes}, " +
          s"selectedPressure=${selectedPressure(pressureView)}, " +
          s"highWatermarkAgeMs=${pressureView.highWatermarkAgeMs.getOrElse(-1L)}, " +
          s"maxHighWatermarkAgeMsForRewrite=$maxHighWatermarkAgeMsForRewrite, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      read
    } else if (modifyPlans) {
      logWarning(
        s"[JVM-AQE] rewriting coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, oldPartitions=${oldSpecs.length}, " +
          s"newPartitions=${newSpecs.length}, maxAllowedPartitions=$maxAllowedPartitions, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      AQEShuffleReadExec(stage, newSpecs)
    } else {
      logWarning(
        s"[JVM-AQE] would rewrite coalesced AQEShuffleReadExec for shuffleStage=${stage.id}, " +
          s"shuffle=$shuffleId, oldPartitions=${oldSpecs.length}, " +
          s"newPartitions=${newSpecs.length}, maxAllowedPartitions=$maxAllowedPartitions, " +
          s"baseTarget=$baseTarget, pressureTarget=$pressureTarget, " +
          s"effectiveTarget=$effectiveTarget, minPartitionSize=$minPartitionSize, " +
          s"capTarget=$capTarget, " +
          s"${oldByteSummary.logString}, newAvgBytes=${newByteSummary.avgBytes}, " +
          f"pressure=$pressure%.4f, controlPressure=$controlPressure%.4f, " +
          f"pressureFactor=$pressureFactor%.4f, signal=${dhSignal.compactString}, " +
          s"${JvmAqeSqlConf.CoalesceModifyPlansEnabled}=false, " +
          s"mapStatsTotalBytes=$mapStatsTotalBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, pressureView=$pressureViewSummary")
      read
    }
  }

  private def collectCoalesceGroups(plan: SparkPlan): Seq[Seq[JvmCoalesceStageInfo]] = plan match {
    case read @ AQEShuffleReadExec(_: ShuffleQueryStageExec, _) =>
      Seq(collectJvmCoalesceStageInfos(read)).filter(_.nonEmpty)
    case unary: UnaryExecNode =>
      collectCoalesceGroups(unary.child)
    case union: UnionExec =>
      union.children.flatMap(collectCoalesceGroups)
    case join: CartesianProductExec =>
      join.children.flatMap(collectCoalesceGroups)
    case join: BroadcastHashJoinExec =>
      join.children.flatMap(collectCoalesceGroups)
    case join: BroadcastNestedLoopJoinExec =>
      join.children.flatMap(collectCoalesceGroups)
    case p if p.collectLeaves().forall(_.isInstanceOf[ExchangeQueryStageExec]) =>
      Seq(collectJvmCoalesceStageInfos(p)).filter(_.nonEmpty)
    case _ =>
      Seq.empty
  }

  private def collectJvmCoalesceStageInfos(plan: SparkPlan): Seq[JvmCoalesceStageInfo] = plan match {
    case AQEShuffleReadExec(stage: ShuffleQueryStageExec, specs) =>
      Seq(JvmCoalesceStageInfo(stage, specs))
    case other =>
      other.children.flatMap(collectJvmCoalesceStageInfos)
  }

  private def invalidGroupReason(group: Seq[JvmCoalesceStageInfo]): Option[String] = {
    if (group.isEmpty) {
      Some("emptyGroup")
    } else if (group.exists(info => !info.stage.isMaterialized)) {
      Some("stageNotMaterialized")
    } else if (group.exists(_.stage.mapStats.isEmpty)) {
      Some("missingMapStats")
    } else if (group.exists(info => !isSupported(info.stage.shuffle))) {
      Some("unsupportedShuffleOrigin")
    } else if (group.exists(_.specs.isEmpty)) {
      Some("emptyPartitionSpecs")
    } else if (group.exists(info => info.specs.exists(
        spec => !spec.isInstanceOf[CoalescedPartitionSpec]))) {
      Some("nonCoalescedPartitionSpecs")
    } else if (group.map(readKey).distinct.length != group.length) {
      Some("duplicateReadKeysInGroup")
    } else if (group.map(_.stage.mapStats.get.bytesByPartitionId.length).distinct.length > 1) {
      Some("incompatibleReducerCounts")
    } else if (hasConflictingStageCandidates(group.map(info => readKey(info) -> info.specs))) {
      Some("conflictingDuplicateStageIds")
    } else {
      None
    }
  }

  private def updateShuffleReads(
      plan: SparkPlan,
      specsMap: Map[JvmCoalesceReadKey, Seq[ShufflePartitionSpec]]): SparkPlan = plan match {
    case AQEShuffleReadExec(stage: ShuffleQueryStageExec, specs) =>
      val key = JvmCoalesceReadKey(stage.id, partitionSpecKey(specs))
      specsMap.get(key).map(newSpecs => AQEShuffleReadExec(stage, newSpecs)).getOrElse(plan)
    case other =>
      other.mapChildren(updateShuffleReads(_, specsMap))
  }

  private def advisoryPartitionSize(group: Seq[JvmCoalesceStageInfo]): Long = {
    val defaultAdvisorySize = sqlConf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    group match {
      case Seq(stageInfo) =>
        stageInfo.stage.advisoryPartitionSize.getOrElse(defaultAdvisorySize)
      case _ =>
        defaultAdvisorySize
    }
  }

  private def hasConflictingStageCandidates(
      candidates: Seq[(JvmCoalesceReadKey, Seq[ShufflePartitionSpec])]): Boolean = {
    candidates.groupBy(_._1).exists { case (_, grouped) =>
      grouped.map { case (_, specs) => partitionSpecKey(specs) }.distinct.length > 1
    }
  }

  private def addGroupCandidates(
      specsMap: mutable.HashMap[JvmCoalesceReadKey, Seq[ShufflePartitionSpec]],
      candidates: Seq[(JvmCoalesceReadKey, Seq[ShufflePartitionSpec])],
      group: Seq[JvmCoalesceStageInfo]): Unit = {
    val conflictingKeys = candidates.collect {
      case (key, specs)
          if specsMap.get(key).exists(existing =>
            partitionSpecKey(existing) != partitionSpecKey(specs)) =>
        key
    }.toSet
    if (conflictingKeys.nonEmpty) {
      candidates.map(_._1).foreach(specsMap.remove)
      logWarning(
        s"[JVM-AQE] keeping coalesce group; action=keep, " +
          s"reason=conflictingExistingStageRewrite, conflictingKeys=$conflictingKeys, " +
          s"removedKeys=${candidates.map(_._1).mkString("[", ",", "]")}, " +
          s"${groupSummary(group)}")
    } else {
      candidates.foreach { case (key, specs) =>
        specsMap.put(key, specs)
      }
    }
  }

  private def capCoalescedSpecsByStage(
      specsByStage: Seq[Seq[ShufflePartitionSpec]],
      maxPartitions: Int): Option[Seq[Seq[ShufflePartitionSpec]]] = {
    if (specsByStage.isEmpty || maxPartitions <= 0) {
      return None
    }

    val coalescedByStage = specsByStage.map { specs =>
      if (specs.exists(spec => !spec.isInstanceOf[CoalescedPartitionSpec])) {
        return None
      }
      specs.map(_.asInstanceOf[CoalescedPartitionSpec])
    }
    val desiredPartitions = coalescedByStage.head.length
    if (desiredPartitions <= maxPartitions) {
      return Some(specsByStage)
    }
    if (coalescedByStage.exists(_.length != desiredPartitions)) {
      return None
    }

    val referenceRanges = coalescedByStage.head.map { spec =>
      (spec.startReducerIndex, spec.endReducerIndex)
    }
    if (!coalescedByStage.forall(stageSpecs =>
        stageSpecs.map(spec => (spec.startReducerIndex, spec.endReducerIndex)) == referenceRanges)) {
      return None
    }

    val chunks = 0.until(maxPartitions).map { outputIndex =>
      val start = (outputIndex.toLong * desiredPartitions / maxPartitions).toInt
      val end = (((outputIndex + 1).toLong * desiredPartitions / maxPartitions).toInt)
        .max(start + 1)
        .min(desiredPartitions)
      (start, end)
    }

    Some(coalescedByStage.map { stageSpecs =>
      chunks.map { case (startIndex, endIndex) =>
        val chunk = stageSpecs.slice(startIndex, endIndex)
        val dataSize = chunk.map(_.dataSize.getOrElse(0L)).sum
        CoalescedPartitionSpec(
          chunk.head.startReducerIndex,
          chunk.last.endReducerIndex,
          dataSize)
      }
    })
  }

  private def groupSummary(group: Seq[JvmCoalesceStageInfo]): String = {
    val stageIds = group.map(_.stage.id).mkString(",")
    val shuffleIds = group.map(_.stage.mapStats.map(_.shuffleId).getOrElse(-1)).mkString(",")
    val oldPartitions = group.map(info => s"${info.stage.id}:${info.oldPartitionCount}")
      .mkString(",")
    s"shuffleStages=[$stageIds], shuffles=[$shuffleIds], oldPartitionsByStage=[$oldPartitions]"
  }

  private def decisionSummary(decisions: Seq[JvmCoalesceStageDecision]): String = {
    val body = decisions.map { decision =>
      s"${decision.info.stage.id}:${decision.info.oldPartitionCount}->" +
        s"${decision.newPartitionCount}/cap=${decision.maxAllowedPartitions}"
    }.mkString(",")
    s"partitionsByStage=[$body]"
  }

  private def partitionSpecKey(specs: Seq[ShufflePartitionSpec]): Seq[(Int, Int)] = {
    specs.map {
      case spec: CoalescedPartitionSpec =>
        (spec.startReducerIndex, spec.endReducerIndex)
      case _ =>
        (-1, -1)
    }
  }

  private def readKey(info: JvmCoalesceStageInfo): JvmCoalesceReadKey = {
    JvmCoalesceReadKey(info.stage.id, partitionSpecKey(info.specs))
  }

  private def coalescedSpecs(
      stage: ShuffleQueryStageExec,
      targetSize: Long,
      minPartitionSize: Long): Option[Seq[ShufflePartitionSpec]] = {
    val newSpecs = ShufflePartitionsUtil.coalescePartitions(
      Seq(stage.mapStats),
      Seq(None),
      advisoryTargetSize = targetSize,
      minNumPartitions = 1,
      minPartitionSize = minPartitionSize)

    newSpecs.headOption
  }

  private def advisoryPartitionSize(stage: ShuffleQueryStageExec): Long = {
    val defaultAdvisorySize = sqlConf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    stage.advisoryPartitionSize.getOrElse(defaultAdvisorySize)
  }

  private def dynamicTargetFactor(pressure: Double): Double = {
    val denominator = math.max(0.0001d, 1.0d - pressureThreshold)
    val pressureSeverity = clamp((pressure - pressureThreshold) / denominator, 0.0d, 1.0d)
    val factor = 1.0d - pressureSeverity * sensitivity * (1.0d - minTargetFactor)
    clamp(factor, minTargetFactor, 1.0d)
  }

  private def targetSize(baseTarget: Long, factor: Double): Long = {
    math.max(1L, (baseTarget.toDouble * clamp(factor, minTargetFactor, 1.0d)).toLong)
  }

  private def cappedPartitionCount(
      oldPartitions: Int,
      reducerCount: Int,
      expansionFactor: Double): Int = {
    val expanded = math.ceil(oldPartitions.toDouble * expansionFactor).toInt
    math.max(oldPartitions + 1, expanded).min(reducerCount)
  }

  private def targetForMaxPartitions(totalBytes: Long, maxPartitions: Int): Long = {
    if (totalBytes <= 0L || maxPartitions <= 0) {
      1L
    } else {
      math.max(1L, math.ceil(totalBytes.toDouble / maxPartitions.toDouble).toLong)
    }
  }

  private def coalescedBytesSummary(
      specsByStage: Seq[Seq[ShufflePartitionSpec]],
      fallbackTotalBytes: Long,
      fallbackPartitions: Int): JvmCoalesceByteSummary = {
    combinedCoalescedPartitionBytes(specsByStage) match {
      case Some(bytes) if bytes.nonEmpty =>
        JvmCoalesceByteSummary(averageBytes(bytes.sum, bytes.length), bytes.max)
      case _ =>
        val fallbackAvg = averageBytes(fallbackTotalBytes, fallbackPartitions)
        val individualBytes = specsByStage.flatMap { specs =>
          specs.collect {
            case spec: CoalescedPartitionSpec => spec.dataSize.getOrElse(0L)
          }
        }
        val fallbackMax = if (individualBytes.nonEmpty) {
          individualBytes.max
        } else {
          fallbackAvg
        }
        JvmCoalesceByteSummary(fallbackAvg, fallbackMax)
    }
  }

  private def combinedCoalescedPartitionBytes(
      specsByStage: Seq[Seq[ShufflePartitionSpec]]): Option[Seq[Long]] = {
    if (specsByStage.isEmpty) {
      return None
    }
    val coalescedByStage = specsByStage.map { specs =>
      if (specs.exists(spec => !spec.isInstanceOf[CoalescedPartitionSpec])) {
        return None
      }
      specs.map(_.asInstanceOf[CoalescedPartitionSpec])
    }
    val partitionCount = coalescedByStage.head.length
    if (partitionCount == 0 || coalescedByStage.exists(_.length != partitionCount)) {
      return None
    }

    val referenceRanges = coalescedByStage.head.map { spec =>
      (spec.startReducerIndex, spec.endReducerIndex)
    }
    if (!coalescedByStage.forall(stageSpecs =>
        stageSpecs.map(spec => (spec.startReducerIndex, spec.endReducerIndex)) == referenceRanges)) {
      return None
    }

    Some(0.until(partitionCount).map { partitionIndex =>
      coalescedByStage.map(_(partitionIndex).dataSize.getOrElse(0L)).sum
    })
  }

  private def averageBytes(totalBytes: Long, partitions: Int): Long = {
    if (totalBytes <= 0L || partitions <= 0) {
      0L
    } else {
      math.ceil(totalBytes.toDouble / partitions.toDouble).toLong
    }
  }

  private def benefitGuardTriggered(
      oldByteSummary: JvmCoalesceByteSummary,
      baseTarget: Long,
      pressureView: PressureView): Boolean = {
    minOldAvgFactor > 0.0d &&
      minOldMaxFactor > 0.0d &&
      !benefitGuardBypassTriggered(pressureView) &&
      oldByteSummary.avgBytes < targetBytesForFactor(baseTarget, minOldAvgFactor) &&
      oldByteSummary.maxBytes < targetBytesForFactor(baseTarget, minOldMaxFactor)
  }

  private def benefitGuardBypassTriggered(pressureView: PressureView): Boolean = {
    pressureView.selected.exists { snapshot =>
      val humongousGc =
        benefitGuardBypassOnHumongousGc &&
          snapshot.lastGcCause.toLowerCase(java.util.Locale.ROOT).contains("humongous")
      val lowReclaimGc =
        benefitGuardBypassLowReclaimBytes > 0L &&
          snapshot.lastGcDurationMs > 0L &&
          snapshot.lastGcReclaimedBytes <= benefitGuardBypassLowReclaimBytes
      humongousGc || lowReclaimGc
    }
  }

  private def benefitGuardBypassSummary(pressureView: PressureView): String = {
    pressureView.selected.map { snapshot =>
      val triggered = benefitGuardBypassTriggered(pressureView)
      s"triggered=$triggered humongousEnabled=$benefitGuardBypassOnHumongousGc " +
        s"lowReclaimBytes=$benefitGuardBypassLowReclaimBytes " +
        s"lastGcCause=${snapshot.lastGcCause} " +
        s"lastGcDurationMs=${snapshot.lastGcDurationMs} " +
        s"lastGcReclaimedBytes=${snapshot.lastGcReclaimedBytes}"
    }.getOrElse(
      s"triggered=false humongousEnabled=$benefitGuardBypassOnHumongousGc " +
        s"lowReclaimBytes=$benefitGuardBypassLowReclaimBytes selected=none")
  }

  private def highWatermarkAgeGuardTriggered(pressureView: PressureView): Boolean = {
    val maxAgeMs = maxHighWatermarkAgeMsForRewrite
    maxAgeMs >= 0L &&
      highWatermarkOnlyPressure(pressureView) &&
      pressureView.highWatermarkAgeMs.exists(_ > maxAgeMs)
  }

  private def highWatermarkOnlyPressure(pressureView: PressureView): Boolean = {
    pressureView.highWatermarkPressure >= pressureThreshold &&
      pressureView.highWatermarkPressure >= selectedPressure(pressureView) &&
      selectedPressure(pressureView) < pressureThreshold
  }

  private def selectedPressure(pressureView: PressureView): Double = {
    pressureView.selected.map { snapshot =>
      math.max(snapshot.heapUsedRatio, clamp(snapshot.recentGcTimeRatio, 0.0d, 1.0d))
    }.getOrElse(0.0d)
  }

  private def targetBytesForFactor(baseTarget: Long, factor: Double): Long = {
    if (baseTarget <= 0L || factor <= 0.0d) {
      0L
    } else {
      math.min(Long.MaxValue.toDouble, baseTarget.toDouble * factor).toLong
    }
  }

  private def minPartitionSizeFor(targetSize: Long): Long = {
    if (Utils.isTesting) {
      sqlConf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE).min(targetSize / 5L)
    } else {
      sqlConf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_SIZE)
    }
  }

  private def identitySpecs(stage: ShuffleQueryStageExec): Seq[ShufflePartitionSpec] = {
    stage.mapStats match {
      case Some(stats) if stats.bytesByPartitionId.nonEmpty =>
        stats.bytesByPartitionId.zipWithIndex.map { case (size, reducerIndex) =>
          CoalescedPartitionSpec(reducerIndex, reducerIndex + 1, size)
        }.toSeq
      case _ =>
        Seq(CoalescedPartitionSpec(0, 0, 0L))
    }
  }

  private def clamp(value: Double, min: Double, max: Double): Double = {
    math.max(min, math.min(max, value))
  }
}

private case class JvmCoalesceStageInfo(
    stage: ShuffleQueryStageExec,
    specs: Seq[ShufflePartitionSpec]) {
  def oldPartitionCount: Int = specs.length
}

private case class JvmCoalesceReadKey(
    stageId: Int,
    currentSpecs: Seq[(Int, Int)])

private case class JvmCoalesceStageDecision(
    info: JvmCoalesceStageInfo,
    newSpecs: Seq[ShufflePartitionSpec],
    maxAllowedPartitions: Int) {
  def newPartitionCount: Int = newSpecs.length
}

private case class JvmCoalesceByteSummary(
    avgBytes: Long,
    maxBytes: Long) {
  def logString: String =
    s"oldAvgCoalescedBytes=$avgBytes, oldMaxCoalescedBytes=$maxBytes"
}

private[adaptive] object JvmAqeSqlConf {
  val Enabled = "spark.jvmAqe.sql.enabled"
  val ModifyPlansEnabled = "spark.jvmAqe.enabled"
  val PressureThreshold = "spark.jvmAqe.sql.pressureThreshold"
  val DecisionLogEnabled = "spark.jvmAqe.sql.decisionLog.enabled"
  val SnapshotStaleThresholdMs = "spark.jvmAqe.sql.snapshotStaleThresholdMs"
  val PressureHighWatermarkWindowMs = "spark.jvmAqe.sql.pressureHighWatermarkWindowMs"

  val CoalesceEnabled = "spark.jvmAqe.sql.coalesce.enabled"
  val CoalesceModifyPlansEnabled = "spark.jvmAqe.sql.coalesce.modifyPlans.enabled"
  val CoalesceAction = "spark.jvmAqe.sql.coalesce.action"
  val CoalesceMinTargetFactor = "spark.jvmAqe.sql.coalesce.minTargetFactor"
  val CoalesceSensitivity = "spark.jvmAqe.sql.coalesce.sensitivity"
  val CoalesceMinShuffleBytes = "spark.jvmAqe.sql.coalesce.minShuffleBytes"
  val CoalesceTinyStageBytes = "spark.jvmAqe.sql.coalesce.tinyStageBytes"
  val CoalesceTinyStageMaxPartitions = "spark.jvmAqe.sql.coalesce.tinyStageMaxPartitions"
  val CoalesceMaxExpansionFactor = "spark.jvmAqe.sql.coalesce.maxExpansionFactor"
  val CoalesceMinOldAvgFactor = "spark.jvmAqe.sql.coalesce.minOldAvgFactor"
  val CoalesceMinOldMaxFactor = "spark.jvmAqe.sql.coalesce.minOldMaxFactor"
  val CoalesceMaxHighWatermarkAgeMsForRewrite =
    "spark.jvmAqe.sql.coalesce.maxHighWatermarkAgeMsForRewrite"
  val CoalesceBenefitGuardBypassOnHumongousGc =
    "spark.jvmAqe.sql.coalesce.benefitGuardBypassOnHumongousGc"
  val CoalesceBenefitGuardBypassLowReclaimBytes =
    "spark.jvmAqe.sql.coalesce.benefitGuardBypassLowReclaimBytes"
  val CoalesceActionDynamic = "dynamic"
  val CoalesceActionRestore = "restore"

  val JoinSelectionEnabled = "spark.jvmAqe.sql.joinSelection.enabled"
  val JoinSelectionModifyPlansEnabled = "spark.jvmAqe.sql.joinSelection.modifyPlans.enabled"
  val JoinSelectionDisableAdaptiveBroadcastOnRiskEnabled =
    "spark.jvmAqe.sql.joinSelection.disableAdaptiveBroadcastOnRisk.enabled"
  val BroadcastExpansionFactor = "spark.jvmAqe.sql.joinSelection.broadcastExpansionFactor"
  val BroadcastHeapSafetyFraction = "spark.jvmAqe.sql.joinSelection.broadcastHeapSafetyFraction"
  val BroadcastTinyFloorBytes = "spark.jvmAqe.sql.joinSelection.broadcastTinyFloorBytes"

  // Entropy observe-only mode config keys
  val EntropyObserveEnabled       = "spark.jvmAqe.sql.entropy.observe.enabled"
  val EntropyKInflation           = "spark.jvmAqe.sql.entropy.kInflation"
  val EntropyOpMultiplierSort     = "spark.jvmAqe.sql.entropy.opMultiplier.sort"
  val EntropyOpMultiplierSmj      = "spark.jvmAqe.sql.entropy.opMultiplier.smj"
  val EntropyOpMultiplierHashAgg  = "spark.jvmAqe.sql.entropy.opMultiplier.hashAgg"
  val EntropyOpMultiplierBhj      = "spark.jvmAqe.sql.entropy.opMultiplier.bhj"
  val EntropyOpMultiplierDefault  = "spark.jvmAqe.sql.entropy.opMultiplier.default"
  val EntropyHThreshold                = "spark.jvmAqe.sql.entropy.h.threshold"
  val EntropyDThreshold                = "spark.jvmAqe.sql.entropy.d.threshold"
  val EntropyHWeights                  = "spark.jvmAqe.sql.entropy.h.weights"
  val EntropyHWindowMs                 = "spark.jvmAqe.sql.entropy.h.windowMs"
  val EntropyHLowReclaimThresholdBytes = "spark.jvmAqe.sql.entropy.h.lowReclaimThresholdBytes"

  def effectivePressure: Double = {
    JvmPressureStore.currentPressureView(Long.MaxValue).pressure
  }

  private def clamp(value: Double, min: Double, max: Double): Double = {
    math.max(min, math.min(max, value))
  }
}
