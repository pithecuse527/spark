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

import org.apache.spark.MapOutputStatistics
import org.apache.spark.internal.Logging
import org.apache.spark.jvmaqe.JvmPressureStore
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{
  HintInfo,
  Join,
  JoinHint,
  LogicalPlan,
  NO_BROADCAST_HASH
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Adds NO_BROADCAST_HASH hints during AQE re-optimization when a broadcast-eligible shuffled
 * join side is associated with high JVM pressure.
 */
final class JvmPressureAwareJoinSelection(session: SparkSession)
  extends Rule[LogicalPlan] with JoinSelectionHelper with Logging {

  private def enabled: Boolean =
    session.conf.get(JvmAqeSqlConf.Enabled, "true").toBoolean &&
      session.conf.get(JvmAqeSqlConf.JoinSelectionEnabled, "true").toBoolean

  private def modifyPlans: Boolean =
    session.conf.get(JvmAqeSqlConf.JoinSelectionModifyPlansEnabled,
      session.conf.get(JvmAqeSqlConf.ModifyPlansEnabled, "false")).toBoolean

  private def pressureThreshold: Double =
    session.conf.get(JvmAqeSqlConf.PressureThreshold, "0.70").toDouble

  private def broadcastExpansionFactor: Double =
    math.max(1.0d, session.conf.get(JvmAqeSqlConf.BroadcastExpansionFactor, "7.0").toDouble)

  private def broadcastHeapSafetyFraction: Double = {
    val configured = session.conf.get(JvmAqeSqlConf.BroadcastHeapSafetyFraction, "0.60").toDouble
    math.max(0.01d, math.min(1.0d, configured))
  }

  private def broadcastTinyFloorBytes: Long =
    math.max(0L, session.conf.get(JvmAqeSqlConf.BroadcastTinyFloorBytes, "0").toLong)

  private def decisionLogEnabled: Boolean =
    session.conf.get(JvmAqeSqlConf.DecisionLogEnabled, "true").toBoolean

  private def disableAdaptiveBroadcastOnRisk: Boolean =
    session.conf.get(JvmAqeSqlConf.JoinSelectionDisableAdaptiveBroadcastOnRiskEnabled,
      "true").toBoolean

  private def snapshotStaleThresholdMs: Long =
    math.max(0L, session.conf.get(JvmAqeSqlConf.SnapshotStaleThresholdMs, "10000").toLong)

  private def broadcastThreshold: Long = {
    val conf = session.sessionState.conf
    conf.getConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD)
      .getOrElse(conf.autoBroadcastJoinThreshold)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!enabled) {
      return plan
    }

    plan.transformDown {
      case j @ ExtractEquiJoinKeys(_, _, _, _, _, _, _, hint) =>
        val leftDecision = sideDecision(j, isLeft = true, hint)
        val rightDecision = sideDecision(j, isLeft = false, hint)
        val decisions = Seq(leftDecision, rightDecision).flatten

        if (decisions.isEmpty) {
          j
        } else if (modifyPlans) {
          decisions.foreach(logDecision(_, dryRun = false))
          j.copy(hint = applyDecisions(hint, decisions))
        } else {
          decisions.foreach(logDecision(_, dryRun = true))
          j
        }
    }
  }

  private def sideDecision(
      join: Join,
      isLeft: Boolean,
      hint: JoinHint): Option[JvmBroadcastDecision] = {
    val existingHint = if (isLeft) hint.leftHint else hint.rightHint
    if (existingHint.exists(_.strategy.isDefined)) {
      return None
    }

    val canBuild = if (isLeft) {
      canBuildBroadcastLeft(join.joinType)
    } else {
      canBuildBroadcastRight(join.joinType)
    }
    if (!canBuild || broadcastThreshold < 0) {
      return None
    }

    val plan = if (isLeft) join.left else join.right
    plan match {
      case LogicalQueryStage(_, stage: ShuffleQueryStageExec)
          if stage.isMaterialized && stage.mapStats.isDefined =>
        pressureDecision(join, isLeft, stage, stage.mapStats.get)
      case _ =>
        None
    }
  }

  private def pressureDecision(
      join: Join,
      isLeft: Boolean,
      stage: ShuffleQueryStageExec,
      mapStats: MapOutputStatistics): Option[JvmBroadcastDecision] = {
    val shuffleId = mapStats.shuffleId
    val sizeInBytes = mapStats.bytesByPartitionId.sum
    val pressureView = JvmPressureStore.currentPressureView(snapshotStaleThresholdMs)
    val pressure = pressureView.pressure
    val dhSignal = JvmAqeDhSignal.forBytes(session, Seq(sizeInBytes), "broadcasthashjoin")
    val broadcastRisky = JvmPressureStore.isBroadcastRisky(
      sizeInBytes,
      broadcastExpansionFactor,
      broadcastHeapSafetyFraction,
      snapshotStaleThresholdMs)
    val reducerCount = mapStats.bytesByPartitionId.length
    val maxReducerBytes =
      if (mapStats.bytesByPartitionId.isEmpty) 0L else mapStats.bytesByPartitionId.max
    val broadcastEligible = sizeInBytes <= broadcastThreshold
    val tinyFloor = broadcastTinyFloorBytes
    if (tinyFloor > 0L && broadcastEligible && sizeInBytes < tinyFloor) {
      if (decisionLogEnabled) {
        val side = if (isLeft) "left" else "right"
        logInfo(
          s"[JVM-AQE] tiny-broadcast floor; keep BHJ; side=$side, " +
            s"shuffleStage=${stage.id}, shuffle=$shuffleId, joinType=${join.joinType}, " +
            s"sideSize=$sizeInBytes, tinyFloorBytes=$tinyFloor, " +
            s"broadcastThreshold=$broadcastThreshold, " +
            f"pressure=$pressure%.4f, pressureThreshold=$pressureThreshold%.4f, " +
            s"signal=${dhSignal.compactString}")
      }
      return None
    }
    val dhRisky = dhSignal.highD && (dhSignal.highH || broadcastRisky)
    val shouldAvoidBroadcast =
      broadcastEligible && (pressure >= pressureThreshold || broadcastRisky || dhRisky)

    if (decisionLogEnabled) {
      val side = if (isLeft) "left" else "right"
      logInfo(
        s"[JVM-AQE] considered broadcast join side; side=$side, " +
          s"shuffleStage=${stage.id}, shuffle=$shuffleId, joinType=${join.joinType}, " +
          s"sideSize=$sizeInBytes, reducerCount=$reducerCount, " +
          s"maxReducerBytes=$maxReducerBytes, broadcastThreshold=$broadcastThreshold, " +
          s"broadcastEligible=$broadcastEligible, broadcastRisky=$broadcastRisky, " +
          s"dhRisky=$dhRisky, signal=${dhSignal.compactString}, " +
          f"pressure=$pressure%.4f, pressureThreshold=$pressureThreshold%.4f, " +
          s"modifyPlans=$modifyPlans, pressureView=${pressureView.compactString}")
    }

    if (shouldAvoidBroadcast) {
      if (modifyPlans && disableAdaptiveBroadcastOnRisk) {
        disableAdaptiveBroadcastThreshold(sizeInBytes, pressureView.compactString)
      }
      Some(JvmBroadcastDecision(
        isLeft = isLeft,
        join = join,
        shuffleStageId = stage.id,
        shuffleId = shuffleId,
        sizeInBytes = sizeInBytes,
        pressure = pressure,
        signalSummary = dhSignal.compactString,
        pressureSummary = pressureView.compactString))
    } else {
      None
    }
  }

  private def disableAdaptiveBroadcastThreshold(
      riskySideSize: Long,
      pressureSummary: String): Unit = {
    val key = SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key
    val currentValue = session.conf.get(key, null)
    if (currentValue != "-1") {
      session.conf.set(key, "-1")
      logWarning(
        s"[JVM-AQE] disabled adaptive auto broadcast threshold after risky side; " +
          s"$key=-1, previous=$currentValue, riskySideSize=$riskySideSize, " +
          s"pressureSummary=$pressureSummary")
    }
  }

  private def applyDecisions(
      hint: JoinHint,
      decisions: Seq[JvmBroadcastDecision]): JoinHint = {
    decisions.foldLeft(hint) { case (current, decision) =>
      if (decision.isLeft) {
        current.copy(leftHint = Some(current.leftHint.getOrElse(HintInfo())
          .copy(strategy = Some(NO_BROADCAST_HASH))))
      } else {
        current.copy(rightHint = Some(current.rightHint.getOrElse(HintInfo())
          .copy(strategy = Some(NO_BROADCAST_HASH))))
      }
    }
  }

  private def logDecision(decision: JvmBroadcastDecision, dryRun: Boolean): Unit = {
    val action = if (dryRun) "would inject" else "injecting"
    val side = if (decision.isLeft) "left" else "right"
    logWarning(
      s"[JVM-AQE] $action NO_BROADCAST_HASH on $side join side; " +
        s"shuffleStage=${decision.shuffleStageId}, shuffle=${decision.shuffleId}, " +
        s"joinType=${decision.join.joinType}, " +
        s"sideSize=${decision.sizeInBytes}, broadcastThreshold=$broadcastThreshold, " +
        f"pressure=${decision.pressure}%.4f, " +
        s"signal=${decision.signalSummary}, " +
        s"${JvmAqeSqlConf.JoinSelectionModifyPlansEnabled}=$modifyPlans, " +
        s"pressureSummary=${decision.pressureSummary}")
  }
}

private final case class JvmBroadcastDecision(
    isLeft: Boolean,
    join: Join,
    shuffleStageId: Int,
    shuffleId: Int,
    sizeInBytes: Long,
    pressure: Double,
    signalSummary: String,
    pressureSummary: String)
