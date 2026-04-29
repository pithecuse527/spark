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

import org.apache.spark.SparkConf

object JvmAqeConf {
  val ENABLED = "spark.jvmAqe.enabled"
  val MODIFY_PLANS_ENABLED = "spark.jvmAqe.modifyPlans.enabled"
  val PRESSURE_THRESHOLD = "spark.jvmAqe.pressureThreshold"
  val COALESCE_ENABLED = "spark.jvmAqe.coalesce.enabled"
  val COALESCE_MODIFY_PLANS_ENABLED = "spark.jvmAqe.coalesce.modifyPlans.enabled"
  val COALESCE_MIN_TARGET_FACTOR = "spark.jvmAqe.coalesce.minTargetFactor"
  val COALESCE_SENSITIVITY = "spark.jvmAqe.coalesce.sensitivity"
  val JOIN_SELECTION_ENABLED = "spark.jvmAqe.joinSelection.enabled"
  val JOIN_SELECTION_MODIFY_PLANS_ENABLED =
    "spark.jvmAqe.joinSelection.modifyPlans.enabled"

  val ExecutorSampleIntervalMs = "spark.jvmAqe.executor.sampleIntervalMs"
  val DriverSampleIntervalMs = "spark.jvmAqe.driver.sampleIntervalMs"
  val RecentGcWindowMs = "spark.jvmAqe.recentGcWindowMs"
  val LogSnapshots = "spark.jvmAqe.logSnapshots"

  val DefaultExecutorSampleIntervalMs: Long = 1000L
  val DefaultDriverSampleIntervalMs: Long = 1000L
  val DefaultRecentGcWindowMs: Long = 30000L
  val DefaultLogSnapshots: Boolean = true

  def executorSampleIntervalMs(conf: SparkConf): Long = {
    positiveLong(conf, ExecutorSampleIntervalMs, DefaultExecutorSampleIntervalMs)
  }

  def driverSampleIntervalMs(conf: SparkConf): Long = {
    positiveLong(conf, DriverSampleIntervalMs, DefaultDriverSampleIntervalMs)
  }

  def recentGcWindowMs(conf: SparkConf): Long = {
    positiveLong(conf, RecentGcWindowMs, DefaultRecentGcWindowMs)
  }

  def logSnapshots(conf: SparkConf): Boolean = {
    conf.getBoolean(LogSnapshots, DefaultLogSnapshots)
  }

  private def positiveLong(conf: SparkConf, key: String, defaultValue: Long): Long = {
    val value = conf.getLong(key, defaultValue)
    if (value > 0L) value else defaultValue
  }
}
