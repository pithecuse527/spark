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

import java.lang.management.{GarbageCollectorMXBean, ManagementFactory, MemoryUsage}
import java.net.InetAddress
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.JavaConverters._

import com.codahale.metrics.Gauge

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.internal.Logging

class JvmAqeDriverPlugin extends DriverPlugin with Logging {
  private val memoryMxBean = ManagementFactory.getMemoryMXBean
  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala.toSeq
  private val latest = new AtomicReference[JvmPressureSnapshot]()
  private val receivedSnapshots = new AtomicLong(0L)

  @volatile private var scheduler: ScheduledExecutorService = _
  @volatile private var logSnapshots: Boolean = JvmAqeConf.DefaultLogSnapshots
  @volatile private var pluginContext: PluginContext = _

  override def init(
      sc: SparkContext,
      pluginContext: PluginContext): util.Map[String, String] = {
    this.pluginContext = pluginContext
    logSnapshots = JvmAqeConf.logSnapshots(pluginContext.conf())
    JvmPressureStore.reset()

    val intervalMs = JvmAqeConf.driverSampleIntervalMs(pluginContext.conf())
    scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory(
      "jvm-aqe-driver-sampler"))
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = sampleDriverJvm()
      },
      0L,
      intervalMs,
      TimeUnit.MILLISECONDS)

    logInfo(
      s"[JVM-AQE] Driver plugin initialized intervalMs=$intervalMs logSnapshots=$logSnapshots")
    Collections.emptyMap()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    val registry = pluginContext.metricRegistry()
    registry.register("receivedSnapshots", gauge(receivedSnapshots.get()))
    registry.register("latestHeapUsed", gauge(latestSnapshot.map(_.heapUsed).getOrElse(0L)))
    registry.register("latestHeapMax", gauge(latestSnapshot.map(_.heapMax).getOrElse(0L)))
    registry.register("latestRecentGcTimeRatio", gauge(
      latestSnapshot.map(_.recentGcTimeRatio).getOrElse(0.0d)))
    registry.register("trackedJvmProcesses", gauge(JvmPressureStore.allSnapshots.size))
  }

  override def receive(message: Object): Object = {
    message match {
      case snapshot: JvmPressureSnapshot =>
        val restamped = snapshot.copy(timestampMs = System.currentTimeMillis())
        JvmPressureStore.update(restamped)
        latest.set(restamped)
        receivedSnapshots.incrementAndGet()
        if (logSnapshots) {
          logInfo(s"[JVM-AQE] Received JVM pressure snapshot: ${restamped.compactString}")
        }
        "ACK"
      case other =>
        val className = if (other == null) "null" else other.getClass.getName
        logWarning(s"[JVM-AQE] Ignoring unknown plugin message type: $className")
        "UNKNOWN"
    }
  }

  override def shutdown(): Unit = {
    val localScheduler = scheduler
    if (localScheduler != null) {
      localScheduler.shutdownNow()
    }
    logInfo(s"[JVM-AQE] Driver plugin stopped. ${JvmPressureStore.summaryString}")
    JvmPressureStore.reset()
  }

  private def sampleDriverJvm(): Unit = {
    try {
      val snapshot = currentDriverSnapshot()
      JvmPressureStore.update(snapshot)
      latest.set(snapshot)
    } catch {
      case e: Exception =>
        logWarning(s"[JVM-AQE] Failed to sample driver JVM pressure: ${e.getMessage}")
    }
  }

  private def currentDriverSnapshot(): JvmPressureSnapshot = {
    val heap = memoryMxBean.getHeapMemoryUsage
    val nonHeap = memoryMxBean.getNonHeapMemoryUsage
    val gcStats = aggregateGcStats(gcBeans)
    JvmPressureSnapshot(
      executorId = "driver",
      host = localHostName(),
      timestampMs = System.currentTimeMillis(),
      heapUsed = nonNegative(heap.getUsed),
      heapMax = usableMax(heap),
      nonHeapUsed = nonNegative(nonHeap.getUsed),
      nonHeapMax = usableMax(nonHeap),
      totalGcCount = gcStats._1,
      totalGcTimeMs = gcStats._2,
      recentGcTimeRatio = 0.0d,
      lastGcName = "",
      lastGcCause = "",
      lastGcAction = "",
      lastGcDurationMs = 0L,
      lastGcHeapBefore = 0L,
      lastGcHeapAfter = 0L,
      lastGcReclaimedBytes = 0L)
  }

  private def latestSnapshot: Option[JvmPressureSnapshot] = {
    Option(latest.get()).orElse(JvmPressureStore.latestJvmPressure)
  }

  private def daemonThreadFactory(name: String): ThreadFactory = {
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, name)
        thread.setDaemon(true)
        thread
      }
    }
  }

  private def gauge[T](f: => T): Gauge[T] = new Gauge[T] {
    override def getValue: T = f
  }

  private def usableMax(usage: MemoryUsage): Long = {
    if (usage.getMax > 0L) usage.getMax else usage.getCommitted
  }

  private def localHostName(): String = {
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _: Exception => "unknown"
    }
  }

  private def nonNegative(value: Long): Long = math.max(0L, value)

  private def aggregateGcStats(beans: Seq[GarbageCollectorMXBean]): (Long, Long) = {
    val counts = beans.map(bean => nonNegative(bean.getCollectionCount)).sum
    val times = beans.map(bean => nonNegative(bean.getCollectionTime)).sum
    (counts, times)
  }
}
