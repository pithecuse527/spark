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
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import javax.management.{Notification, NotificationEmitter, NotificationListener}
import javax.management.openmbean.CompositeData

import scala.collection.JavaConverters._

import com.codahale.metrics.Gauge
import com.sun.management.GarbageCollectionNotificationInfo

import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.internal.Logging

class JvmAqeExecutorPlugin extends ExecutorPlugin with Logging {
  private case class GcEvent(timestampMs: Long, durationMs: Long)

  private val memoryMxBean = ManagementFactory.getMemoryMXBean
  private val gcBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala.toSeq
  private val recentGcEvents = new ConcurrentLinkedQueue[GcEvent]()
  private val lastGcSnapshot = new AtomicReference[LastGc](LastGc.Empty)
  private val lastSentHeapUsed = new AtomicLong(0L)
  private val lastSentHeapMax = new AtomicLong(0L)
  private val lastSentNonHeapUsed = new AtomicLong(0L)
  private val lastSentRecentGcRatioBits =
    new AtomicLong(java.lang.Double.doubleToRawLongBits(0.0d))

  @volatile private var pluginContext: PluginContext = _
  @volatile private var executorId: String = _
  @volatile private var host: String = _
  @volatile private var recentGcWindowMs: Long = JvmAqeConf.DefaultRecentGcWindowMs
  @volatile private var scheduler: ScheduledExecutorService = _
  @volatile private var registeredListeners: Seq[(NotificationEmitter, NotificationListener)] =
    Seq.empty

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    pluginContext = ctx
    executorId = ctx.executorID()
    host = localHostName()
    recentGcWindowMs = JvmAqeConf.recentGcWindowMs(ctx.conf())
    val intervalMs = JvmAqeConf.executorSampleIntervalMs(ctx.conf())

    registerMetrics(ctx)
    registerGcNotificationListeners()
    scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreadFactory(
      s"jvm-aqe-executor-sampler-$executorId"))
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = sampleAndSend()
      },
      intervalMs,
      intervalMs,
      TimeUnit.MILLISECONDS)

    logInfo(
      s"[JVM-AQE] Executor plugin initialized executor=$executorId host=$host " +
        s"intervalMs=$intervalMs recentGcWindowMs=$recentGcWindowMs")
  }

  override def shutdown(): Unit = {
    val localScheduler = scheduler
    if (localScheduler != null) {
      localScheduler.shutdownNow()
    }
    registeredListeners.foreach { case (emitter, listener) =>
      try {
        emitter.removeNotificationListener(listener)
      } catch {
        case e: Exception =>
          logDebug(s"[JVM-AQE] Failed to remove GC listener: ${e.getMessage}")
      }
    }
    registeredListeners = Seq.empty
    logInfo(s"[JVM-AQE] Executor plugin stopped executor=$executorId")
  }

  private def registerMetrics(ctx: PluginContext): Unit = {
    val registry = ctx.metricRegistry()
    registry.register("heapUsed", gauge(lastSentHeapUsed.get()))
    registry.register("heapMax", gauge(lastSentHeapMax.get()))
    registry.register("nonHeapUsed", gauge(lastSentNonHeapUsed.get()))
    registry.register("recentGcTimeRatio", gauge(
      java.lang.Double.longBitsToDouble(lastSentRecentGcRatioBits.get())))
  }

  private def registerGcNotificationListeners(): Unit = {
    val builders = scala.collection.mutable.ArrayBuffer.empty[
      (NotificationEmitter, NotificationListener)]
    gcBeans.foreach {
      case emitter: NotificationEmitter =>
        val listener = new NotificationListener {
          override def handleNotification(notification: Notification, handback: Any): Unit = {
            if (GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION ==
                notification.getType) {
              handleGcNotification(notification)
            }
          }
        }
        emitter.addNotificationListener(listener, null, null)
        builders += ((emitter, listener))
      case bean =>
        logDebug(s"[JVM-AQE] GC bean ${bean.getName} does not emit notifications")
    }
    registeredListeners = builders.toSeq
  }

  private def handleGcNotification(notification: Notification): Unit = {
    try {
      val info = GarbageCollectionNotificationInfo.from(
        notification.getUserData.asInstanceOf[CompositeData])
      val gcInfo = info.getGcInfo
      val before = gcInfo.getMemoryUsageBeforeGc
      val after = gcInfo.getMemoryUsageAfterGc
      val heapBefore = heapUsageFromPools(before)
      val heapAfter = heapUsageFromPools(after)
      val durationMs = math.max(0L, gcInfo.getDuration)
      val now = System.currentTimeMillis()

      recentGcEvents.add(GcEvent(now, durationMs))
      pruneRecentGcEvents(now)
      lastGcSnapshot.set(LastGc(
        name = nullToEmpty(info.getGcName),
        cause = nullToEmpty(info.getGcCause),
        action = nullToEmpty(info.getGcAction),
        durationMs = durationMs,
        heapBefore = heapBefore,
        heapAfter = heapAfter,
        reclaimedBytes = math.max(0L, heapBefore - heapAfter)))
    } catch {
      case e: Exception =>
        logWarning(s"[JVM-AQE] Failed to process GC notification: ${e.getMessage}")
    }
  }

  private def sampleAndSend(): Unit = {
    try {
      val snapshot = currentSnapshot()
      lastSentHeapUsed.set(snapshot.heapUsed)
      lastSentHeapMax.set(snapshot.heapMax)
      lastSentNonHeapUsed.set(snapshot.nonHeapUsed)
      lastSentRecentGcRatioBits.set(
        java.lang.Double.doubleToRawLongBits(snapshot.recentGcTimeRatio))
      pluginContext.send(snapshot)
    } catch {
      case e: Exception =>
        logWarning(s"[JVM-AQE] Failed to send JVM pressure snapshot: ${e.getMessage}")
    }
  }

  private def currentSnapshot(): JvmPressureSnapshot = {
    val heap = memoryMxBean.getHeapMemoryUsage
    val nonHeap = memoryMxBean.getNonHeapMemoryUsage
    val gcStats = aggregateGcStats(gcBeans)
    val last = lastGcSnapshot.get()
    JvmPressureSnapshot(
      executorId = executorId,
      host = host,
      timestampMs = System.currentTimeMillis(),
      heapUsed = nonNegative(heap.getUsed),
      heapMax = usableMax(heap),
      nonHeapUsed = nonNegative(nonHeap.getUsed),
      nonHeapMax = usableMax(nonHeap),
      totalGcCount = gcStats._1,
      totalGcTimeMs = gcStats._2,
      recentGcTimeRatio = recentGcTimeRatio(System.currentTimeMillis()),
      lastGcName = last.name,
      lastGcCause = last.cause,
      lastGcAction = last.action,
      lastGcDurationMs = last.durationMs,
      lastGcHeapBefore = last.heapBefore,
      lastGcHeapAfter = last.heapAfter,
      lastGcReclaimedBytes = last.reclaimedBytes)
  }

  private def recentGcTimeRatio(nowMs: Long): Double = {
    pruneRecentGcEvents(nowMs)
    val totalDurationMs = recentGcEvents.asScala.map(_.durationMs).sum
    math.max(0.0d, math.min(1.0d, totalDurationMs.toDouble / recentGcWindowMs.toDouble))
  }

  private def pruneRecentGcEvents(nowMs: Long): Unit = {
    var head = recentGcEvents.peek()
    while (head != null && nowMs - head.timestampMs > recentGcWindowMs) {
      recentGcEvents.poll()
      head = recentGcEvents.peek()
    }
  }

  private def heapUsageFromPools(usages: util.Map[String, MemoryUsage]): Long = {
    usages.asScala.iterator.collect {
      case (name, usage) if isHeapPoolName(name) => nonNegative(usage.getUsed)
    }.sum
  }

  private def isHeapPoolName(name: String): Boolean = {
    !name.contains("Metaspace") &&
      !name.contains("Compressed Class") &&
      !name.contains("CodeHeap")
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

  private def nullToEmpty(value: String): String = if (value == null) "" else value

  private def localHostName(): String = {
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _: Exception => "unknown"
    }
  }

  private def usableMax(usage: MemoryUsage): Long = {
    if (usage.getMax > 0L) usage.getMax else usage.getCommitted
  }

  private def nonNegative(value: Long): Long = math.max(0L, value)

  private def aggregateGcStats(beans: Seq[GarbageCollectorMXBean]): (Long, Long) = {
    val counts = beans.map(bean => nonNegative(bean.getCollectionCount)).sum
    val times = beans.map(bean => nonNegative(bean.getCollectionTime)).sum
    (counts, times)
  }
}

private[jvmaqe] case class LastGc(
    name: String,
    cause: String,
    action: String,
    durationMs: Long,
    heapBefore: Long,
    heapAfter: Long,
    reclaimedBytes: Long)

private[jvmaqe] object LastGc {
  val Empty: LastGc = LastGc("", "", "", 0L, 0L, 0L, 0L)
}
