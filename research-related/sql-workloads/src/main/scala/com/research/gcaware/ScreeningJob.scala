package com.research.gcaware

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * Cluster-mode GC-screening job, the spark-submit counterpart of the PySpark
 * `screening.py` / `tpc_pyspark.py` logic. Executor sizing, AQE, broadcast
 * thresholds and GC flags are supplied by `run-screening.sh` via `--conf`; this
 * job only registers the benchmark tables, runs one named query, times it, and
 * prints a machine-parseable line.
 *
 * Args (positional, from the shell script): <query> <scale> <dataLocation>
 *   query        e.g. "q9" -> bundled resource /<benchmark>/q9.sql
 *   scale        scale factor (informational; dataLocation already encodes it)
 *   dataLocation parquet root, e.g. s3a://spark-obj-storage/tpcds-scale-100
 */
object ScreeningJob {

  // Parquet table directory names (ports tpc_pyspark.TPCDS_TABLES / TPCH_TABLES).
  private val TPCDS_TABLES = Seq(
    "call_center", "catalog_page", "catalog_returns", "catalog_sales",
    "customer", "customer_address", "customer_demographics", "date_dim",
    "household_demographics", "income_band", "inventory", "item", "promotion",
    "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim",
    "warehouse", "web_page", "web_returns", "web_sales", "web_site")

  private val TPCH_TABLES = Seq(
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region",
    "supplier")

  private def tableNames(benchmark: String): Seq[String] = benchmark.toLowerCase match {
    case "tpcds" => TPCDS_TABLES
    case "tpch"  => TPCH_TABLES
    case other   => throw new IllegalArgumentException(s"benchmark must be tpcds or tpch, got '$other'")
  }

  /** Load a bundled query, e.g. ("tpcds", "q9") -> resource /tpcds/q9.sql. */
  private def loadSql(benchmark: String, query: String): String = {
    val name = if (query.endsWith(".sql")) query else s"$query.sql"
    val path = s"/${benchmark.toLowerCase}/$name"
    val in = Option(getClass.getResourceAsStream(path)).getOrElse(
      throw new IllegalArgumentException(s"Query resource not found on classpath: $path"))
    try Source.fromInputStream(in, "UTF-8").mkString finally in.close()
  }

  def run(benchmark: String, args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: <query> <scale> <dataLocation>")
      System.exit(2)
    }
    val query = args(0)
    val scale = args(1)
    val dataLocation = args(2).stripSuffix("/")
    val runId = s"screen_${benchmark}_${query}"

    // Inherit all confs set by spark-submit (sizing, AQE, thresholds, GC, S3A).
    // Respect the app name passed via --name / --conf spark.app.name (run-screening.sh
    // sets it to the informative {bench}-{query}-{sf}-{gc}-{aqe}-{heap}-{cores} form);
    // fall back to runId only when no name was supplied.
    val builder = SparkSession.builder()
    if (!new SparkConf().contains("spark.app.name")) builder.appName(runId)
    val spark = builder.getOrCreate()

    val line = "=" * 46
    var rows = -1L
    var durationMs = 0.0
    var exitCode = 0
    try {
      tableNames(benchmark).foreach { t =>
        spark.read.parquet(s"$dataLocation/$t").createOrReplaceTempView(t)
      }
      val sql = loadSql(benchmark, query)

      println(line)
      println(s"GC Screening (cluster): $query ($benchmark SF$scale)")
      println(line)
      println(s"  Data Location: $dataLocation")
      println(s"  Spark Version: ${spark.version}")
      println(s"  App ID:        ${spark.sparkContext.applicationId}")
      println(line)

      val df = spark.sql(sql)
      println(s"--- Logical Plan ($query) ---")
      println(df.queryExecution.analyzed.toString)

      val start = System.nanoTime()
      rows = df.count()
      durationMs = (System.nanoTime() - start) / 1e6

      println(s"--- Physical Plan after AQE ($query) ---")
      println(df.queryExecution.executedPlan.toString)
      println(s"Query $query completed: rows=$rows, duration=${"%.2f".format(durationMs)} ms")
    } catch {
      case e: Throwable =>
        exitCode = 1
        System.err.println(s"ERROR running $query: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      val appId = spark.sparkContext.applicationId
      // Machine-parseable (the shell script emits the full RESULT: line).
      println(s"QUERY_RESULT:$benchmark:$query:$rows:${"%.2f".format(durationMs)}:$exitCode:$appId")
      spark.stop()
    }
    if (exitCode != 0) System.exit(exitCode)
  }
}

object TpcdsQueryRunner {
  def main(args: Array[String]): Unit = ScreeningJob.run("tpcds", args)
}

object TpchQueryRunner {
  def main(args: Array[String]): Unit = ScreeningJob.run("tpch", args)
}
