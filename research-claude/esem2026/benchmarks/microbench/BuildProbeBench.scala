// Microbenchmark for cache-aware SHJ study.
// Generates two synthetic tables (build, probe), forces ShuffledHashJoin,
// and reports end-to-end time. Combine with perf-attach for cache metrics.
//
// Submit with one of: configs/spark-baseline.conf, spark-v1.conf, spark-v2.conf
//
// Spark-submit example:
//   spark-submit --class research.mascots.BuildProbeBench \
//     --properties-file configs/spark-baseline.conf \
//     research-claude-mascots2026.jar \
//     --buildRows=50000000 --probeRows=500000000 --keyCardinality=10000000 --runs=3

package research.mascots

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BuildProbeBench {

  case class Args(
    buildRows: Long = 10000000L,
    probeRows: Long = 100000000L,
    keyCardinality: Long = 1000000L,
    payloadBytes: Int = 32,
    runs: Int = 3,
    outPath: String = ""
  )

  def parse(argv: Array[String]): Args = {
    var a = Args()
    argv.foreach { s =>
      val kv = s.stripPrefix("--").split("=", 2)
      kv match {
        case Array("buildRows", v)      => a = a.copy(buildRows = v.toLong)
        case Array("probeRows", v)      => a = a.copy(probeRows = v.toLong)
        case Array("keyCardinality", v) => a = a.copy(keyCardinality = v.toLong)
        case Array("payloadBytes", v)   => a = a.copy(payloadBytes = v.toInt)
        case Array("runs", v)           => a = a.copy(runs = v.toInt)
        case Array("outPath", v)        => a = a.copy(outPath = v)
        case _                          => sys.error(s"unknown arg: $s")
      }
    }
    a
  }

  def main(argv: Array[String]): Unit = {
    val args = parse(argv)
    val spark = SparkSession.builder().appName("MascotsBuildProbeBench").getOrCreate()
    import spark.implicits._

    println(s"[bench] $args")

    val build = spark.range(args.buildRows)
      .select(
        (col("id") % args.keyCardinality).alias("k"),
        lit("x" * args.payloadBytes).alias("v_build")
      ).repartition(col("k")) // force shuffle so SHJ path is exercised
      .hint("SHUFFLE_HASH")
      .cache()
    build.count()

    val probe = spark.range(args.probeRows)
      .select(
        (col("id") % args.keyCardinality).alias("k"),
        lit("y" * args.payloadBytes).alias("v_probe")
      ).repartition(col("k"))
    probe.count() // warm

    val timings = (1 to args.runs).map { run =>
      val t0 = System.nanoTime()
      val out = probe.join(build.hint("SHUFFLE_HASH"), Seq("k"), "inner")
      val n = out.count()
      val ms = (System.nanoTime() - t0) / 1e6
      println(f"[bench] run=$run rows=$n ms=$ms%.1f")
      ms
    }

    val median = timings.sorted.apply(timings.size / 2)
    val mean = timings.sum / timings.size
    println(f"[bench] median_ms=$median%.1f mean_ms=$mean%.1f")

    if (args.outPath.nonEmpty) {
      import java.nio.file.{Files, Paths, StandardOpenOption}
      val line = s"${java.time.Instant.now}\t${args.buildRows}\t${args.probeRows}\t${args.keyCardinality}\t$median\t$mean\n"
      Files.write(
        Paths.get(args.outPath),
        line.getBytes,
        StandardOpenOption.CREATE, StandardOpenOption.APPEND
      )
    }

    spark.stop()
  }
}
