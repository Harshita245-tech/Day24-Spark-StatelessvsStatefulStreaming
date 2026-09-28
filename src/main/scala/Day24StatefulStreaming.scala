import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day24StatefulStreaming {

  def main(args: Array[String]): Unit = {

    // =====================================================
    // DAY 24 - STATELESS VS STATEFUL STREAMING
    // =====================================================

    val conf = new SparkConf()
      .setAppName("Day24StatefulStreaming")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")

    val ssc = new StreamingContext(conf, Seconds(5))

    // Hide Spark logs
    ssc.sparkContext.setLogLevel("OFF")

    // Checkpoint required for stateful processing
    ssc.checkpoint("/tmp/day24-checkpoint")

    // =====================================================
    // SOCKET INPUT
    // =====================================================

    val lines = ssc.socketTextStream("localhost", 9999)

    // Each input line is a bank account ID.
    // Example:
    // ACC001
    // ACC001
    // ACC002

    val accounts = lines
      .map(_.trim)
      .filter(_.nonEmpty)

    // =====================================================
    // STATELESS PROCESSING
    // Counts only transactions in the CURRENT batch.
    // =====================================================

    val statelessCounts = accounts
      .map(account => (account, 1))
      .reduceByKey(_ + _)

    statelessCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==========================================")
        println("          STATELESS PROCESSING")
        println("==========================================")
        println("Current batch transaction counts:")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, count) =>
              println(account + " -> " + count)
          }

        println("==========================================")
      }
    }

    // =====================================================
    // STATEFUL PROCESSING
    // Maintains running transaction count across batches.
    // =====================================================

    val statefulCounts = accounts
      .map(account => (account, 1))
      .updateStateByKey(
        (newValues: Seq[Int], previousState: Option[Int]) => {

          val previousCount = previousState.getOrElse(0)
          val currentBatchCount = newValues.sum

          Some(previousCount + currentBatchCount)
        }
      )

    statefulCounts.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==========================================")
        println("           STATEFUL PROCESSING")
        println("==========================================")
        println("Running transaction counts:")

        rdd.collect()
          .sortBy(_._1)
          .foreach {
            case (account, count) =>
              println(account + " -> " + count)
          }

        println("==========================================")
      }
    }

    // =====================================================
    // START STREAMING
    // =====================================================

    ssc.start()

    println()
    println("==========================================")
    println("       DAY 24 - STREAMING STARTED")
    println("==========================================")
    println("Batch Interval : 5 seconds")
    println("Socket         : localhost:9999")
    println("Status         : WAITING FOR DATA")
    println()
    println("Send account IDs using nc:")
    println("ACC001")
    println("ACC001")
    println("ACC002")
    println("==========================================")
    println()

    ssc.awaitTermination()
  }
}
