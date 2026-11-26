import java.time.Instant
import java.util.UUID
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object LiveCardJob {
  def run(): Unit = {
    val runId = sys.env("MOVIELENS_LIVE_RUN")
    require(runId.matches("[A-Za-z0-9_-]{1,80}"), "Invalid run ID")

    val broker = sys.env("MOVIELENS_LIVE_BROKER")
    val base = s"/movie-live/$runId"

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val schema = new StructType()
      .add("run_id", StringType)
      .add("event_seq", LongType)
      .add("user_id", LongType)
      .add("movie_id", LongType)
      .add("rating", IntegerType)
      .add("timestamp", LongType)
      .add("sent_at_ms", LongType)

    val events = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", broker)
      .option("subscribe", "movielens-live-ratings")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 10000)
      .option("failOnDataLoss", "true")
      .load()
      .select(from_json(col("value").cast("string"), schema).as("e"))
      .select("e.*")
      .filter(
        col("run_id") === lit(runId) &&
        col("event_seq") >= 0 &&
        col("user_id") > 0 &&
        col("movie_id") > 0 &&
        col("rating").between(1, 5) &&
        col("timestamp") > 0
      )

    def commit(batch: DataFrame, batchId: Long): Unit = {
      val target = new Path(
        base + "/batches/" + f"batch-$batchId%020d"
      )
      val fs = target.getFileSystem(
        spark.sparkContext.hadoopConfiguration
      )

      if (!fs.exists(target)) {
        val summary = batch.agg(
          count(lit(1)).as("rating_count"),
          avg("rating").as("avg_rating"),
          countDistinct("user_id").as("active_users"),
          avg(
            when(col("rating") <= 2, 1.0).otherwise(0.0)
          ).as("low_rating_ratio")
        ).persist()

        try {
          val total = summary.first().getAs[Long]("rating_count")
          if (total == 0) return

          val result = summary
            .withColumn("run_id", lit(runId))
            .withColumn("batch_id", lit(batchId))
            .withColumn("committed_at", lit(Instant.now().toString))

          fs.mkdirs(target.getParent)
          val staging = new Path(
            target.toString + ".staging-" + UUID.randomUUID()
          )

          result.coalesce(1).write.json(staging.toString)

          require(
            fs.rename(staging, target),
            "Failed to commit live batch"
          )
        } finally {
          summary.unpersist()
        }
      }

      // Retry reuses the saved result; notification can be repeated.
      val saved = spark.read.json(target.toString)

      saved.select(
        lit(runId).as("key"),
        to_json(
          struct(saved.columns.map(name => col(name)): _*)
        ).as("value")
      ).write
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("topic", "movielens-live-updates")
        .option("kafka.acks", "all")
        .save()

      println(
        s"LIVE_COMMITTED run=$runId batch=$batchId path=$target"
      )
    }

    val query = events.writeStream
      .option("checkpointLocation", base + "/checkpoint")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch((batch: DataFrame, batchId: Long) =>
        commit(batch, batchId)
      )
      .start()

    println(s"LIVE_STARTED run=$runId broker=$broker")
    query.awaitTermination()
  }
}

try {
  LiveCardJob.run()
} catch {
  case scala.util.control.NonFatal(error) =>
    error.printStackTrace()
    spark.stop()
    System.exit(1)
}
