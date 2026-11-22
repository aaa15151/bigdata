import org.apache.hadoop.fs.{Path, FileContext, Options}
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.util.Try
import scala.util.control.NonFatal

def runSimilarity(): Unit = {
  import spark.implicits._
  val base = sys.env.getOrElse("MOVIELENS_HDFS_BASE", "hdfs://namenode:9000/movie")
  val target = new Path(base + "/result/rdd")
  val fs = target.getFileSystem(sc.hadoopConfiguration)
  require(!fs.exists(target), "Output already exists: " + target)
  val stage = new Path(target.getParent, ".rdd-" + java.util.UUID.randomUUID())
  val total = sc.longAccumulator("total_lines")
  val malformed = sc.longAccumulator("malformed")
  val bounds = sc.longAccumulator("out_of_range")
  val valid = sc.longAccumulator("valid_lines")
  val parsed = sc.textFile(base + "/raw/ratings.dat", 8).flatMap { line =>
    total.add(1)
    val p = line.split("::", -1)
    val row = if (p.length != 4) None else Try((p(0).toInt,p(1).toInt,p(2).toDouble,p(3).toLong)).toOption
    row match {
      case None => malformed.add(1); None
      case Some((u,m,r,t)) if u <= 0 || m <= 0 || r.isNaN || r < 1 || r > 5 || t <= 0 => bounds.add(1); None
      case Some(x) => valid.add(1); Some(x)
    }
  }.persist(StorageLevel.MEMORY_AND_DISK)
  try {
    val validCount = parsed.count()
    // Capture accumulator values after one materialization. Retries may replay updates;
    // fail validation rather than publish an inflated quality report.
    require(valid.value == validCount && total.value == valid.value + malformed.value + bounds.value, "Accumulator mismatch; rerun job")
    val quality = (total.value, valid.value, malformed.value, bounds.value)
    require(validCount == spark.read.parquet(base + "/clean/ratings").count(), "Raw/clean counts differ")
    val likes = parsed.filter(_._3 >= 4).map(x => ((x._1,x._2),x._4))
      .reduceByKey((a: Long,b: Long) => math.max(a,b)).persist(StorageLevel.MEMORY_AND_DISK)
    val counts = likes.map { case ((u,m),t) => (m,1L) }.reduceByKey(_ + _).filter(_._2 >= 20).collectAsMap().toMap
    val countBroadcast = sc.broadcast(counts)
    val movieBroadcast = sc.broadcast(spark.read.parquet(base + "/clean/movies")
      .select("movie_id","title","genres").as[(Int,String,String)].collect().map(x => x._1 -> (x._2,x._3)).toMap)
    // Keep bounded buffers during both map-side and reduce-side combining.
    def recent(xs: Vector[(Int,Long)]): Vector[(Int,Long)] = xs.sortBy(x => (-x._2,x._1)).take(200)
    val histories = likes.filter(x => countBroadcast.value.contains(x._1._2))
      .map { case ((u,m),t) => (u,(m,t)) }.combineByKey[Vector[(Int,Long)]](
        (x: (Int,Long)) => Vector(x),
        (a: Vector[(Int,Long)],x: (Int,Long)) => recent(a :+ x),
        (a: Vector[(Int,Long)],b: Vector[(Int,Long)]) => recent(a ++ b))
    val pairs = histories.flatMap { case (_,xs) =>
      val ids = xs.map(_._1).sorted
      ids.indices.iterator.flatMap(i => (i+1 until ids.size).iterator.map(j => ((ids(i),ids(j)),1L)))
    }.reduceByKey(_ + _, 16).filter(_._2 >= 5).persist(StorageLevel.MEMORY_AND_DISK)
    val pairCount = pairs.count()
    require(pairCount > 0, "No movie pairs")
    pairs.map { case ((a,b),n) => (a,b,n) }.toDF("movie_id","other_movie_id","co_count")
      .write.parquet(new Path(stage,"cooccurrence").toString)
    // partitionBy shuffles once by source movie; broadcast joins avoid metadata shuffles.
    val directed = pairs.flatMap { case ((a,b),n) => Seq((a,(b,n)),(b,(a,n))) }
      .partitionBy(new HashPartitioner(16))
    val scores = directed.map { case (a,(b,n)) =>
      val sim = n.toDouble / math.sqrt(countBroadcast.value(a).toDouble * countBroadcast.value(b))
      val metadata = movieBroadcast.value.getOrElse(b,("Unknown",""))
      (a,b,n,sim,metadata._1,metadata._2)
    }.toDF("movie_id","similar_movie_id","co_count","similarity","title","genres")
    val top = scores.withColumn("rank",row_number().over(Window.partitionBy("movie_id")
      .orderBy(desc("similarity"),desc("co_count"),asc("similar_movie_id"))))
      .filter(col("rank") <= 10).persist(StorageLevel.MEMORY_AND_DISK)
    val rows = top.count()
    require(top.filter(col("similarity") < 0 || col("similarity") > 1).count() == 0, "Invalid similarity")
    top.write.parquet(new Path(stage,"movie_similarity").toString)
    Seq(quality).toDF("total_lines","valid_lines","malformed_lines","out_of_range_lines")
      .coalesce(1).write.json(new Path(stage,"data_quality").toString)
    FileContext.getFileContext(target.toUri,sc.hadoopConfiguration).rename(stage,target,Options.Rename.NONE)
    println(s"RDD_OK valid=$validCount pairs=$pairCount similarities=$rows path=$target")
  } finally {
    parsed.unpersist()
    if (fs.exists(stage)) fs.delete(stage,true)
  }
}
val result = try { sc.setLogLevel("WARN"); runSimilarity(); 0 } catch {
  case NonFatal(e) => e.printStackTrace(); 1
} finally { spark.stop() }
sys.exit(result)
