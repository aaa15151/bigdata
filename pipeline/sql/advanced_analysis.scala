import org.apache.hadoop.fs.{Path, FileContext, Options}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.control.NonFatal

def runAdvanced(): Unit = {
  val session = SparkSession.builder().enableHiveSupport().getOrCreate()
  import session.implicits._
  val base = sys.env.getOrElse("MOVIELENS_HDFS_BASE", "hdfs://namenode:9000/movie")
  val target = new Path(base + "/result/sql")
  val fs = target.getFileSystem(sc.hadoopConfiguration)
  require(!fs.exists(target), "Output exists: " + target)
  val stage = new Path(target.getParent, ".sql-" + java.util.UUID.randomUUID())
  val tables = Seq("ods_ratings_part", "dws_user_profile_cube", "ads_age_genre_pivot", "ads_genre_top10", "ads_daily_moving_avg")
  tables.foreach(t => require(!session.catalog.tableExists("movielens." + t), "Table already exists: " + t))
  session.conf.set("spark.sql.session.timeZone", "UTC")
  session.conf.set("spark.sql.shuffle.partitions", "8")
  session.sql("SET hive.exec.dynamic.partition=true")
  session.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
  val ages = Map(1 -> "18岁以下",18 -> "18–24岁",25 -> "25–34岁",35 -> "35–44岁",45 -> "45–49岁",50 -> "50–55岁",56 -> "56岁及以上")
  val occupations = Seq("其他","学术/教育","艺术家","文书/行政","大学生/研究生","客户服务","医生/医疗","管理人员","农民","家庭主妇/主夫","中小学生","律师","程序员","退休人员","销售/营销","科学家","自由职业","技术/工程","技工","无业","作家")
  session.udf.register("age_label", (n: Int) => ages.getOrElse(n,"未知"))
  session.udf.register("occupation_label", (n: Int) => occupations.lift(n).getOrElse("未知"))
  try {
    val ratings = session.table("movielens.ods_ratings")
    val count = ratings.count()
    require(count == 1000209L, "Unexpected input count: " + count)
    val partitioned = ratings.withColumn("event_date",to_date(from_unixtime(col("timestamp"))))
      .withColumn("year",year(col("event_date"))).withColumn("month",month(col("event_date"))).drop("event_date")
    partitioned.write.partitionBy("year","month").parquet(new Path(stage,"ratings_part").toString)
    val joined = session.sql("""SELECT /*+ BROADCAST(u, m) */ r.*, u.gender, u.age_group,
      age_label(u.age_group) AS age_label, u.occupation, occupation_label(u.occupation) AS occupation_label,
      m.title, m.genres FROM movielens.ods_ratings r
      JOIN movielens.dim_users u ON r.user_id=u.user_id
      JOIN movielens.dim_movies m ON r.movie_id=m.movie_id""")
    joined.createOrReplaceTempView("m2_joined")
    session.sql("SELECT *, explode(split(genres, '[|]')) AS genre FROM m2_joined").createOrReplaceTempView("m2_expanded")
    // Genre detail counts are multi-label. Genre-ALL subtotals use unexpanded ratings.
    val detail = session.sql("""SELECT gender, age_group, genre, COUNT(*) AS rating_count,
      AVG(rating) AS avg_rating, CAST(grouping_id() AS BIGINT) AS grouping_id
      FROM m2_expanded GROUP BY CUBE(gender, age_group, genre) HAVING grouping(genre)=0""")
    val totals = session.sql("""SELECT gender, age_group, CAST(NULL AS STRING) AS genre,
      COUNT(*) AS rating_count, AVG(rating) AS avg_rating,
      CAST(grouping_id()*2+1 AS BIGINT) AS grouping_id
      FROM m2_joined GROUP BY CUBE(gender, age_group)""")
    val cube = detail.unionByName(totals)
    require(cube.filter("grouping_id=7").select("rating_count").as[Long].head() == count,"Cube total mismatch")
    def save(df: DataFrame, name: String): Unit = df.write.parquet(new Path(stage,name).toString)
    save(cube,"user_profile_cube")
    val genres = Seq("Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western")
    save(session.table("m2_expanded").groupBy("age_group","age_label").pivot("genre",genres).count().na.fill(0L),"age_genre_pivot")
    save(session.sql("""SELECT * FROM (
      SELECT movie_id,title,genre,rating_count,avg_rating,
      ROW_NUMBER() OVER(PARTITION BY genre ORDER BY avg_rating DESC,rating_count DESC,movie_id) AS rank
      FROM (SELECT movie_id,title,genre,COUNT(*) AS rating_count,AVG(rating) AS avg_rating
      FROM m2_expanded GROUP BY movie_id,title,genre HAVING COUNT(*)>=50) a
      ) b WHERE rank<=10"""),"genre_top10")
    session.sql("""SELECT to_date(from_unixtime(timestamp)) AS rating_date, COUNT(*) AS rating_count
      FROM movielens.ods_ratings GROUP BY to_date(from_unixtime(timestamp))""").createOrReplaceTempView("m2_daily")
    session.sql("""SELECT explode(sequence(min(rating_date),max(rating_date),interval 1 day)) AS rating_date
      FROM m2_daily""").createOrReplaceTempView("m2_dates")
    save(session.sql("""SELECT rating_date,rating_count,moving_avg_7d,previous_count,
      CASE WHEN previous_count>0 THEN (rating_count-previous_count)*1.0/previous_count END AS change_ratio
      FROM (SELECT d.rating_date,coalesce(r.rating_count,0) AS rating_count,
      AVG(coalesce(r.rating_count,0)) OVER(ORDER BY d.rating_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d,
      LAG(coalesce(r.rating_count,0)) OVER(ORDER BY d.rating_date) AS previous_count
      FROM m2_dates d LEFT JOIN m2_daily r ON d.rating_date=r.rating_date) x"""),"daily_moving_avg")
    save(session.sql("""SELECT occupation,occupation_label,COUNT(*) AS rating_count,
      COUNT(DISTINCT user_id) AS user_count,AVG(rating) AS avg_rating
      FROM m2_joined GROUP BY occupation,occupation_label"""),"occupation_profile")
    val plans = Seq("BROADCAST_JOIN\n" + joined.queryExecution.executedPlan.toString(),
      "MERGE_JOIN_COMPARISON\n" + session.sql("""SELECT /*+ MERGE(r,u) */ r.user_id,u.gender FROM movielens.ods_ratings r
      JOIN movielens.dim_users u ON r.user_id=u.user_id""").queryExecution.executedPlan.toString())
    session.createDataset(plans).coalesce(1).write.text(new Path(stage,"join_plans").toString)
    FileContext.getFileContext(target.toUri,sc.hadoopConfiguration).rename(stage,target,Options.Rename.NONE)
    // Publish metadata only after all filesystem outputs are complete.
    session.sql(s"""CREATE EXTERNAL TABLE movielens.ods_ratings_part
      (user_id INT,movie_id INT,rating DOUBLE,`timestamp` BIGINT)
      PARTITIONED BY (year INT,month INT) STORED AS PARQUET LOCATION '$target/ratings_part'""")
    session.sql("MSCK REPAIR TABLE movielens.ods_ratings_part")
    Seq("dws_user_profile_cube" -> "user_profile_cube","ads_age_genre_pivot" -> "age_genre_pivot",
      "ads_genre_top10" -> "genre_top10","ads_daily_moving_avg" -> "daily_moving_avg").foreach { case (table,path) =>
      val schema = session.read.parquet(s"$target/$path").schema.fields
        .map(f => "`" + f.name + "` " + f.dataType.sql).mkString(",")
      session.sql(s"CREATE EXTERNAL TABLE movielens.$table ($schema) STORED AS PARQUET LOCATION '$target/$path'")
    }
    require(session.table("movielens.ods_ratings_part").count() == count,"Partition count mismatch")
    session.sql("SHOW PARTITIONS movielens.ods_ratings_part").show(100,false)
    val pruned = session.sql("EXPLAIN FORMATTED SELECT * FROM movielens.ods_ratings_part WHERE year=2000 AND month=11")
    val full = session.sql("EXPLAIN FORMATTED SELECT * FROM movielens.ods_ratings_part")
    full.union(pruned).coalesce(1).write.text(s"$target/partition_plans")
    session.sql("SELECT * FROM movielens.ads_genre_top10 WHERE genre='Sci-Fi' ORDER BY rank").show(10,false)
    println(s"SQL_ADVANCED_OK ratings=$count path=$target")
  } finally {
    if(fs.exists(stage)) fs.delete(stage,true)
  }
}
val m2Exit = try { sc.setLogLevel("WARN"); runAdvanced(); 0 } catch {
  case NonFatal(e) => e.printStackTrace(); 1
} finally { spark.stop() }
sys.exit(m2Exit)
