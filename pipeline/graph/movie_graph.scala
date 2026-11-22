import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{Path, FileContext, Options}
import scala.util.control.NonFatal

def runMovieGraph(): Unit = {
  import spark.implicits._
  val base = sys.env.getOrElse("MOVIELENS_HDFS_BASE","hdfs://namenode:9000/movie")
  val outputName = sys.env.getOrElse("MOVIELENS_GRAPH_OUTPUT", "graph_v3")
  require(outputName.matches("graph[_a-zA-Z0-9]*"), "Invalid graph output name")
  val target = new Path(base + "/result/" + outputName)
  val fs = target.getFileSystem(sc.hadoopConfiguration)
  require(!fs.exists(target), "Output already exists: " + target)
  val stage = new Path(target.getParent,".graph-" + java.util.UUID.randomUUID())
  try {
    // n(A) matches M1: distinct users who rated A >= 4, before the per-user cap.
    val likes = spark.read.parquet(base + "/clean/ratings").filter(col("rating") >= 4)
      .groupBy("movie_id").agg(countDistinct("user_id").as("like_count"))
    val pairs = spark.read.parquet(base + "/result/rdd/cooccurrence").filter(col("co_count") >= 30)
    val left = likes.select(col("movie_id"),col("like_count").as("n_a"))
    val right = likes.select(col("movie_id").as("other_movie_id"),col("like_count").as("n_b"))
    val scored = pairs.join(left,Seq("movie_id")).join(right,Seq("other_movie_id"))
      .withColumn("similarity",col("co_count") / sqrt(col("n_a").cast("double") * col("n_b")))
      .filter(col("similarity") >= 0.15)
    val directed = scored.select(col("movie_id").as("src"),col("other_movie_id").as("dst"),col("co_count"),col("similarity"))
      .union(scored.select(col("other_movie_id").as("src"),col("movie_id").as("dst"),col("co_count"),col("similarity")))
    val nearest = directed.withColumn("neighbor_rank",row_number().over(
      org.apache.spark.sql.expressions.Window.partitionBy("src").orderBy(desc("similarity"),desc("co_count"),asc("dst"))))
      .filter(col("neighbor_rank") <= 10)
    // Union kNN: keep an edge when either endpoint selects the other.
    val selected = nearest.select(
      least(col("src"),col("dst")).as("movie_id"),
      greatest(col("src"),col("dst")).as("other_movie_id"),col("co_count"))
      .dropDuplicates("movie_id","other_movie_id")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val pairCount = selected.count()
    require(pairCount <= 20000, "Graph exceeds edge budget")
    require(pairCount > 0,"No edges meet the threshold")
    val edges = selected.select("movie_id","other_movie_id","co_count").as[(Int,Int,Long)].rdd
      .flatMap { case (a,b,n) => Seq(Edge(a.toLong,b.toLong,n),Edge(b.toLong,a.toLong,n)) }
    val means = spark.read.parquet(base + "/clean/ratings").groupBy("movie_id").agg(avg("rating").as("mean_rating"))
    val metadata = spark.read.parquet(base + "/clean/movies").join(means,Seq("movie_id"))
      .select("movie_id","title","genres","mean_rating").as[(Int,String,String,Double)].rdd
      .map { case (id,title,genres,mean) => (id.toLong,(title,genres,mean)) }
    val initial = Graph(metadata,edges,("Unknown","",0.0)).partitionBy(PartitionStrategy.EdgePartition2D,8)
    val strong = initial.subgraph(epred = edge => edge.attr >= 30)
    val graph = strong.outerJoinVertices(strong.degrees)((_,attr,degree)=>(attr,degree.getOrElse(0)))
      .subgraph(vpred=(_,attr)=>attr._2>0).mapVertices((_,attr)=>attr._1).cache()
    val vertexCount = graph.vertices.count()
    val edgeCount = graph.edges.count()
    val ranks = graph.staticPageRank(20).vertices
    val labels = LabelPropagation.run(graph,10).vertices
    val components = graph.connectedComponents().vertices
    // Edges are reciprocal. outDegrees counts unique neighbors, not twice the degree.
    val degrees = graph.outDegrees
    val neighborMean = graph.aggregateMessages[(Double,Long)](
      ctx => ctx.sendToSrc((ctx.dstAttr._3,1L)),
      (a,b)=>(a._1+b._1,a._2+b._2)).mapValues(x=>x._1/x._2)
    val rankRows = graph.vertices.join(ranks).join(degrees).join(neighborMean).map {
      case (id,(((attr,rank),degree),neighbor)) => (id,attr._1,attr._2,rank,degree,neighbor)
    }.toDF("movie_id","title","genres","pagerank","degree","neighbor_avg_rating").persist(StorageLevel.MEMORY_AND_DISK)
    require(rankRows.count()==vertexCount,"PageRank coverage mismatch")
    val communityRows = labels.map { case (id,label)=>(id,label) }.toDF("movie_id","community_id")
    val sizes = communityRows.groupBy("community_id").agg(count(lit(1)).as("movie_count"))
    val genreCounts = communityRows.join(rankRows.select("movie_id","genres"),Seq("movie_id"))
      .withColumn("genre",explode(split(col("genres"),"[|]")))
      .groupBy("community_id","genre").count()
    val summary = sizes.join(genreCounts.groupBy("community_id")
      .agg(sort_array(collect_list(struct(col("count"),col("genre"))),false).as("genre_counts")),Seq("community_id"))
    val top = rankRows.orderBy(desc("pagerank"),asc("movie_id")).limit(100)
      .join(communityRows,Seq("movie_id")).persist(StorageLevel.MEMORY_AND_DISK)
    val topIds = top.select("movie_id").as[Long].collect().toSet
    val topBroadcast = sc.broadcast(topIds)
    val smallEdges = selected.as[(Int,Int,Long)].rdd.filter { case (a,b,_) => topBroadcast.value(a.toLong) && topBroadcast.value(b.toLong) }
      .map { case (a,b,n)=>(a.toLong,b.toLong,n) }.toDF("source","target","co_count")
    rankRows.write.parquet(new Path(stage,"pagerank").toString)
    communityRows.write.parquet(new Path(stage,"communities").toString)
    summary.write.json(new Path(stage,"community_summary").toString)
    components.map { case (id,component)=>(id,component) }.toDF("movie_id","component_id")
      .write.parquet(new Path(stage,"components").toString)
    top.coalesce(1).write.json(new Path(stage,"edges_top/nodes").toString)
    smallEdges.coalesce(1).write.json(new Path(stage,"edges_top/edges").toString)
    val communityCount = sizes.count()
    val componentCount = components.map(_._2).distinct().count()
    Seq((vertexCount,pairCount,edgeCount,communityCount,componentCount,30,10,20,10))
      .toDF("vertices","undirected_pairs","directed_edges","communities","components","min_co_count","union_top_k","pagerank_iterations","label_iterations")
      .coalesce(1).write.json(new Path(stage,"metrics").toString)
    FileContext.getFileContext(target.toUri,sc.hadoopConfiguration).rename(stage,target,Options.Rename.NONE)
    rankRows.orderBy(desc("pagerank"),asc("movie_id")).show(20,false)
    summary.orderBy(desc("movie_count")).show(30,false)
    println(s"GRAPH_OK vertices=$vertexCount pairs=$pairCount communities=$communityCount components=$componentCount nodes=${topIds.size} path=$target")
  } finally {
    if(fs.exists(stage)) fs.delete(stage,true)
  }
}
val graphExit = try { sc.setLogLevel("WARN"); runMovieGraph(); 0 } catch {
  case NonFatal(e) => e.printStackTrace(); 1
} finally { spark.stop() }
sys.exit(graphExit)
