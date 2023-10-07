import org.apache.spark.sql.SparkSession


object PageRank {

  // Do not modify
  val PageRankIterations = 10

  /**
    * Input graph is a plain text file of the following format:
    *
    *   follower  followee
    *   follower  followee
    *   follower  followee
    *   ...
    *
    * where the follower and followee are separated by `\t`.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `outputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * where node and rank are separated by `\t`.
    *
    * @param inputGraphPath path of the input graph.
    * @param outputPath path of the output of page rank.
    * @param iterations number of iterations to run on the PageRank.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      outputPath: String,
      iterations: Int,
      spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    val iters = iterations
    val lines = spark.read.textFile(inputGraphPath).rdd.cache()
    val rdd1 = lines.flatMap(s => s.split("\t"))
    val vertice = rdd1.distinct().map(x=>x.toInt)
    val vertices=vertice.count()
    var ranks = vertice.map(v =>(v,1.0 / vertices))
    val links = lines.map { s =>
      val parts = s.split("\t")
      (parts(0).toInt, parts(1).toInt)
    }.groupByKey().cache()
    for (i <- 1 to 10){
      val contribs = links.join(ranks,links.partitioner.get).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))

      }.cache()
      val danglingcontribs =( 1.0- (contribs.map(v => v._2).sum())) / vertices
      val finalcontrib = vertice.map(v =>(v,danglingcontribs))
      val res=  contribs.union(finalcontrib)

      ranks = res.reduceByKey(_ + _).mapValues(0.15 / vertices + 0.85 * _)

    }
    val output = ranks.coalesce(1)
    output.map(tuple => s"${tuple._1}\t${tuple._2}").saveAsTextFile(outputPath)

   // output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
  }
  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val pageRankOutputPath = args(1)

    calculatePageRank(inputGraph, pageRankOutputPath, PageRankIterations, spark)
  }
}
