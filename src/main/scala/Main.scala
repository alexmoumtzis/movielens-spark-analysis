import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


object Main extends App {


  //  val spark = SparkSession.builder
  //    .master("local[*]")
  //    .appName("QueryRddDFS")
  //    .getOrCreate()


  val spark = SparkSession.builder
    .appName("QueryRddDFS")
    .master("yarn")
    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
    //.config("spark.yarn.jars", "hdfs://clu01.softnet.tuc.gr:8020/user/xenia/jars/*.jar")
    .config("spark.hadoop.yarn.resourcemanager.address", "http://clu01.softnet.tuc.gr:8189")
    .config("spark.hadoop.yarn.application.classpath",
      "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*," +
        "$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*," +
        "$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*," +
        "$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*," +
        "$HADOOP_YARN_HOME/lib/*")
    .getOrCreate()


  val hdfsURI = "hdfs://clu01.softnet.tuc.gr:8020"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsURI)
  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  //  val hdfsURI = "hdfs://localhost:9000"
  //  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsURI)
  //  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  //HDFS input paths
  //  val paths = Map(
  //    "ratings" -> "/user/alexis/ml-latest/ratings.csv",
  //    "tags" -> "/user/alexis/ml-latest/tags.csv",
  //    "movies" -> "/user/alexis/ml-latest/movies.csv",
  //    "genomeScores" -> "/user/alexis/ml-latest/genome-scores.csv",
  //    "genomeTags" -> "/user/alexis/ml-latest/genome-tags.csv"
  //  )


  val paths = Map(
    "ratings" -> "/user/chrisa/ml-latest/ratings.csv",
    "tags" -> "/user/chrisa/ml-latest/tags.csv",
    "movies" -> "/user/chrisa/ml-latest/movies.csv",
    "genomeScores" -> "/user/chrisa/ml-latest/genome-scores.csv",
    "genomeTags" -> "/user/chrisa/ml-latest/genome-tags.csv"
  )

  //  val outputPaths = Map(
  //    "q1" -> "/user/alexis/ml-latest/output-iceberg-query",
  //    "q2" -> "/user/alexis/ml-latest/output-dominant-tags",
  //    "q3" -> "/user/alexis/ml-latest/output-iceberg-popular-tags",
  //    "q4" -> "/user/alexis/ml-latest/output-avg-rating-tag",
  //    "q5" -> "/user/alexis/ml-latest/output-skyline-tags",
  //    "q6" -> "/user/alexis/ml-latest/output-skyline-movies",
  //    "q7" -> "/user/alexis/ml-latest/output-tag-relevance-correlation",
  //    "q8" -> "/user/alexis/ml-latest/output-reverse-nearest-neighbor",
  //    "q9" -> "/user/alexis/ml-latest/output-overhyped-movies",
  //    "q10" -> "/user/alexis/ml-latest/output-topk-similar-users"
  //  )


  val outputPaths = Map(
    "q1" -> "/user/fp25_9/output-iceberg-query",
    "q2" -> "/user/fp25_9/output-dominant-tags",
    "q3" -> "/user/fp25_9/output-iceberg-popular-tags",
    "q4" -> "/user/fp25_9/output-avg-rating-tag",
    "q5" -> "/user/fp25_9/output-skyline-tags",
    "q6" -> "/user/fp25_9/output-skyline-movies",
    "q7" -> "/user/fp25_9/output-tag-relevance-correlation",
    "q8" -> "/user/fp25_9/output-reverse-nearest-neighbor",
    "q9" -> "/user/fp25_9/output-overhyped-movies",
    "q10" -> "/user/fp25_9/output-topk-similar-users"
  )


  val rddQueries = RDDQueries(spark, paths)

  val result_q1 = rddQueries.topTagsByGenre()
  val formatted_q1 = result_q1.map { case (genre, tag, count, avg) =>
    f"$genre,$tag,$count,$avg%.2f"
  }
  hdfs.delete(new Path(outputPaths("q1")), true)
  formatted_q1.coalesce(1).saveAsTextFile(outputPaths("q1"))

  // Query 2
  val result_q2 = rddQueries.topDominantTagsByGenreWithAvgRating()
  val formatted_q2 = result_q2.map { case ((genre, tag), avgRating) => s"$genre,$tag,$avgRating" }
  hdfs.delete(new Path(outputPaths("q2")), true)
  formatted_q2.saveAsTextFile(outputPaths("q2"))

  //Query 3
  val result_q3 = rddQueries.popularAndRelevantTags()
  val formatted_q3 = result_q3.map { case (tag, count, relevance) => f"$tag,$count,$relevance%.3f" }
  hdfs.delete(new Path(outputPaths("q3")), true)
  formatted_q3.coalesce(1).saveAsTextFile(outputPaths("q3"))

  //Query 4
  val result_q4 = rddQueries.averageRatingPerTag()
  val formatted_q4 = result_q4.map { case (tag, avgRating) => f"$tag,$avgRating%.3f" }
  hdfs.delete(new Path(outputPaths("q4")), true)
  formatted_q4.coalesce(1).saveAsTextFile(outputPaths("q4"))

  // Query 5
  val result_q5 = rddQueries.skylineGenreTagUser()
  val formatted_q5 = result_q5.map { case (genre, tag, rating, users) => f"$genre,$tag,$rating%.2f,$users" }
  hdfs.delete(new Path(outputPaths("q5")), true)
  formatted_q5.coalesce(1).saveAsTextFile(outputPaths("q5"))


  val dfQueries = DFQueries(spark, paths)


  //-------------------- Query 6 --------------------
  val skylineDF = dfQueries.skylineMovies()
  val formattedSkyline = skylineDF.map { case (movieId, rating, count, rel) => f"$movieId,$rating%.2f,$count,$rel%.3f" }
  hdfs.delete(new Path(outputPaths("q6")), true)
  formattedSkyline.coalesce(1).saveAsTextFile(outputPaths("q6"))

  //Query 7
  val correlation = dfQueries.tagRelevanceRatingCorrelation()
  val correlationRDD = spark.sparkContext.parallelize(Seq(f"Pearson correlation: $correlation%.5f"))
  hdfs.delete(new Path(outputPaths("q7")), true)
  correlationRDD.coalesce(1).saveAsTextFile(outputPaths("q7"))


  //Query 8
  val similarityDF = dfQueries.computeUserMovieCosineSimilarity(100).cache()
  similarityDF.count()


  val resultQ8 = similarityDF.rdd.map { row =>
    val userId = row.getAs[Int]("userId")
    val sim = row.getAs[Double]("cosine_similarity")
    f"$userId,${sim}%.5f"
  }

  hdfs.delete(new Path(outputPaths("q8")), true)
  resultQ8.saveAsTextFile(outputPaths("q8"))


  // Query 9
  val result_q9 = dfQueries.overhypedMovies()
  hdfs.delete(new Path(outputPaths("q9")), true)
  result_q9.saveAsTextFile(outputPaths("q9"))


  val K = 10

  // Query 10
  val result_q10 = similarityDF.limit(K).rdd.map { row =>
    val userId = row.getAs[Int]("userId")
    val sim = row.getAs[Double]("cosine_similarity")
    f"$userId,${sim}%.5f"
  }
  hdfs.delete(new Path(outputPaths("q10")), true)
  result_q10.saveAsTextFile(outputPaths("q10"))


  spark.stop()


}
