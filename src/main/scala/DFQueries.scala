import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

case class DFQueries(spark: SparkSession, paths: Map[String, String]) {

  import spark.implicits._

  // ------------------ Shared Preloaded Datasets ------------------
  val ratingsDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(paths("ratings"))

  val genomeScoresDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(paths("genomeScores"))

  val genomeTagsDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(paths("genomeTags"))

  // ------------------ Query 6: Skyline Movies ------------------
  def skylineMovies(): RDD[(String, Double, Long, Double)] = {
    val avgRatingDF = ratingsDF.groupBy("movieId").agg(avg("rating").as("avg_movie_rating"))
    val ratingCountDF = ratingsDF.groupBy("movieId").agg(count("*").as("rating_count"))
    val filteredScoresDF = genomeScoresDF.filter($"relevance" >= 0.1)
    val avgTagRelevanceDF = filteredScoresDF.groupBy("movieId").agg(avg("relevance").as("avg_tag_relevance"))

    val movieStatsDF = avgRatingDF
      .join(ratingCountDF, Seq("movieId"))
      .join(avgTagRelevanceDF, Seq("movieId"))


    val movieStats = movieStatsDF
      .select("movieId", "avg_movie_rating", "rating_count", "avg_tag_relevance")
      .as[(String, Double, Long, Double)]
      .collect()
      .toList

    val skyline = movieStats.filter { case (_, rating1, count1, relevance1) =>
      movieStats.forall {
        case (_, rating2, count2, relevance2) =>
          rating2 <= rating1 || count2 <= count1 || relevance2 <= relevance1
      }
    }

    spark.sparkContext.parallelize(skyline)
  }

  // ------------------ Query 7: Correlation ------------------
  def tagRelevanceRatingCorrelation(): Double = {
    val filteredScoresDF = genomeScoresDF.filter($"relevance" >= 0.1)
    val avgTagRelevanceDF = filteredScoresDF.groupBy("movieId").agg(avg("relevance").as("avg_tag_relevance"))
    val avgRatingDF = ratingsDF.groupBy("movieId").agg(avg("rating").as("avg_user_rating"))

    val joinedDF = avgTagRelevanceDF.join(avgRatingDF, Seq("movieId")).select("avg_tag_relevance", "avg_user_rating")

    joinedDF.stat.corr("avg_tag_relevance", "avg_user_rating")
  }

  // ------------------ Query 8 ----------------------
  def computeUserMovieCosineSimilarity(targetMovieId: Int): DataFrame = {
    val likedThreshold = 4.0

    val likedRatingsDF = ratingsDF
      .filter($"rating" > likedThreshold)
      .select("userId", "movieId")

    val filteredGenomeScoresDF = genomeScoresDF
      .filter($"relevance" >= 0.2)
      .select("movieId", "tagId", "relevance")

    val targetVectorDF = filteredGenomeScoresDF
      .filter($"movieId" === targetMovieId)
      .select("tagId", "relevance")
      .withColumnRenamed("relevance", "target_relevance")

    val targetNorm = math.sqrt(
      targetVectorDF
        .withColumn("squared", pow($"target_relevance", 2))
        .agg(sum("squared")).first().getDouble(0)
    )

    val userTagRelevanceDF = likedRatingsDF
      .join(filteredGenomeScoresDF, "movieId")
      .groupBy("userId", "tagId")
      .agg(avg("relevance").as("user_avg_relevance"))

    val joined = userTagRelevanceDF.join(targetVectorDF, Seq("tagId"))

    val userDotProduct = joined
      .withColumn("product", $"user_avg_relevance" * $"target_relevance")
      .groupBy("userId")
      .agg(
        sum("product").as("dot_product"),
        sqrt(sum(pow($"user_avg_relevance", 2))).as("user_norm")
      )

    val cosineSimilarityDF = userDotProduct
      .withColumn("cosine_similarity",
        when($"user_norm" =!= 0,
          $"dot_product" / ($"user_norm" * targetNorm)
        ).otherwise(0.0)
      )
      .select("userId", "cosine_similarity")
      .orderBy(desc("cosine_similarity"))

    cosineSimilarityDF
  }


  // ------------------ Query 9: Overhyped Movies ------------------
  def overhypedMovies(): RDD[String] = {
    val selectedTagsDF = Seq("action", "classic", "thriller").toDF("tag")

    val selectedTagIdsDF = genomeTagsDF
      .select("tagId", "tag")
      .join(selectedTagsDF, Seq("tag"))
      .select("tagId")
      .distinct()

    val filteredRelevanceDF = genomeScoresDF
      .join(selectedTagIdsDF, Seq("tagId"))
      .filter($"relevance" >= 0.1)

    val avgRelevancePerMovieDF = filteredRelevanceDF.groupBy("movieId").agg(avg("relevance").as("avg_relevance_to_selected_tags"))
    val avgRatingDF = ratingsDF.groupBy("movieId").agg(avg("rating").as("avg_rating"))

    val overhypedMoviesDF = avgRelevancePerMovieDF
      .join(avgRatingDF, Seq("movieId"))
      .filter($"avg_relevance_to_selected_tags" >= 0.8 && $"avg_rating" < 2.5)
      .orderBy($"avg_relevance_to_selected_tags".desc)

    overhypedMoviesDF.rdd.map { row =>
      val movieId = row.getAs[Int]("movieId")
      val relevance = row.getAs[Double]("avg_relevance_to_selected_tags")
      val rating = row.getAs[Double]("avg_rating")
      f"$movieId,$relevance%.3f,$rating%.2f"
    }
  }


}
