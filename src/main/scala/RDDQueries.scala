import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

case class RDDQueries(spark: SparkSession, paths: Map[String, String]) {
  val sc = spark.sparkContext

  // ------------------- Shared RDDs for tags, ratings.. -------------------
  val ratingsRDD = sc.textFile(paths("ratings"))
    .filter(!_.startsWith("userId"))
    .map(_.split(","))
    .map(fields => (fields(0).toInt, fields(1).toInt, fields(2).toDouble)) // (userId, movieId, rating)

  val tagsRDD = sc.textFile(paths("tags"))
    .filter(!_.startsWith("userId"))
    .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
    .map(fields => (fields(0).toInt, fields(1).toInt, fields(2).trim.toLowerCase)) // (userId, movieId, tag)

  val moviesRDD = sc.textFile(paths("movies"))
    .filter(!_.startsWith("movieId"))
    .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
    .map(fields => (fields(0).toInt, fields(2).split("\\|"))) // (movieId, genres)

  val genomeScoresRDD = sc.textFile(paths("genomeScores"))
    .filter(!_.startsWith("movieId"))
    .map(_.split(","))
    .map(fields => (fields(0).toInt, fields(1).toInt, fields(2).toDouble)) // (movieId, tagId, relevance)

  val genomeTagsRDD = sc.textFile(paths("genomeTags"))
    .filter(!_.startsWith("tagId"))
    .map(_.split(","))
    .map(fields => (fields(0).toInt, fields(1))) // (tagId, tagText)

  // ------------------- Query 1 -------------------
  def topTagsByGenre(): RDD[(String, String, Int, Double)] = {
    // (movieId, genre)
    val movieGenres = moviesRDD.flatMap { case (movieId, genres) =>
      genres.map(genre => (movieId, genre))
    }

    // (movieId, tag)
    val movieTags = tagsRDD
      .map { case (_, movieId, tag) => (movieId, tag) }

    // ((genre, tag), movieId)
    val genreTagMovie = movieGenres
      .join(movieTags)
      .map { case (movieId, (genre, tag)) => ((genre, tag), movieId) }
      .distinct()

    // Filter genre-tag pairs appearing in at least 100 movies
    val genreTagCounts = genreTagMovie
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
      .filter { case (_, count) => count > 100 }

    // Keep only those (genre, tag) pairs
    val filteredGenreTagMovie = genreTagMovie
      .join(genreTagCounts)
      .map { case ((genre, tag), (movieId, _)) => (movieId, (genre, tag)) }

    // (movieId, rating)
    val ratings = ratingsRDD.map { case (_, movieId, rating) => (movieId, rating) }

    val partitioner = new HashPartitioner(100)

    val ratingsPartitioned = ratings.partitionBy(partitioner)
    val filteredGenreTagMoviePartitioned = filteredGenreTagMovie.partitionBy(partitioner)

    // Join and compute average rating per (genre, tag)
    val genreTagRatings = filteredGenreTagMoviePartitioned
      .join(ratingsPartitioned) // (movieId, ((genre, tag), rating))
      .map { case (_, ((genre, tag), rating)) => ((genre, tag), (rating, 1)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (sum, count) => sum / count }
      .filter { case (_, avg) => avg > 4.0 }

    // Combine with count info for final output
    val result = genreTagRatings
      .join(genreTagCounts) // ((genre, tag), (avg, count))
      .map { case ((genre, tag), (avg, count)) => (genre, tag, count, avg) }

    result
  }


  // ------------------- Query 2 -------------------
  def topDominantTagsByGenreWithAvgRating(): RDD[((String, String), Double)] = {
    val movieGenres = moviesRDD.flatMap { case (movieId, genres) =>
      genres.map(g => (movieId, g))
    }

    val movieTags = tagsRDD.map { case (_, movieId, tag) => (movieId, tag) }

    val ratings = ratingsRDD.map { case (_, movieId, rating) => (movieId, rating) }

    val genreTagCounts = movieGenres
      .join(movieTags)
      .distinct()
      .map { case (_, (genre, tag)) => ((genre, tag), 1) }
      .reduceByKey(_ + _)


    val topTagsPerGenre = genreTagCounts
      .map { case ((genre, tag), count) => (genre, (tag, count)) }
      .groupByKey()
      .mapValues(_.maxBy(_._2))
      .map { case (genre, (tag, _)) => ((genre, tag), 1) }

    val genreTagMovie = movieGenres.join(movieTags).map {
      case (movieId, (genre, tag)) => ((genre, tag), movieId)
    }.distinct() // prevent duplicates

    val topTagMovies = topTagsPerGenre.join(genreTagMovie)
      .map { case ((genre, tag), (_, movieId)) => (movieId, (genre, tag)) }

    val tagRatings = topTagMovies.join(ratings)
      .map { case (_, ((genre, tag), rating)) => ((genre, tag), (rating, 1)) }

    tagRatings
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (sum, count) => sum / count }
  }

  // ------------------- Query 3 -------------------
  def popularAndRelevantTags(): RDD[(String, Int, Double)] = {
    val filteredScores = genomeScoresRDD
      .filter { case (_, _, relevance) => relevance >= 0.3 }
      .map { case (movieId, tagId, relevance) => (tagId, (movieId, relevance)) }

    val aggregatedByTag = filteredScores
      .groupByKey()
      .mapValues { iterable =>
        val movieSet = iterable.map(_._1).toSet
        val relevanceSum = iterable.map(_._2).sum
        val count = iterable.size
        (movieSet.size, relevanceSum / count)
      }
      .filter { case (_, (movieCount, avgRel)) =>
        movieCount > 100 && avgRel > 0.8
      }


    aggregatedByTag.join(genomeTagsRDD)
      .map { case (_, ((count, rel), tagText)) => (tagText, count, rel) }
  }

  // ------------------- Query 4 -------------------
  def averageRatingPerTag(): RDD[(String, Double)] = {
    val tags = tagsRDD.map { case (userId, movieId, tag) => ((userId, movieId), tag) }
    val ratings = ratingsRDD.map { case (userId, movieId, rating) => ((userId, movieId), rating) }

    tags.join(ratings)
      .map { case (_, (tag, rating)) => (tag, (rating, 1)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (sum, count) => sum / count }
  }

  // ------------------- Query 5 -------------------
  def skylineGenreTagUser(): RDD[(String, String, Double, Int)] = {
    val movieGenres = moviesRDD.flatMap { case (movieId, genres) => genres.map(genre => (movieId, genre)) } // (movieId, genre) pairs for each genre a movie belongs to

    val tagsWithMovie = tagsRDD.map { case (_, movieId, tag) => (movieId, tag) } // (movieId, tag) pairs from tag data

    val genreTagMovieMap = movieGenres.join(tagsWithMovie)
      .map { case (movieId, (genre, tag)) => ((genre, tag), movieId) } // ((genre, tag), movieId) mapping for genre-tag combinations per movie

    val genreTagCounts = genreTagMovieMap.distinct()
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
      .filter(_._2 > 200) // ((genre, tag), count) where the tag appears in >200 movies of the genre

    val filteredMovieMap = genreTagMovieMap.distinct()
      .join(genreTagCounts)
      .map { case ((genre, tag), (movieId, _)) => (movieId, (genre, tag)) } // (movieId, (genre, tag)) only for frequent genre-tag combos

    val ratings = ratingsRDD.map { case (_, movieId, rating) => (movieId, rating) } // (movieId, rating) for each rating

    val avgGenreTagRatings = filteredMovieMap
      .join(ratings)
      .map { case (_, ((genre, tag), rating)) => ((genre, tag), (rating, 1)) }
      .reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2) }
      .mapValues { case (sum, count) => sum / count } // ((genre, tag), avg_rating) over all movies with that genre-tag

    val tagsWithUser = tagsRDD.map { case (userId, movieId, tag) => (movieId, (tag, userId)) } // (movieId, (tag, userId)) to track tag origins

    val genreTagToUser = tagsWithUser.join(movieGenres)
      .map { case (_, ((tag, userId), genre)) => ((genre, tag), userId) }
      .distinct() // ((genre, tag), userId) for distinct users who used a tag in a genre

    val genreTagUserCounts = genreTagToUser
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
      .join(genreTagCounts)
      .map { case ((genre, tag), (userCount, _)) => ((genre, tag), userCount) } // ((genre, tag), user_count) filtered by tag frequency

    val combined = avgGenreTagRatings.join(genreTagUserCounts)
      .filter { case (_, (avgRating, userCount)) => avgRating >= 3.5 && userCount >= 200 } // ((genre, tag), (avgRating, userCount)) that pass thresholds

    val skyline = combined.cartesian(combined)
      .filter { case (((g1, t1), _), ((g2, t2), _)) => !(g1 == g2 && t1 == t2) }
      .map { case (((g1, t1), (r1, u1)), ((_, _), (r2, u2))) =>
        ((g1, t1), !(r2 > r1 && u2 > u1))
      } // mark (genre, tag) as dominated if another has both better rating and user count
      .groupByKey()
      .filter { case (_, flags) => flags.forall(identity) }
      .map { case ((genre, tag), _) => ((genre, tag), ()) } // keep only non-dominated (genre, tag) pairs

    val finalSkyline = skyline
      .join(combined)
      .map { case ((genre, tag), (_, (rating, users))) => (genre, tag, rating, users) } // output (genre, tag, avg_rating, user_count) for skyline points

    finalSkyline
  }


}
