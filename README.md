# MovieLens — Large-Scale Movie Analytics with Apache Spark

A Scala/Apache Spark project that runs **10 analytical queries** on the [MovieLens Latest](https://grouplens.org/datasets/movielens/) dataset. Queries are implemented using both the **RDD API** and the **DataFrame/SQL API**, covering iceberg queries, skyline queries, tag-relevance analysis, correlation, cosine similarity, and more.

> Developed as part of the **INF424** course.

---

## Table of Contents

- [Overview](#overview)
- [Dataset](#dataset)
- [Queries](#queries)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Build & Run](#build--run)
- [Configuration](#configuration)
- [Output](#output)

---

## Overview

The application reads five CSV files from HDFS (`ratings`, `tags`, `movies`, `genome-scores`, `genome-tags`) and executes a suite of analytical queries that demonstrate different Spark programming paradigms and data-analysis techniques.

| Queries 1–5 | Queries 6–10 |
|---|---|
| Implemented with the low-level **RDD API** (`RDDQueries.scala`) | Implemented with the high-level **DataFrame API** (`DFQueries.scala`) |

---

## Dataset

The project uses the **MovieLens Latest** dataset, which consists of:

| File | Description |
|---|---|
| `ratings.csv` | User ratings for movies (userId, movieId, rating, timestamp) |
| `tags.csv` | User-applied tags for movies (userId, movieId, tag, timestamp) |
| `movies.csv` | Movie metadata (movieId, title, genres) |
| `genome-scores.csv` | Tag relevance scores per movie (movieId, tagId, relevance) |
| `genome-tags.csv` | Tag ID to tag name mapping (tagId, tag) |

---

## Queries

### RDD-based (Queries 1–5)

| # | Name | Description |
|---|---|---|
| **Q1** | Top Tags by Genre (Iceberg) | Finds genre–tag pairs that appear in >100 movies and have an average rating >4.0. |
| **Q2** | Dominant Tags by Genre | Identifies the most dominant (most frequent) tag per genre and computes its average rating. |
| **Q3** | Popular & Relevant Tags | Returns genome tags with relevance ≥ 0.3, appearing in >100 movies, and average relevance >0.8. |
| **Q4** | Average Rating per Tag | Computes the average user rating for each user-applied tag. |
| **Q5** | Skyline Genre-Tag-User | Finds non-dominated (genre, tag) pairs based on average rating and distinct user count (skyline query). |

### DataFrame-based (Queries 6–10)

| # | Name | Description |
|---|---|---|
| **Q6** | Skyline Movies | Identifies skyline movies that are not dominated on average rating, rating count, and average tag relevance. |
| **Q7** | Tag Relevance–Rating Correlation | Computes the Pearson correlation between average tag relevance and average user rating per movie. |
| **Q8** | Reverse Nearest Neighbor (Cosine Similarity) | Computes cosine similarity between each user's tag-relevance profile and a target movie's profile. |
| **Q9** | Overhyped Movies | Finds movies with high relevance to selected tags (*action*, *classic*, *thriller*) but low average ratings. |
| **Q10** | Top-K Similar Users | Returns the top-K users most similar to a target movie based on the cosine similarity computed in Q8. |

---

## Tech Stack

| Component | Version |
|---|---|
| **Scala** | 2.11.8 |
| **Apache Spark** | 2.3.1 |
| **Apache Hadoop Client** | 3.3.1 |
| **Build Tool** | sbt |

---

## Project Structure

```
MovieLens/
├── build.sbt                       # SBT build definition & dependencies
├── project/
│   └── build.properties            # SBT version
├── src/
│   └── main/
│       └── scala/
│           ├── Main.scala          # Entry point — orchestrates all queries
│           ├── RDDQueries.scala    # Queries 1–5 (RDD API)
│           └── DFQueries.scala     # Queries 6–10 (DataFrame API)
└── README.md
```

---

## Prerequisites

- **Java 8** (JDK)
- **sbt** (Scala Build Tool)
- Access to an **HDFS cluster** with the MovieLens dataset uploaded, *or* a local Spark/Hadoop setup

---

## Build & Run

### 1. Clone the repository

```bash
git clone https://github.com/alexmoumtzis/movielens-spark-analysis.git
cd MovieLens
```

### 2. Build the JAR

```bash
sbt package
```

The fat JAR will be generated under `target/scala-2.11/`.

### 3. Submit to Spark

```bash
spark-submit \
  --class Main \
  --master yarn \
  target/scala-2.11/movielens_2.11-0.1.0-SNAPSHOT.jar
```

> **Local mode**: Uncomment the local `SparkSession` builder and HDFS paths in `Main.scala`, then run with `--master local[*]`.

---

## Configuration

All paths are configured in `Main.scala`:

| Setting | Description |
|---|---|
| `paths` | HDFS input paths to the five MovieLens CSV files |
| `outputPaths` | HDFS output directories for each query result |
| Spark config | Cluster address, YARN resource manager, classpath settings |

To run locally, swap the active `SparkSession` builder and path maps to the commented-out local versions in `Main.scala`.

---

## Output

Each query writes its results to a separate HDFS directory (configured in `outputPaths`). Output format is CSV-style text:

| Query | Output Format |
|---|---|
| Q1 | `genre,tag,count,avg_rating` |
| Q2 | `genre,tag,avg_rating` |
| Q3 | `tag,count,avg_relevance` |
| Q4 | `tag,avg_rating` |
| Q5 | `genre,tag,avg_rating,user_count` |
| Q6 | `movieId,avg_rating,rating_count,avg_relevance` |
| Q7 | `Pearson correlation: <value>` |
| Q8 | `userId,cosine_similarity` |
| Q9 | `movieId,avg_relevance,avg_rating` |
| Q10 | `userId,cosine_similarity` |

---
