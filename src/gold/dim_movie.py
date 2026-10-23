from pyspark.sql import functions as F

from common.spark_session import get_spark


def main():
    spark = get_spark("gold_dim_movie")

    ml = (spark.read.parquet("s3a://bronze/movielens/movies")
          .select(F.col("movieId").cast("int").alias("movie_id"),
                  F.col("title").alias("title_ml"),
                  F.col("genres").alias("genres_ml")))

    links = (spark.read.parquet("s3a://bronze/movielens/links")
             .select(F.col("movieId").cast("int").alias("movie_id"),
                     F.col("tmdbId").cast("int").alias("tmdb_id")))

    tmdb = (spark.read.format("delta").load("s3a://silver/movie")
            .filter("record_status = 1")
            .select("tmdb_id",
                    F.col("title").alias("title_tmdb"),
                    "release_date", "budget", "revenue", "runtime",
                    "status", "original_language", "overview"))

    dim = ml.join(links, "movie_id", "left").join(tmdb, "tmdb_id", "left")

    (dim.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save("s3a://gold/dim_movie"))
    print("[gold] dim_movie:", dim.count(), "dòng")


if __name__ == "__main__":
    main()