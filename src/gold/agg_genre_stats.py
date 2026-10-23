from pyspark.sql import functions as F

from common.spark_session import get_spark


def main():
    spark = get_spark("gold_agg_genre_stats")

    r = spark.read.format("delta").load("s3a://gold/fact_rating").select("movie_id", "rating")
    d = spark.read.format("delta").load("s3a://gold/dim_movie").select("movie_id", "genres_ml")

    # fact_rating lớn, dim_movie nhỏ: broadcast dim để khỏi shuffle bảng lớn
    j = r.join(F.broadcast(d), "movie_id")
    g = j.withColumn("genre", F.explode(F.split("genres_ml", r"\|")))

    out = (g.groupBy("genre")
            .agg(F.count("*").alias("n_ratings"),
                 F.round(F.avg("rating"), 3).alias("avg_rating"))
            .orderBy(F.desc("n_ratings")))

    out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://gold/agg_genre_stats")
    out.show(20, truncate=False)


if __name__ == "__main__":
    main()