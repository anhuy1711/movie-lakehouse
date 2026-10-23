import argparse

from pyspark.sql import functions as F

from common.spark_session import get_spark
from common.writers import write_partition_overwrite


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--p-date", required=True)
    args = ap.parse_args()

    spark = get_spark("gold_fact_movie_daily_snapshot")

    snap = (spark.read.format("delta").load("s3a://silver/movie_daily_snapshot")
            .filter(F.col("p_date") == args.p_date))

    links = (spark.read.parquet("s3a://bronze/movielens/links")
             .select(F.col("movieId").cast("int").alias("movie_id"),
                     F.col("tmdbId").cast("int").alias("tmdb_id")))

    out = snap.join(F.broadcast(links), "tmdb_id", "left")
    write_partition_overwrite(spark, out, "s3a://gold/fact_movie_daily_snapshot", "p_date", args.p_date)
    print("[gold] fact_movie_daily_snapshot:", out.count(), "dòng cho", args.p_date)


if __name__ == "__main__":
    main()