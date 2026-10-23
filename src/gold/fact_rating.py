from pyspark.sql import functions as F

from common.spark_session import get_spark


def main():
    spark = get_spark("gold_fact_rating")
    r = spark.read.format("delta").load("s3a://silver/ratings")

    fact = (r.withColumn("rated_date", F.to_date("rated_at"))
             .withColumn("rated_year", F.year("rated_at")))

    (fact.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .partitionBy("rated_year").save("s3a://gold/fact_rating"))
    print("[gold] fact_rating:", fact.count(), "dòng")


if __name__ == "__main__":
    main()