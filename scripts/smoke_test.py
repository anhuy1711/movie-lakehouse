from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke_test").getOrCreate()
spark.range(5).write.mode("overwrite").format("delta").save("s3a://meta/smoke_test")
print("ĐỌC LẠI:", spark.read.format("delta").load("s3a://meta/smoke_test").count())