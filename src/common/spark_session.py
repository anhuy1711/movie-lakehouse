from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    # Toàn bộ cấu hình S3/Delta nằm ở conf/spark-defaults.conf nên code không lặp lại.
    return SparkSession.builder.appName(app_name).getOrCreate()