import os

from pyspark.sql.utils import AnalysisException

ROOT = os.environ.get("CHECKPOINT_ROOT", "s3a://meta/checkpoints")
DEFAULT = "1970-01-01 00:00:00"


def get_checkpoint(spark, table: str, default: str = DEFAULT) -> str:
    try:
        row = spark.read.json(f"{ROOT}/{table}").first()
        return row["value"] if row else default
    except AnalysisException:      # chưa có checkpoint: lần chạy đầu tiên
        return default


def set_checkpoint(spark, table: str, value: str) -> None:
    (spark.createDataFrame([(table, value)], ["table", "value"])
          .coalesce(1).write.mode("overwrite").json(f"{ROOT}/{table}"))