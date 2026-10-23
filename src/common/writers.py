from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from common.cdc import add_hash, classify_changes


def table_exists(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path)
        return True
    except AnalysisException:
        return False


def write_insert_only_merge(spark, df, path: str, pk: list) -> None:
    """Chỉ thêm dòng chưa có (theo khóa chính). Chạy lại không sinh trùng."""
    if not table_exists(spark, path):
        df.write.format("delta").save(path)
        return
    df.createOrReplaceTempView("_src")
    on = " AND ".join(f"t.{k} = s.{k}" for k in pk)
    spark.sql(f"MERGE INTO delta.`{path}` t USING _src s ON {on} "
              f"WHEN NOT MATCHED THEN INSERT *")


def write_partition_overwrite(spark, df, path: str, part_col: str, p_date: str) -> None:
    """Ghi đè đúng partition của ngày chạy. Chạy lại cùng ngày không sinh trùng."""
    if not table_exists(spark, path):
        df.write.format("delta").partitionBy(part_col).save(path)
        return
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"{part_col} = '{p_date}'")
       .partitionBy(part_col).save(path))


def write_scd4a_cdc5(spark, src, path: str, pk: list) -> dict:
    """CDC 5: so hash nguồn với bảng hiện có, rồi MERGE phần thay đổi."""
    biz_cols = src.columns
    src = add_hash(src, biz_cols)

    if not table_exists(spark, path):
        n = src.count()
        (src.withColumn("etl_timestamp", F.current_timestamp())
            .withColumn("record_status", F.lit(1))
            .withColumn("change_type", F.lit("CDC_INSERT"))
            .write.format("delta").save(path))
        return {"CDC_INSERT": n}

    cur = spark.read.format("delta").load(path)
    changes = classify_changes(src, cur, pk, biz_cols).cache()
    stats = {r["change_type"]: r["count"]
             for r in changes.groupBy("change_type").count().collect()}
    if stats:
        changes.createOrReplaceTempView("_changes")
        on = " AND ".join(f"t.{k} = s.{k}" for k in pk)
        spark.sql(f"MERGE INTO delta.`{path}` t USING _changes s ON {on} "
                  f"WHEN MATCHED THEN UPDATE SET * "
                  f"WHEN NOT MATCHED THEN INSERT *")
    changes.unpersist()
    return stats