from functools import reduce

from pyspark.sql import functions as F

TECH_COLS = ["ds_hash_index", "etl_timestamp", "record_status", "change_type"]


def add_hash(df, cols):
    """Gộp các cột nghiệp vụ thành một mã băm. Hai dòng cùng hash nghĩa là không đổi."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in sorted(cols)]
    return df.withColumn("ds_hash_index", F.sha2(F.concat_ws("||", *parts), 256))


def classify_changes(src, cur, pk, biz_cols):
    """So sánh nguồn hôm nay (src) với bảng hiện có (cur).

    Trả về các dòng cần ghi, mỗi dòng gắn change_type:
      CDC_INSERT: có ở nguồn, chưa có ở bảng
      CDC_UPDATE: có ở cả hai nhưng hash khác (hoặc dòng từng bị xóa nay xuất hiện lại)
      CDC_DELETE: có ở bảng, biến mất khỏi nguồn (xóa mềm: record_status = 0)
    Dòng không đổi sẽ không xuất hiện trong kết quả.
    """
    final_cols = list(biz_cols) + TECH_COLS
    on = reduce(lambda a, b: a & b, [F.col(f"s.{k}") == F.col(f"c.{k}") for k in pk])
    j = src.alias("s").join(cur.alias("c"), on, "full_outer")

    s_missing = F.col(f"s.{pk[0]}").isNull()
    c_missing = F.col(f"c.{pk[0]}").isNull()
    now = F.current_timestamp()

    inserts = (j.filter(c_missing).select("s.*")
               .withColumn("etl_timestamp", now)
               .withColumn("record_status", F.lit(1))
               .withColumn("change_type", F.lit("CDC_INSERT")))

    changed = (F.col("s.ds_hash_index") != F.col("c.ds_hash_index")) | (F.col("c.record_status") == 0)
    updates = (j.filter(~s_missing & ~c_missing & changed).select("s.*")
               .withColumn("etl_timestamp", now)
               .withColumn("record_status", F.lit(1))
               .withColumn("change_type", F.lit("CDC_UPDATE")))

    deletes = (j.filter(s_missing & (F.col("c.record_status") == 1)).select("c.*")
               .withColumn("etl_timestamp", now)
               .withColumn("record_status", F.lit(0))
               .withColumn("change_type", F.lit("CDC_DELETE")))

    return (inserts.select(final_cols)
            .unionByName(updates.select(final_cols))
            .unionByName(deletes.select(final_cols)))