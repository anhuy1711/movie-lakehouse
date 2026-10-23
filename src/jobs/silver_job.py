import argparse

from pyspark.sql import functions as F

from common import writers
from common.checkpoint import get_checkpoint, set_checkpoint
from common.config_loader import load_config
from common.spark_session import get_spark
from common.transforms import apply_columns, apply_dq, dedup


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, help="Tên file config trong config/silver (không có .yaml)")
    ap.add_argument("--p-date", required=True, help="Ngày chạy, dạng YYYY-MM-DD")
    ap.add_argument("--upper", default=None,
                    help="Chỉ để mô phỏng CDC 1.1: chỉ lấy dòng có checkpoint_column <= giá trị này")
    args = ap.parse_args()

    cfg = load_config("silver", args.table)
    spark = get_spark(f"silver_{args.table}")

    name = cfg["name"]
    pk = cfg["primary_key"]
    cdc_type = str(cfg["cdc"]["type"])
    writer = cfg["writer"]["type"]
    target = cfg["target"]["path"]

    # 1. Đọc nguồn (nếu có khai báo partition thì chỉ đọc partition của ngày chạy)
    src = cfg["source"]
    df = spark.read.format(src["format"]).load(src["path"])
    if src.get("partition"):
        df = df.filter(F.col(src["partition"]) == args.p_date)

    # 2. Đổi tên và ép kiểu theo config
    df = apply_columns(df, cfg["columns"])

    # 3. CDC 1.1 theo cột thời gian: chỉ lấy dòng mới hơn checkpoint
    if cdc_type == "1.1" and writer == "insert_only_merge":
        cp_col = cfg["cdc"]["checkpoint_column"]
        last = get_checkpoint(spark, name)
        df = df.filter(F.col(cp_col) > F.lit(last).cast("timestamp"))
        if args.upper:
            df = df.filter(F.col(cp_col) <= F.lit(args.upper).cast("timestamp"))
        print(f"[silver] {name}: checkpoint cũ = {last}, upper = {args.upper}")

    # 4. Kiểm tra chất lượng và bỏ trùng
    good, bad = apply_dq(df, cfg["columns"])
    good = dedup(good, pk).cache()
    n_good, n_bad = good.count(), bad.count()
    print(f"[silver] {name}: {n_good} dòng đạt, {n_bad} dòng bị loại bởi quy tắc chất lượng")

    # 5. Ghi theo loại writer
    if writer == "insert_only_merge":
        if n_good == 0:
            print(f"[silver] {name}: không có dòng mới, bỏ qua.")
            return
        writers.write_insert_only_merge(spark, good, target, pk)
        new_cp = good.agg(F.max(cfg["cdc"]["checkpoint_column"])).first()[0]
        set_checkpoint(spark, name, str(new_cp))
        print(f"[silver] {name}: checkpoint mới = {new_cp}")

    elif writer == "partition_overwrite":
        pcol = cfg["writer"]["partition_column"]
        good = good.withColumn(pcol, F.lit(args.p_date))
        writers.write_partition_overwrite(spark, good, target, pcol, args.p_date)
        print(f"[silver] {name}: đã ghi đè partition {pcol}={args.p_date}")

    elif writer == "scd4a_cdc5":
        if n_good == 0:
            # Nguồn rỗng sẽ bị coi là "xóa hết": chặn lại để tránh mất dữ liệu.
            raise RuntimeError(f"{name}: nguồn rỗng, dừng để không đánh dấu xóa toàn bộ bảng.")
        stats = writers.write_scd4a_cdc5(spark, good, target, pk)
        print(f"[silver] {name}: kết quả CDC 5 = {stats}")

    else:
        raise ValueError(f"Loại writer không hỗ trợ: {writer}")


if __name__ == "__main__":
    main()