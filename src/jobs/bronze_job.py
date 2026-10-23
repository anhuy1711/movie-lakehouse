import argparse

from pyspark.sql import functions as F

from common.config_loader import load_config
from common.spark_session import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, help="Tên file config trong config/bronze (không có .yaml)")
    ap.add_argument("--p-date", required=True, help="Ngày chạy, dạng YYYY-MM-DD")
    args = ap.parse_args()

    cfg = load_config("bronze", args.table)
    spark = get_spark(f"bronze_{args.table}")

    src = cfg["source"]
    reader = spark.read.format(src["format"])
    for k, v in src.get("options", {}).items():
        reader = reader.option(k, v)
    df = reader.load(src["path"].format(p_date=args.p_date))

    df = (df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name()))

    part = cfg["target"].get("partition_by")
    if part == "p_date":
        df = df.withColumn("p_date", F.lit(args.p_date))
        w = df.write.mode("overwrite").partitionBy("p_date")
    else:
        w = df.write.mode("overwrite")
    w.parquet(cfg["target"]["path"])

    n = spark.read.parquet(cfg["target"]["path"]).count()
    print(f"[bronze] {args.table}: đã ghi {n} dòng vào {cfg['target']['path']}")


if __name__ == "__main__":
    main()