import argparse

from common.spark_session import get_spark


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", required=True)
    ap.add_argument("--format", default="delta", choices=["delta", "parquet"])
    ap.add_argument("--rows", type=int, default=10)
    args = ap.parse_args()

    spark = get_spark("show_table")
    df = spark.read.format(args.format).load(args.path)
    df.printSchema()
    print("Số dòng:", df.count())
    df.show(args.rows, truncate=False)


if __name__ == "__main__":
    main()