from functools import reduce

from pyspark.sql import functions as F

TYPES = {
    "int": "int", "long": "bigint", "double": "double", "string": "string",
    "date": "date", "timestamp": "timestamp", "boolean": "boolean",
}


def apply_columns(df, cols_cfg: dict):
    """Chọn đúng các cột khai báo trong config, đổi tên và ép kiểu."""
    out = []
    for src_name, spec in cols_cfg.items():
        c = F.col(src_name)
        if spec.get("epoch"):                       # epoch giây -> timestamp
            c = F.to_timestamp(c.cast("long"))
        else:
            c = c.cast(TYPES[spec["type"]])
        out.append(c.alias(spec.get("to", src_name)))
    return df.select(*out)


def apply_dq(df, cols_cfg: dict):
    """Tách dữ liệu thành (dòng tốt, dòng bị loại) theo quy tắc trong config."""
    conds = []
    for src_name, spec in cols_cfg.items():
        name = spec.get("to", src_name)
        col = F.col(name)
        if spec.get("nullable") is False:
            conds.append(col.isNotNull())
        if "min" in spec:
            conds.append(col.isNull() | (col >= spec["min"]))
        if "max" in spec:
            conds.append(col.isNull() | (col <= spec["max"]))
    ok = reduce(lambda a, b: a & b, conds, F.lit(True))
    ok = F.coalesce(ok, F.lit(False))
    return df.filter(ok), df.filter(~ok)


def dedup(df, pk: list):
    return df.dropDuplicates(pk)