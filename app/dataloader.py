import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType


def load_test_data(
    dtype: str,
    dt_range: tuple,
    topcodes: list,
    config,
    gage_cli,
    spark_sess,
):
    objects = gage_cli.find_object(
        bucket_nm=config.bucket_name, dtype=dtype, dt_range=dt_range, topcode=topcodes
    )
    print(f"found {len(objects)} object files in an object storage")
    base_path = f"s3a://{config.bucket_name}/MQM/primitive/{dtype}_SPC"
    df = (
        spark_sess.session.read.option("mergeSchema", "true")
        .option("basePath", base_path)
        .parquet(*objects)
    )
    print(f"created pyspark dataframe with {len(objects)} objects")
    return df


def load_line_info(factory: str, spark_sess, ltype=None, maker=None):
    schema = StructType(
        [
            StructField("FACTORY", StringType(), True),
            StructField("LINE_CODE", StringType(), True),
            StructField("LINE_NAME", StringType(), True),
            StructField("PROCESS_TYPE", StringType(), True),
            StructField("MANUF_PART", StringType(), True),
            StructField("LINE_LOC", StringType(), True),
            StructField("LINE_TYPE", StringType(), True),
            StructField("OP_ST_TIME", StringType(), True),
            StructField("IN_USE", StringType(), True),
        ]
    )
    # 파일 로딩 후 spark dataframe으로 전환
    line_info_pd = pd.read_csv("master_info/line_info.txt", sep="\t")
    line_info_df = spark_sess.session.createDataFrame(line_info_pd, schema=schema)
    line_info_df = line_info_df.where(F.lower("FACTORY") == factory)
    if ltype:
        line_info_df = line_info_df.where(F.col("LINE_TYPE").isin(ltype))

    if maker:
        outsourcing_lines = line_info_df.where(
            F.col("LINE_NM").startswith("DDT")  # 대동통신
            | F.col("LINE_NM").startswith("MBT")  # 모바일텍
            | F.col("LINE_NM").startswith("KMT")  # 케이엠텍
            | F.col("LINE_NM").startswith("DT")  # 드림텍
            | F.col("LINE_NM").startswith("HS")  # 한솔
        )
        if maker == "outsourcing":
            return outsourcing_lines
        if maker == "inhouse":
            inhouse_lines = line_info_df.join(outsourcing_lines, "LINE_NM", "leftanti")
            return inhouse_lines
    return line_info_df
