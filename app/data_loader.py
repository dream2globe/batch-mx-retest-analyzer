import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from app.settings import AppSettings


def null_condition(field_name: str):
    return (
        (F.col(field_name) == "")
        | (F.col(field_name) == "null")
        | F.col(field_name).isNull()
    )


def load_and_preprocess_data(
    spark: SparkSession, settings: AppSettings, s3_bucket: str
) -> DataFrame:
    """
    Load and preprocess data from S3.

    Args:
        spark (SparkSession): The Spark session.
        settings (AppSettings): Application settings.
        s3_bucket (str): The S3 bucket name.

    Returns:
        DataFrame: The preprocessed data.
    """
    # This is a placeholder for the actual data loading logic.
    # In a real scenario, you would read data from S3 based on the date ranges
    # and other parameters in the settings.
    # For this example, we'll create a dummy dataframe.

    # Dummy data to simulate the structure of the actual data
    schema = T.StructType(
        [
            T.StructField("dt", T.IntegerType(), True),
            T.StructField("INSP_DT", T.StringType(), True),
            T.StructField("P_N", T.StringType(), True),
            T.StructField("MODEL", T.StringType(), True),
            T.StructField("LINE_CODE", T.StringType(), True),
            T.StructField("BCR_IP", T.StringType(), True),
            T.StructField("JIG", T.StringType(), True),
            T.StructField("TEST_NAME", T.StringType(), True),
            T.StructField("RESULT", T.StringType(), True),
            T.StructField("FAILITEM", T.StringType(), True),
            T.StructField("TESTCODE", T.StringType(), True),
            T.StructField("ERROR_CODE", T.StringType(), True),
            T.StructField("TEST_TIME", T.StringType(), True),
        ]
    )

    data = [
        (
            20250707,
            "20250707080000",
            "PN1",
            "SM-F966N",
            "L1",
            "IP1",
            "J1",
            "TEST1",
            "PASS",
            "",
            "TOP42",
            "",
            "10.5",
        ),
        (
            20250707,
            "20250707080100",
            "PN1",
            "SM-F966N",
            "L1",
            "IP1",
            "J1",
            "TEST2",
            "FAIL",
            "ITEM1",
            "TOP41",
            "E034",
            "20.0",
        ),
        (
            20250707,
            "20250707090000",
            "PN1",
            "SM-F966N",
            "L1",
            "IP1",
            "J1",
            "TEST2",
            "PASS",
            "",
            "TOP41",
            "",
            "15.0",
        ),
    ]
    master_df = spark.createDataFrame(data, schema=schema)

    # The actual data loading would look something like this:
    # master_df = spark.read.parquet(f"s3a://{s3_bucket}/master_data/")

    columns = [
        "dt",
        F.to_timestamp("INSP_DT", "yyyyMMddHHmmss").alias("INSP_DT"),
        F.unix_timestamp("INSP_DT", "yyyyMMddHHmmss").alias("UNIX_INSP_DT"),
        "P_N",
        "MODEL",
        "LINE_CODE",
        "BCR_IP",
        "JIG",
        "TEST_NAME",
        F.upper("RESULT").alias("RESULT"),
        "FAILITEM",
        "TESTCODE",
        "ERROR_CODE",
        F.col("TEST_TIME").cast(T.DoubleType()),
    ]

    basic_df = (
        master_df.select(columns)
        .where(~null_condition("P_N"))
        .where(
            (F.col("INSP_DT") >= settings.start_duration)
            & (F.col("INSP_DT") <= settings.end_duration)
        )
    )

    return basic_df
