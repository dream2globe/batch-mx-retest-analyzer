from pyspark.sql import SparkSession

from app.settings import settings


def get_spark_session() -> SparkSession:
    """
    Get or create a Spark session.

    Returns:
        SparkSession: The Spark session.
    """
    spark_builder = (
        SparkSession.builder.appName(settings.spark.app_name)
        .master(settings.spark.master)
        .config("spark.driver.host", settings.spark.driver_host)
        .config("spark.driver.port", settings.spark.driver_port)
        .config("spark.ui.port", settings.spark.ui_port)
        .config("spark.executor.instances", settings.spark.executor_instances)
        .config("spark.executor.cores", settings.spark.executor_cores)
        .config("spark.executor.memory", settings.spark.executor_memory)
        .config("spark.driver.memory", settings.spark.driver_memory)
        .config("spark.jars.packages", settings.spark.jars_packages)
        .config("spark.sql.extensions", settings.spark.sql_extensions)
        .config(
            "spark.sql.catalog.spark_catalog",
            settings.spark.sql_catalog_spark_catalog,
        )
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3.endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    return spark_builder.getOrCreate()
