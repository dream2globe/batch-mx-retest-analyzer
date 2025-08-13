from pyspark.sql import DataFrame

from app.settings import AppSettings, S3Settings


def write_results_to_s3(
    retry_df: DataFrame,
    retest_df: DataFrame,
    app_settings: AppSettings,
    s3_settings: S3Settings,
):
    """
    Writes the analysis results to S3 in CSV format.

    Args:
        retry_df (DataFrame): The DataFrame containing the retry analysis results.
        retest_df (DataFrame): The DataFrame containing the retest analysis results.
        app_settings (AppSettings): The application settings.
        s3_settings (S3Settings): The S3 settings.
    """
    start_date = app_settings.start_duration.split(" ")[0]
    end_date = app_settings.end_duration.split(" ")[0]
    factory = app_settings.factory

    retry_output_path = (
        f"s3a://{s3_settings.bucket}/output/retry_{factory}_{start_date}_{end_date}.csv"
    )
    retest_output_path = f"s3a://{s3_settings.bucket}/output/retest_{factory}_{start_date}_{end_date}.csv"

    retry_df.write.mode("overwrite").csv(retry_output_path, header=True)
    retest_df.write.mode("overwrite").csv(retest_output_path, header=True)
