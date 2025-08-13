from app.analyzer import analyze_retest_data
from app.data_loader import load_and_preprocess_data
from app.settings import settings
from app.spark import get_spark_session
from app.writer import write_results_to_s3


def main():
    """
    Main function to run the retest analysis pipeline.
    """
    spark = get_spark_session()
    basic_df = load_and_preprocess_data(spark, settings.app, settings.s3.bucket)
    retry_df, retest_df = analyze_retest_data(basic_df)
    write_results_to_s3(retry_df, retest_df, settings.app, settings.s3)
    spark.stop()


if __name__ == "__main__":
    main()
