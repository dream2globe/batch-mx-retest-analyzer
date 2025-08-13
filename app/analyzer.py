import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def analyze_retest_data(basic_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Analyzes the preprocessed data to identify and quantify retest and retry cases.

    Args:
        basic_df (DataFrame): The preprocessed data.

    Returns:
        tuple[DataFrame, DataFrame]: A tuple containing two DataFrames:
                                     - The first DataFrame has the retry analysis results.
                                     - The second DataFrame has the retest analysis results.
    """
    # Determine the most produced models
    production_num_df = (
        basic_df.where(F.col("TESTCODE") == "TOP42")
        .groupby("MODEL")
        .agg(F.count("*").alias("final_test_number"))
        .orderBy(F.desc("final_test_number"))
    )
    models = [row.MODEL for row in production_num_df.limit(5).collect()]

    have_retry_df = (
        basic_df.where(F.col("MODEL").isin(models))
        .where(F.col("ERROR_CODE") == "E034")
        .select("MODEL", "TESTCODE")
        .distinct()
    )

    pass_df = (
        basic_df.where(F.col("MODEL").isin(models))
        .join(have_retry_df, ["MODEL", "TESTCODE"], "inner")
        .where(F.col("RESULT") == "PASS")
    )

    wind_model_seq = Window.partitionBy(["MODEL"]).orderBy(F.desc("count"))

    testcode_dist_num_df = (
        pass_df.groupBy("MODEL", "P_N")
        .agg(F.count_distinct("TESTCODE").alias("MODEL_TEST_DIST_NUM"))
        .groupBy("MODEL", "MODEL_TEST_DIST_NUM")
        .count()
        .where(F.col("MODEL_TEST_DIST_NUM") > 5)
        .orderBy("MODEL", F.desc("count"))
        .withColumn("RANK", F.rank().over(wind_model_seq))
        .orderBy("MODEL", "RANK")
    )

    testcode_dist_num_df = testcode_dist_num_df.where(F.col("RANK") == 1).cache()

    wind_pn_seq = Window.partitionBy(["MODEL", "P_N"]).orderBy(F.asc("INSP_DT"))
    wind_pn_all = Window.partitionBy(["MODEL", "P_N"]).rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    pure_pass_set_df = (
        pass_df.withColumn(
            "PN_TEST_NUM", F.size(F.collect_list("TESTCODE").over(wind_pn_all))
        )
        .withColumn(
            "PN_TEST_DIST_NUM", F.size(F.collect_set("TESTCODE").over(wind_pn_all))
        )
        .join(
            testcode_dist_num_df.select(["MODEL", "MODEL_TEST_DIST_NUM"]),
            "MODEL",
            "left",
        )
        .where(
            (F.col("PN_TEST_NUM") == F.col("MODEL_TEST_DIST_NUM"))
            & (F.col("PN_TEST_DIST_NUM") == F.col("MODEL_TEST_DIST_NUM"))
        )
        .withColumn("TESTCODE_SEQUENCE", F.row_number().over(wind_pn_seq))
    )

    wind_spec = Window.partitionBy("MODEL", "TESTCODE_SEQUENCE").orderBy(
        F.desc("SEQUENCE_COUNT")
    )
    test_seq_df = (
        pure_pass_set_df.groupBy("MODEL", "TESTCODE_SEQUENCE", "TESTCODE")
        .agg(
            F.count("*").alias("SEQUENCE_COUNT"),
        )
        .withColumn("RANK", F.row_number().over(wind_spec))
    )

    test_seq_df = (
        test_seq_df.where(F.col("RANK") == 1)
        .select(
            "MODEL",
            "TESTCODE",
            F.col("TESTCODE_SEQUENCE").alias("TEST_SEQ"),
        )
        .orderBy("MODEL", "TEST_SEQ")
    )

    test_seq_df.cache()

    adusted_seq_df = (
        basic_df.join(F.broadcast(test_seq_df), ["MODEL", "TESTCODE"], "inner")
        .withColumn(
            "INTER_TEST_SEQ",
            F.when(F.col("TESTCODE").isin(["TOP31", "TOP51"]), 1).otherwise(0),
        )
        .withColumn("TEST_SEQ", F.col("TEST_SEQ") - F.col("INTER_TEST_SEQ"))
    )

    retest_point = (F.col("INSP_DT_DIFF") > 60 * 60 * 1) & (
        F.col("PRE_TEST_SEQ") >= F.col("TEST_SEQ")
    )

    added_retestid_df = (
        adusted_seq_df.withColumn(
            "PRE_TEST_SEQ", F.lag(F.col("TEST_SEQ"), 1).over(wind_pn_seq)
        )
        .fillna(0, subset=["PRE_TEST_SEQ"])
        .withColumn(
            "INSP_DT_DIFF",
            F.col("UNIX_INSP_DT") - F.lag(F.col("UNIX_INSP_DT"), 1).over(wind_pn_seq),
        )
        .withColumn(
            "RETEST_GID",
            F.sum(F.when(retest_point, 1).otherwise(0)).over(wind_pn_seq),
        )
    )
    wind_cols = ["MODEL", "P_N", "TEST_SEQ", "TESTCODE", "RETEST_GID"]
    wind_seq_retry = Window.partitionBy(wind_cols).orderBy(F.asc("INSP_DT"))
    wind_all_retry = (
        Window.partitionBy(wind_cols)
        .orderBy(F.asc("INSP_DT"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    is_preerrorcode_null = (
        (F.col("PRE_ERROR_CODE") == "null")
        | (F.col("PRE_ERROR_CODE") == "")
        | (F.col("PRE_ERROR_CODE").isNull())
    )
    retry_point = (
        (F.col("ERROR_CODE") == "E034")
        & is_preerrorcode_null
        & (F.col("PRE_TEST_SEQ") == F.col("TEST_SEQ"))
    )
    added_retryid_df = (
        added_retestid_df.withColumn(
            "PRE_ERROR_CODE", F.lag(F.col("ERROR_CODE"), 1).over(wind_seq_retry)
        )
        .withColumn(
            "ERROR_CODES", F.collect_list(F.col("ERROR_CODE")).over(wind_all_retry)
        )
        .withColumn("PRE_TEST_SEQ", F.lag(F.col("TEST_SEQ")).over(wind_seq_retry))
        .withColumn(
            "RETRY_GID",
            F.sum(F.when(retry_point, 1).otherwise(0)).over(wind_seq_retry),
        )
    )
    columns = [
        "INSP_DT",
        "P_N",
        "MODEL",
        F.concat_ws("_", F.col("BCR_IP"), F.col("JIG")).alias("IPJIG"),
        "TESTCODE",
        "TEST_NAME",
        "PRE_TEST_SEQ",
        "TEST_SEQ",
        "RETEST_GID",
        "RETRY_GID",
        "RESULT",
        "PRE_ERROR_CODE",
        "ERROR_CODE",
        "FAILITEM",
        "TEST_TIME",
    ]
    retest_base_df = added_retryid_df.select(columns)

    # Retry analysis
    wind_cols_retry_analytic = ["P_N", "TESTCODE", "RETEST_GID", "RETRY_GID"]
    wind_seq_retry_analytic = Window.partitionBy(wind_cols_retry_analytic).orderBy(
        F.asc("INSP_DT")
    )
    wind_all_retry_analytic = (
        Window.partitionBy(wind_cols_retry_analytic)
        .orderBy(F.asc("INSP_DT"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    retry_analytic_df = _create_analytic_df(
        retest_base_df, wind_seq_retry_analytic, wind_all_retry_analytic
    )
    retry_result_df = _get_result_df(retry_analytic_df)

    # Retest analysis
    wind_cols_retest_analytic = ["P_N", "TESTCODE", "RETEST_GID"]
    wind_seq_retest_analytic = Window.partitionBy(wind_cols_retest_analytic).orderBy(
        F.asc("INSP_DT")
    )
    wind_all_retest_analytic = (
        Window.partitionBy(wind_cols_retest_analytic)
        .orderBy(F.asc("INSP_DT"))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    retest_analytic_df = _create_analytic_df(
        retest_base_df, wind_seq_retest_analytic, wind_all_retest_analytic
    )
    retest_result_df = _get_result_df(retest_analytic_df)

    return retry_result_df, retest_result_df


def _create_analytic_df(
    df: DataFrame, window_seq: Window, window_all: Window
) -> DataFrame:
    return (
        df.withColumn("RESULTS", F.collect_list("RESULT").over(window_all))
        .withColumn("DIST_RESULTS", F.collect_set("RESULT").over(window_all))
        .withColumn("DIST_IPJIG", F.collect_set("IPJIG").over(window_all))
        .withColumn("SEQ", F.row_number().over(window_seq))
        .withColumn(
            "IS_TRUE_ALRAM",
            F.array_contains("DIST_RESULTS", "FAIL")
            & (F.size("DIST_RESULTS") == 1)
            & (F.size("RESULTS") > 1),
        )
        .withColumn(
            "IS_FALSE_ALRAM",
            F.array_contains("RESULTS", "PASS") & F.array_contains("RESULTS", "FAIL"),
        )
        .withColumn(
            "IS_NO_ALRAM",
            F.array_contains("DIST_RESULTS", "PASS") & (F.size("DIST_RESULTS") == 1),
        )
        .withColumn(
            "IS_CHANGED_IPJIG",
            F.when((F.size("DIST_IPJIG") > 1), True).otherwise(False),
        )
        .withColumn("FIRST_FAILITEM", F.first("FAILITEM").over(window_all))
        .withColumn(
            "FIRST_FAILITEM",
            F.when(F.col("IS_NO_ALRAM") == True, F.col("TESTCODE")).otherwise(
                F.col("FIRST_FAILITEM")
            ),
        )
        .where(F.col("IS_TRUE_ALRAM") | F.col("IS_FALSE_ALRAM") | F.col("IS_NO_ALRAM"))
        .withColumn("TEST_NUM", F.size("RESULTS"))
        .withColumn("TEST_TIME_SUM", (F.sum("TEST_TIME").over(window_all)))
    )


def _get_result_df(analytic_df: DataFrame) -> DataFrame:
    result_df = (
        analytic_df.where(F.col("SEQ") == 1)
        .groupBy("MODEL", "TEST_NAME", "TESTCODE", "FIRST_FAILITEM")
        .agg(
            F.sum(F.when(F.col("IS_TRUE_ALRAM") == "true", 1).otherwise(0)).alias(
                "TRUE_ALRAM_INPUTNUM"
            ),
            F.sum(F.when(F.col("IS_FALSE_ALRAM") == "true", 1).otherwise(0)).alias(
                "FALSE_ALRAM_INPUTNUM"
            ),
            F.sum(F.when(F.col("IS_NO_ALRAM") == "true", 1).otherwise(0)).alias(
                "NO_ALRAM_INPUTNUM"
            ),
            F.sum(
                F.when(F.col("IS_TRUE_ALRAM") == "true", F.col("TEST_NUM")).otherwise(0)
            ).alias("TRUE_ALRAM_TESTNUM"),
            F.sum(
                F.when(F.col("IS_FALSE_ALRAM") == "true", F.col("TEST_NUM")).otherwise(
                    0
                )
            ).alias("FALSE_ALRAM_TESTNUM"),
            F.sum(
                F.when(F.col("IS_NO_ALRAM") == "true", F.col("TEST_NUM")).otherwise(0)
            ).alias("NO_ALRAM_TESTNUM"),
            F.sum(
                F.when(
                    F.col("IS_TRUE_ALRAM") == "true", F.col("TEST_TIME_SUM")
                ).otherwise(0)
            ).alias("TRUE_ALRAM_TESTTIME_SUM"),
            F.sum(
                F.when(
                    F.col("IS_FALSE_ALRAM") == "true", F.col("TEST_TIME_SUM")
                ).otherwise(0)
            ).alias("FALSE_ALRAM_TESTTIME_SUM"),
            F.sum(
                F.when(
                    F.col("IS_NO_ALRAM") == "true", F.col("TEST_TIME_SUM")
                ).otherwise(0)
            ).alias("NO_ALRAM_TESTTIME_SUM"),
            F.sum(F.when(F.col("IS_CHANGED_IPJIG") == "true", 1).otherwise(0)).alias(
                "CHANGED_IPJIG_SUM"
            ),
        )
        .withColumn(
            "TRUE_ALRAM_TESTTIME_AVG",
            F.round(F.col("TRUE_ALRAM_TESTTIME_SUM") / F.col("TRUE_ALRAM_TESTNUM"), 2),
        )
        .withColumn(
            "FALSE_ALRAM_TESTTIME_AVG",
            F.round(
                F.col("FALSE_ALRAM_TESTTIME_SUM") / F.col("FALSE_ALRAM_TESTNUM"), 2
            ),
        )
        .withColumn(
            "NO_ALRAM_TESTTIME_AVG",
            F.round(F.col("NO_ALRAM_TESTTIME_SUM") / F.col("NO_ALRAM_TESTNUM"), 2),
        )
        .withColumn(
            "TRUE_ALRAM_RATE",
            F.round(
                F.col("TRUE_ALRAM_INPUTNUM")
                / (F.col("TRUE_ALRAM_INPUTNUM") + F.col("FALSE_ALRAM_INPUTNUM")),
                2,
            ),
        )
    ).fillna(0)

    pass_df = result_df.where(result_df["FIRST_FAILITEM"] == result_df["TESTCODE"])
    pass_cols = ["MODEL", "TESTCODE"] + [
        col for col in result_df.columns if col.startswith("NO_ALRAM_")
    ]
    fail_df = result_df.where(result_df["FIRST_FAILITEM"] != result_df["TESTCODE"])
    fail_cols = [col for col in result_df.columns if not col.startswith("NO_ALRAM_")]

    final_df = fail_df.select(fail_cols).join(
        pass_df.select(pass_cols), on=["MODEL", "TESTCODE"], how="left"
    )
    return final_df
