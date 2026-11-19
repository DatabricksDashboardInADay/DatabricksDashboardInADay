from datetime import date

from pyspark.sql import DataFrame, functions as F, SparkSession


def date_generation(start_date: date, end_date: date) -> DataFrame:
    """One row per calendar day between start_date and end_date (inclusive)."""
    spark = SparkSession.builder.getOrCreate()
    return spark.range(1).select(
        F.explode(F.sequence(F.lit(start_date), F.lit(end_date), F.expr("interval 1 day"))).alias("date")
    )


def feature_generation(dates_df: DataFrame, store_key: int, base_volume: int) -> DataFrame:
    """Attach the store and its base daily order volume to the date spine."""
    return (
        dates_df
        .withColumn("date", F.to_date("date"))
        .withColumn("store_key", F.lit(store_key))
        .withColumn("base_volume", F.lit(base_volume))
    )
