from datetime import datetime
from pyspark.sql import DataFrame, functions as F, SparkSession


def date_generation(start_date: str, end_date: str) -> DataFrame:
    """
    Produce a DataFrame with one row per calendar day between start_date and end_date
    and helper columns 'start_date' and 'end_date' as DATE.
    """
    start_col = F.to_date(F.lit(start_date))
    end_col = F.to_date(F.lit(end_date))

    spark = SparkSession.builder.getOrCreate()

    return (
        spark.range(1)
        .select(F.explode(F.sequence(start_col, end_col, F.expr("interval 1 day"))).alias("date"))
        .withColumn("start_date", start_col)
        .withColumn("end_date", end_col)
    )


def feature_generation(
    dates_df: DataFrame,
    store_key: int,
    base_volume: int,
    covid_sales_impact_factor: float,
    covid_impact_start_date: str,
    covid_impact_end_date: str,
) -> DataFrame:
    """
    Enrich dates_df with calendar features and simple COVID progress metrics.
    """
    covid_start_lit = F.to_date(F.lit(covid_impact_start_date))

    try:
        dt_start = datetime.strptime(covid_impact_start_date, "%Y-%m-%d").date()
        dt_end = datetime.strptime(covid_impact_end_date, "%Y-%m-%d").date()
        covid_days_total = max((dt_end - dt_start).days, 1)  # avoid division by zero
    except Exception:
        covid_days_total = 1

    df = (
        dates_df
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("calendar_week", F.weekofyear("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "E"))
        .withColumn("is_weekend", F.col("day_of_week").isin(6, 7))
        .withColumn("store_key", F.lit(store_key))
        .withColumn("base_volume", F.lit(base_volume))
        .withColumn("covid_sales_impact_factor", F.lit(covid_sales_impact_factor))
        .withColumn(
            "months_since_start",
            (F.col("year") - F.year("start_date")) * 12 + (F.col("month") - F.month("start_date"))
        )
        .withColumn(
            "season",
            F.when(F.col("month").isin(12, 1, 2), "winter")
             .when(F.col("month").isin(3, 4, 5), "spring")
             .when(F.col("month").isin(6, 7, 8), "summer")
             .otherwise("autumn")
        )
        .withColumn(
            "days_since_covid_start",
            F.when(F.col("date") >= covid_start_lit, F.datediff(F.col("date"), covid_start_lit))
        )
        .withColumn(
            "covid_progress",
            F.when(
                F.lit(covid_days_total) > 0,
                F.col("days_since_covid_start").cast("double") / F.lit(float(covid_days_total))
            )
        )
    )

    return df
