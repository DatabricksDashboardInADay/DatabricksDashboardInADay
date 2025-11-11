from pyspark.sql import DataFrame, functions as F

def date_generation(spark, start_date, end_date):
  dates_df = (
      spark.range(1)
      .select(
          F.explode(
              F.sequence(
                  F.to_date(F.lit(start_date)),
                  F.to_date(F.lit(end_date)),
                  F.expr("interval 1 day")
              )
          ).alias("date")
      ).withColumn("start_date", F.to_date(F.lit(start_date)))
      .withColumn("end_date", F.to_date(F.lit(end_date)))
  )
  return(dates_df)

def feature_generation(
    dates_df: DataFrame,
    store_key: int,
    base_volume: int,
    covid_sales_impact_factor: float,
    covid_impact_start_date: str,
    covid_impact_end_date: str
) -> DataFrame:
    covid_start_lit = F.to_date(F.lit(covid_impact_start_date))
    covid_end_lit = F.to_date(F.lit(covid_impact_end_date))
    covid_days_total = F.datediff(covid_end_lit, covid_start_lit)

    return (
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
    ).withColumn(
            "season",
            F.when(F.col("month").isin(12, 1, 2), "winter")
             .when(F.col("month").isin(3, 4, 5), "spring")
             .when(F.col("month").isin(6, 7, 8), "summer")
             .otherwise("autumn")
        )        .withColumn(
            "days_since_covid_start",
            F.when(
                F.col("date") >= covid_start_lit,
                F.datediff(F.col("date"), covid_start_lit)
            )
        )
        # Normalized progress (0–1)
        .withColumn(
            "covid_progress",
            (
                F.col("days_since_covid_start").cast("double") /
                F.lit(covid_days_total.cast("double"))
            )
        )

    )



    #         .withColumn(
    #     "organic_growth_factor",
    #     F.lit(1.0) + F.col("months_since_start") * F.lit(mom_growth)
    # )