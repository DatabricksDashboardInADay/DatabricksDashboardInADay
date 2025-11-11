from functools import reduce
from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from data_generation import date_generation, feature_generation
from impact_factors import (
    add_holiday_features,
    add_day_of_week_features,
    add_month_over_month_growth_features,
    add_covid_store_features,
    add_total_impact_features,
    add_random_noise_features,
    add_seasonality_features,
)
from dimensions import add_dim_product_key, add_dim_customer_key, create_dim_product, create_dim_customer, create_dim_store, create_dim_date

def build_store_dataframe(
    spark: SparkSession,
    base_dates_df: DataFrame,
    store_config: Dict,
) -> DataFrame:
    """
    Build the full feature DataFrame for a single store.
    store_config holds all store-specific parameters, including per-store
    start/end date (store open/close).
    """

    # 🔹 0) Derive per-store date range from the global date grid
    store_start_date = store_config.get("store_start_date")
    store_end_date = store_config.get("store_end_date")

    store_dates_df = base_dates_df

    # Base df should already have a 'date' column; be explicit and cast to date.
    store_dates_df = store_dates_df.withColumn("date", F.to_date("date"))

    if store_start_date is not None:
        store_dates_df = store_dates_df.filter(
            F.col("date") >= F.to_date(F.lit(store_start_date))
        )

    if store_end_date is not None:
        store_dates_df = store_dates_df.filter(
            F.col("date") <= F.to_date(F.lit(store_end_date))
        )

    # 1️⃣ Start from per-store date range but add store-specific features
    df = feature_generation(
        dates_df=store_dates_df,
        store_key=store_config["store_key"],
        base_volume=store_config["base_volume"],
        covid_sales_impact_factor=store_config["covid_sales_impact_factor"],
        covid_impact_start_date=store_config["covid_impact_start_date"],
        covid_impact_end_date=store_config["covid_impact_end_date"],
    )

    # 2️⃣ Random noise
    df = add_random_noise_features(
        df=df,
        spark=spark,
        stddev=store_config.get("noise_stddev", 0.05),
        seed=store_config.get("noise_seed", 1),
    )

    # 3️⃣ Holidays
    df = add_holiday_features(
        df=df,
        spark=spark,
    )

    # 4️⃣ Day-of-week pattern
    df = add_day_of_week_features(
        df=df,
        spark=spark,
        mon_factor=store_config.get("mon_factor", 0.95),
        tue_factor=store_config.get("tue_factor", 1.00),
        wed_factor=store_config.get("wed_factor", 1.05),
        thu_factor=store_config.get("thu_factor", 1.05),
        fri_factor=store_config.get("fri_factor", 1.10),
        sat_factor=store_config.get("sat_factor", 1.20),
        sun_factor=store_config.get("sun_factor", 1.10),
    )

    # 5️⃣ Organic MoM growth
    df = add_month_over_month_growth_features(
        df=df,
        spark=spark,
        monthly_growth_pct=store_config.get("monthly_growth_pct", 1.0),
    )

    # 6️⃣ COVID curve
    df = add_covid_store_features(
        df=df,
        spark=spark,
        covid_start=store_config.get("covid_start", "2020-03-01"),
        covid_drop_depth=store_config.get("covid_drop_depth", 0.8),
        covid_drop_sigma=store_config.get("covid_drop_sigma", 100.0),
        covid_recovery_rate=store_config.get("covid_recovery_rate", 0.003),
        covid_recovery_start=store_config.get("covid_recovery_start", "2023-06-01"),
    )

    # 7️⃣ Seasonality (winter/spring/summer/autumn)
    df = add_seasonality_features(
        df=df,
        spark=spark,
        winter_factor=store_config.get("winter_factor", 0.95),
        spring_factor=store_config.get("spring_factor", 1.00),
        summer_factor=store_config.get("summer_factor", 1.10),
        autumn_factor=store_config.get("autumn_factor", 0.98),
    )

    # 8️⃣ Total impact factor (+ simulated_volume via base_volume inside add_total_impact_features)
    df = add_total_impact_features(
        df=df,
        spark=spark,
        factor_cols=[
            "random_impact_factor",
            "holiday_impact_factor",
            "day_of_week_impact_factor",
            "organic_mom_growth_factor",
            "covid_impact_factor",
            "seasonality_impact_factor",
        ],
        output_col="total_impact_factor",
        # base_volume is already in df from feature_generation,
        # and add_total_impact_features will create "simulated_volume"
    )

    return df


# Base path for your Databricks volume
VOLUME_BASE_PATH = "dbfs:/Volumes/main/default/synthetic_data_volume"
spark.sql("CREATE VOLUME if not exists main.default.synthetic_data_volume")

def save_df(
    df: DataFrame,
    name: str,
    base_path: str = VOLUME_BASE_PATH,
    fmt: str = "csv",
    single_file: bool = True,
    header: bool = True,
    mode: str = "overwrite",
) -> None:
    """
    Generic saver for a Spark DataFrame.

    - fmt: "csv" or "parquet"
    - single_file: coalesce(1) if True (nice for small synthetic data)
    - header: only used for CSV
    """

    path = f"{base_path}/{name}"

    writer_df = df.coalesce(1) if single_file else df

    writer = writer_df.write.mode(mode)

    if fmt == "csv":
        writer.option("header", header).csv(path)
    elif fmt == "parquet":
        writer.parquet(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    print(f"✅ Saved {name} as {fmt} to {path}")

def main():
    spark = SparkSession.builder.getOrCreate()

    # Shared date range for all stores (global min/max)
    base_dates_df = date_generation(
        spark=spark,
        start_date="2010-01-01",   # 👈 match earliest store_start_date
        end_date="2025-12-31",     # 👈 match latest store_end_date
    )  # .cache() if reused a lot

    # Define store-specific settings (unchanged from your last version)
    store_configs: List[Dict] = [
        {
            # Flagship store, downtown, open since 2010, mature + stable
            "store_key": 1,
            "store_start_date": "2010-01-01",
            "store_end_date": "2025-12-31",

            "base_volume": 260,                       # high base volume
            "covid_sales_impact_factor": "high",     # heavy COVID impact (downtown)
            "covid_impact_start_date": "2020-03-01",
            "covid_impact_end_date": "2022-12-31",

            "noise_stddev": 0.04,                    # relatively stable
            "noise_seed": 1,
            "monthly_growth_pct": 0.5,               # mature store, slow growth

            # day-of-week factors: strong weekday commuter pattern
            "mon_factor": 0.95,
            "tue_factor": 1.05,
            "wed_factor": 1.10,
            "thu_factor": 1.10,
            "fri_factor": 1.15,
            "sat_factor": 1.10,
            "sun_factor": 1.00,

            # covid curve tuning
            "covid_start": "2020-03-15",
            "covid_drop_depth": 0.85,                # up to ~85% drop at peak
            "covid_drop_sigma": 110.0,
            "covid_recovery_rate": 0.0025,           # relatively slow recovery
            "covid_recovery_start": "2022-01-01",
        },
        {
            # Residential neighborhood store, opens 2012, steady grower, less COVID impact
            "store_key": 2,
            "store_start_date": "2012-06-01",
            "store_end_date": "2025-12-31",

            "base_volume": 180,
            "covid_sales_impact_factor": "medium",
            "covid_impact_start_date": "2020-03-01",
            "covid_impact_end_date": "2022-06-30",

            "noise_stddev": 0.05,
            "noise_seed": 2,
            "monthly_growth_pct": 0.8,               # decent long-term growth

            # weekend brunch + family traffic
            "mon_factor": 0.90,
            "tue_factor": 0.98,
            "wed_factor": 1.02,
            "thu_factor": 1.05,
            "fri_factor": 1.10,
            "sat_factor": 1.25,
            "sun_factor": 1.20,

            # covid curve tuning: smaller dip, quicker recovery
            "covid_start": "2020-03-10",
            "covid_drop_depth": 0.65,
            "covid_drop_sigma": 90.0,
            "covid_recovery_rate": 0.0040,           # faster recovery
            "covid_recovery_start": "2021-06-01",
        },
        {
            # Mall store, opened 2015, good growth but COVID hits hard
            "store_key": 3,
            "store_start_date": "2015-03-01",
            "store_end_date": "2025-12-31",

            "base_volume": 220,
            "covid_sales_impact_factor": "high",
            "covid_impact_start_date": "2020-03-15",
            "covid_impact_end_date": "2022-09-30",

            "noise_stddev": 0.06,
            "noise_seed": 3,
            "monthly_growth_pct": 1.0,               # ~1% per month growth

            # strong weekend shopping pattern
            "mon_factor": 0.92,
            "tue_factor": 0.97,
            "wed_factor": 1.00,
            "thu_factor": 1.05,
            "fri_factor": 1.12,
            "sat_factor": 1.30,
            "sun_factor": 1.25,

            # covid curve tuning: deep dip, moderate recovery speed
            "covid_start": "2020-03-20",
            "covid_drop_depth": 0.80,
            "covid_drop_sigma": 100.0,
            "covid_recovery_rate": 0.0030,
            "covid_recovery_start": "2022-03-01",
        },
        {
            # Office-district store, opened 2018, high pre-COVID growth, badly hit then slow recovery
            "store_key": 4,
            "store_start_date": "2018-09-01",
            "store_end_date": "2025-12-31",

            "base_volume": 210,
            "covid_sales_impact_factor": "very_high",
            "covid_impact_start_date": "2020-03-01",
            "covid_impact_end_date": "2023-03-31",

            "noise_stddev": 0.05,
            "noise_seed": 4,
            "monthly_growth_pct": 1.3,               # strong early growth pre-COVID

            # very strong weekday / commuter, weak weekends
            "mon_factor": 1.00,
            "tue_factor": 1.10,
            "wed_factor": 1.15,
            "thu_factor": 1.15,
            "fri_factor": 1.08,
            "sat_factor": 0.80,
            "sun_factor": 0.75,

            # covid curve tuning: very deep dip, slow recovery (WFH impact)
            "covid_start": "2020-03-10",
            "covid_drop_depth": 0.90,                # down to ~10% of normal
            "covid_drop_sigma": 130.0,
            "covid_recovery_rate": 0.0020,           # slow recovery
            "covid_recovery_start": "2023-01-01",
        },
        {
            # Newer trendy store, opened 2021, small but fast-growing, moderate COVID impact
            "store_key": 5,
            "store_start_date": "2021-01-15",
            "store_end_date": "2025-12-31",

            "base_volume": 140,
            "covid_sales_impact_factor": "low",
            "covid_impact_start_date": "2021-01-15",  # opens during pandemic
            "covid_impact_end_date": "2023-12-31",

            "noise_stddev": 0.07,                    # more volatile early-stage store
            "noise_seed": 5,
            "monthly_growth_pct": 1.6,               # aggressive growth

            # very strong weekend + evening social traffic
            "mon_factor": 0.90,
            "tue_factor": 0.95,
            "wed_factor": 1.00,
            "thu_factor": 1.08,
            "fri_factor": 1.20,
            "sat_factor": 1.35,
            "sun_factor": 1.25,

            # covid curve tuning: smaller dip, fairly quick recovery (opens already in COVID world)
            "covid_start": "2021-01-15",
            "covid_drop_depth": 0.60,
            "covid_drop_sigma": 80.0,
            "covid_recovery_rate": 0.0045,
            "covid_recovery_start": "2022-06-01",
        },
       {
        # Only shop / delivery-first concept, opened in COVID, benefits from it
        "store_key": 6,
        "store_start_date": "2020-01-01",
        "store_end_date": "2025-12-31",

        # Slightly lower base volume so it doesn't feel huge right away
        "base_volume": 110,
        "covid_sales_impact_factor": "low",
        "covid_impact_start_date": "2020-05-01",
        "covid_impact_end_date": "2023-01-01",

        # Still a bit noisy, but not crazy
        "noise_stddev": 0.05,
        "noise_seed": 6,

        # 🔑 Much gentler long-term growth
        # 1.0% per month → ~1.8x over 5 years instead of ~4.3x
        "monthly_growth_pct": 1.0,

        # pattern: strong evenings & weekends (WFH & neighborhood)
        "mon_factor": 0.95,
        "tue_factor": 0.98,
        "wed_factor": 1.02,
        "thu_factor": 1.08,
        "fri_factor": 1.18,
        "sat_factor": 1.30,
        "sun_factor": 1.25,

        # covid curve tuning: still positive, but less aggressive and more spread out
        "covid_start": "2020-05-01",
        "covid_drop_depth": -0.08,      # ~8% boost at peak instead of 15%
        "covid_drop_sigma": 240.0,      # wider bump → smoother, less spiky effect
        "covid_recovery_rate": 0.0002,  # very slow normalization
        "covid_recovery_start": "2023-01-01",

        # optional: seasonality tweaks if you want (uses defaults otherwise)
        # "winter_factor": 0.98,
        # "spring_factor": 1.05,
        # "summer_factor": 1.10,
        # "autumn_factor": 1.00,
    }
    ]

    # Build a dataframe per store
    store_dfs = [
        build_store_dataframe(
            spark=spark,
            base_dates_df=base_dates_df,
            store_config=config,
        )
        for config in store_configs
    ]

    # Union all stores into a single DF
    if len(store_dfs) == 1:
        all_stores_df = store_dfs[0]
    else:
        all_stores_df = reduce(
            lambda left, right: left.unionByName(right, allowMissingColumns=True),
            store_dfs,
        )

    # 👉 Apply dimension key assignment functions HERE
    all_stores_df = add_dim_product_key(df=all_stores_df)
    all_stores_df = add_dim_customer_key(df=all_stores_df)

    spark.sql("DROP TABLE IF EXISTS main.default.synthetic_data")
    all_stores_df.write.mode("overwrite").saveAsTable("main.default.synthetic_data")

    # Create store dimensions
    dim_product  = create_dim_product(spark)
    dim_customer = create_dim_customer(spark)
    # dim_store    = create_dim_store(spark)
    dim_date    = create_dim_date(spark)

    # ---------------------------------------------------------
    # Use the helper in a loop for dimensions (CSV)
    # ---------------------------------------------------------

    dim_dfs = {
        "dim_product": dim_product,
        "dim_customer": dim_customer,
        # "dim_store": dim_store,
        "dim_date": dim_date,
    }

    for name, df in dim_dfs.items():
        save_df(df, name=name, fmt="csv", single_file=True, header=True)

    # ---------------------------------------------------------
    # Save fact table as Parquet
    # ---------------------------------------------------------

    save_df(
        all_stores_df,
        name="fact_all_stores",
        fmt="parquet",
        single_file=False,  # usually fine to keep as multiple files
        header=False,       # not used for parquet, just to be explicit
    )

if __name__ == "__main__":
    main()