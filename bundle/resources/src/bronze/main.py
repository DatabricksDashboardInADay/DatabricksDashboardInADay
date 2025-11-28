from functools import reduce
from typing import Dict, List
from pyspark.sql import DataFrame, functions as F
import argparse

from data_generation import date_generation, feature_generation
from impact_factors import (
    add_holiday_features,
    add_day_of_week_features,
    add_month_over_month_growth_features,
    add_covid_store_features,
    add_total_impact_features,
    add_random_noise_features,
    add_seasonality_features,
    remove_helper_columns,
    explode_by_simulated_volume,
    add_quantity_sold
)
from dimensions import (
    add_dim_product_key, add_dim_customer_key,
    create_dim_product, create_dim_customer,
    create_dim_store, create_dim_date
)


def build_store_dataframe(
    base_dates_df: DataFrame,
    store_config: Dict
) -> DataFrame:
    store_dates_df = base_dates_df.withColumn("date", F.to_date("date"))

    if store_config.get("store_start_date"):
        store_dates_df = store_dates_df.filter(
            F.col("date") >= F.to_date(F.lit(store_config["store_start_date"]))
        )

    if store_config.get("store_end_date"):
        store_dates_df = store_dates_df.filter(
            F.col("date") <= F.to_date(F.lit(store_config["store_end_date"]))
        )

    df = feature_generation(
        dates_df=store_dates_df,
        store_key=store_config["store_key"],
        base_volume=store_config["base_volume"],
        covid_sales_impact_factor=store_config["covid_sales_impact_factor"],
        covid_impact_start_date=store_config["covid_impact_start_date"],
        covid_impact_end_date=store_config["covid_impact_end_date"],
    )

    df = add_random_noise_features(df, stddev=store_config.get("noise_stddev", 0.05), seed=store_config.get("noise_seed", 1))
    df = add_holiday_features(df)
    df = add_day_of_week_features(
        df,
        mon_factor=store_config.get("mon_factor", 0.95),
        tue_factor=store_config.get("tue_factor", 1.0),
        wed_factor=store_config.get("wed_factor", 1.05),
        thu_factor=store_config.get("thu_factor", 1.05),
        fri_factor=store_config.get("fri_factor", 1.1),
        sat_factor=store_config.get("sat_factor", 1.2),
        sun_factor=store_config.get("sun_factor", 1.1),
    )
    df = add_month_over_month_growth_features(df, monthly_growth_pct=store_config.get("monthly_growth_pct", 1.0))
    df = add_covid_store_features(
        df,
        covid_start=store_config.get("covid_start", "2020-03-01"),
        covid_drop_depth=store_config.get("covid_drop_depth", 0.8),
        covid_drop_sigma=store_config.get("covid_drop_sigma", 100.0),
        covid_recovery_rate=store_config.get("covid_recovery_rate", 0.003),
        covid_recovery_start=store_config.get("covid_recovery_start", "2023-06-01"),
    )
    df = add_seasonality_features(
        df,
        winter_factor=store_config.get("winter_factor", 0.95),
        spring_factor=store_config.get("spring_factor", 1.0),
        summer_factor=store_config.get("summer_factor", 1.1),
        autumn_factor=store_config.get("autumn_factor", 0.98),
    )
    df = add_total_impact_features(df)
    df = explode_by_simulated_volume(df)
    df = add_quantity_sold(df)
    df = remove_helper_columns(df)

    return df


def save_df(df: DataFrame, name: str, path: str, fmt: str = "csv", single_file: bool = True, header: bool = True, mode: str = "overwrite") -> None:
    path = f"{path}/{name}"
    writer_df = df.coalesce(1) if single_file else df
    writer = writer_df.write.mode(mode)

    if fmt == "csv":
        writer.option("header", header).csv(path)
    elif fmt == "parquet":
        writer.parquet(path)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    print(f"✅ Saved {name} as {fmt} to {path}")


def main(catalog: str):

    volume_path = f"/Volumes/{catalog}/bronze/raw"

    base_dates_df = date_generation(start_date="2010-01-01", end_date="2025-12-31")

    store_configs: List[Dict] = [
        {
            "store_key": 1, "store_start_date": "2010-01-01", "store_end_date": "2025-12-31",
            "base_volume": 26, "covid_sales_impact_factor": "high",
            "covid_impact_start_date": "2020-03-01", "covid_impact_end_date": "2022-12-31",
            "noise_stddev": 0.04, "noise_seed": 1, "monthly_growth_pct": 0.2,
            "mon_factor": 0.95, "tue_factor": 1.05, "wed_factor": 1.10, "thu_factor": 1.10, "fri_factor": 1.15, "sat_factor": 1.10, "sun_factor": 1.0,
            "covid_start": "2020-03-15", "covid_drop_depth": 0.85, "covid_drop_sigma": 110.0, "covid_recovery_rate": 0.0025, "covid_recovery_start": "2022-01-01",
        },
        {
            "store_key": 2, "store_start_date": "2012-06-01", "store_end_date": "2025-12-31",
            "base_volume": 18, "covid_sales_impact_factor": "medium",
            "covid_impact_start_date": "2020-03-01", "covid_impact_end_date": "2022-06-30",
            "noise_stddev": 0.05, "noise_seed": 2, "monthly_growth_pct": 0.15,
            "mon_factor": 0.90, "tue_factor": 0.98, "wed_factor": 1.02, "thu_factor": 1.05, "fri_factor": 1.10, "sat_factor": 1.25, "sun_factor": 1.20,
            "covid_start": "2020-03-10", "covid_drop_depth": 0.65, "covid_drop_sigma": 90.0, "covid_recovery_rate": 0.0040, "covid_recovery_start": "2021-06-01",
        },
        {
            "store_key": 3, "store_start_date": "2015-03-01", "store_end_date": "2025-12-31",
            "base_volume": 22, "covid_sales_impact_factor": "high",
            "covid_impact_start_date": "2020-03-15", "covid_impact_end_date": "2022-09-30",
            "noise_stddev": 0.06, "noise_seed": 3, "monthly_growth_pct": 0.01,
            "mon_factor": 0.92, "tue_factor": 0.97, "wed_factor": 1.0, "thu_factor": 1.05, "fri_factor": 1.12, "sat_factor": 1.30, "sun_factor": 1.25,
            "covid_start": "2020-03-20", "covid_drop_depth": 0.80, "covid_drop_sigma": 100.0, "covid_recovery_rate": 0.0030, "covid_recovery_start": "2022-03-01",
        },
        {
            "store_key": 4, "store_start_date": "2018-09-01", "store_end_date": "2025-12-31",
            "base_volume": 21, "covid_sales_impact_factor": "very_high",
            "covid_impact_start_date": "2020-03-01", "covid_impact_end_date": "2023-03-31",
            "noise_stddev": 0.05, "noise_seed": 4, "monthly_growth_pct": 0.4,
            "mon_factor": 1.0, "tue_factor": 1.10, "wed_factor": 1.15, "thu_factor": 1.15, "fri_factor": 1.08, "sat_factor": 0.8, "sun_factor": 0.75,
            "covid_start": "2020-03-10", "covid_drop_depth": 0.90, "covid_drop_sigma": 130.0, "covid_recovery_rate": 0.0020, "covid_recovery_start": "2023-01-01",
        },
        {
            "store_key": 5, "store_start_date": "2021-01-15", "store_end_date": "2025-12-31",
            "base_volume": 14, "covid_sales_impact_factor": "low",
            "covid_impact_start_date": "2021-01-15", "covid_impact_end_date": "2023-12-31",
            "noise_stddev": 0.07, "noise_seed": 5, "monthly_growth_pct": 0.5,
            "mon_factor": 0.90, "tue_factor": 0.95, "wed_factor": 1.0, "thu_factor": 1.08, "fri_factor": 1.20, "sat_factor": 1.35, "sun_factor": 1.25,
            "covid_start": "2021-01-15", "covid_drop_depth": 0.90, "covid_drop_sigma": 130.0, "covid_recovery_rate": 0.0045, "covid_recovery_start": "2023-06-01",
        },
        {
            "store_key": 6, "store_start_date": "2021-01-01", "store_end_date": "2025-12-31",
            "base_volume": 11, "covid_sales_impact_factor": "low",
            "covid_impact_start_date": "2020-05-01", "covid_impact_end_date": "2023-01-01",
            "noise_stddev": 0.05, "noise_seed": 6, "monthly_growth_pct": 1.0,
            "mon_factor": 0.95, "tue_factor": 0.98, "wed_factor": 1.02, "thu_factor": 1.08, "fri_factor": 1.18, "sat_factor": 1.30, "sun_factor": 1.25,
            "covid_start": "2020-05-01", "covid_drop_depth": -0.08, "covid_drop_sigma": 240.0, "covid_recovery_rate": 0.0002, "covid_recovery_start": "2023-01-01",
        }
    ]

    store_dfs = [build_store_dataframe(base_dates_df, cfg) for cfg in store_configs]
    all_stores_df = reduce(lambda le, r: le.unionByName(r, allowMissingColumns=True), store_dfs) if len(store_dfs) > 1 else store_dfs[0]

    all_stores_df = add_dim_product_key(all_stores_df)
    all_stores_df = add_dim_customer_key(all_stores_df)

    dim_dfs = {
        "dim_product": create_dim_product(),
        "dim_customer": create_dim_customer(),
        "dim_store": create_dim_store(),
        "dim_date": create_dim_date(),
    }

    for name, df in dim_dfs.items():
        save_df(df, name, volume_path)

    fact_table_path = f"{volume_path}/fact_coffee_sales"
    all_stores_df.repartition("store_key").write.mode("overwrite").partitionBy("store_key").parquet(fact_table_path)
    print(f"✅ Fact write complete: saved partitioned Parquet files to {fact_table_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog')
    args = parser.parse_args()
    
    main(args.catalog)
