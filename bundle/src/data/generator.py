"""Generate the Sunny Bay Roastery source-system files (rolling 24-month sales window).

Usage (Databricks notebook or local Spark):

    from generator import generate_all
    generate_all(spark, "/Volumes/<catalog>/bronze/raw", prefix="")
"""

from datetime import date
from functools import reduce
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F

import reference_data as ref
import source_systems as src
from data_generation import date_generation, feature_generation
from impact_factors import (
    add_day_of_week_features,
    add_holiday_features,
    add_launch_ramp_features,
    add_month_over_month_growth_features,
    add_random_noise_features,
    add_seasonality_features,
    add_simulated_volume,
    explode_by_simulated_volume,
)


# Orders per day (before the volume factors are applied) and weekly patterns per store.
STORE_CONFIGS: List[Dict] = [
    {"store_key": 1, "base_volume": 62, "noise_stddev": 0.05, "noise_seed": 1, "monthly_growth_pct": 0.4,
     "dow": (0.95, 1.05, 1.10, 1.10, 1.15, 1.15, 1.00)},
    {"store_key": 2, "base_volume": 46, "noise_stddev": 0.06, "noise_seed": 2, "monthly_growth_pct": 0.5,
     "dow": (0.90, 0.98, 1.02, 1.05, 1.10, 1.25, 1.20)},
    {"store_key": 3, "base_volume": 52, "noise_stddev": 0.06, "noise_seed": 3, "monthly_growth_pct": 0.2,
     "dow": (0.92, 0.97, 1.00, 1.05, 1.12, 1.30, 1.15)},
    {"store_key": 4, "base_volume": 36, "noise_stddev": 0.07, "noise_seed": 4, "monthly_growth_pct": 0.8,
     "dow": (0.90, 0.95, 1.00, 1.08, 1.20, 1.35, 1.25)},
    # Frankfurt: office location, busy on weekdays, quiet at the weekend, opens inside the window
    {"store_key": 5, "base_volume": 44, "noise_stddev": 0.05, "noise_seed": 5, "monthly_growth_pct": 1.0,
     "dow": (1.05, 1.15, 1.20, 1.20, 1.05, 0.45, 0.35), "launch_ramp_months": 4},
    # online shop: open on public holidays, strong weekends
    {"store_key": 6, "base_volume": 10, "noise_stddev": 0.08, "noise_seed": 6, "monthly_growth_pct": 1.2,
     "dow": (0.95, 0.98, 1.02, 1.08, 1.12, 1.25, 1.30), "online": True},
]


def build_store_orders(cfg: Dict, opened_on: date, window_start: date, window_end: date,
                       holidays: List[date]) -> DataFrame:
    """Daily order volumes for one store, exploded to one row per order."""
    start = max(window_start, opened_on)
    df = feature_generation(date_generation(start, window_end), cfg["store_key"], cfg["base_volume"])
    df = add_random_noise_features(df, stddev=cfg["noise_stddev"], seed=cfg["noise_seed"])
    df = add_holiday_features(df, holidays, closed_on_holidays=not cfg.get("online", False))
    mon, tue, wed, thu, fri, sat, sun = cfg["dow"]
    df = add_day_of_week_features(df, mon, tue, wed, thu, fri, sat, sun)
    df = add_month_over_month_growth_features(df, cfg["monthly_growth_pct"], growth_start=window_start)
    df = add_seasonality_features(df)
    df = add_launch_ramp_features(df, cfg.get("launch_ramp_months", 0), opened_on)
    df = add_simulated_volume(df)
    return explode_by_simulated_volume(df).select("store_key", "date", "order_seq")


def build_sources(as_of: Optional[date] = None) -> Dict[str, DataFrame]:
    """Build all source-system DataFrames. Nothing is written."""
    as_of = as_of or date.today()
    window_start, window_end = src.as_of_dates(as_of)
    frankfurt_opened_on = ref.first_of_month(window_start, 8)
    store_rows = ref.stores(frankfurt_opened_on)
    opened = {r[0]: r[5] for r in store_rows}
    employee_rows = ref.employees(store_rows)
    holidays = [d for d, _ in ref.german_public_holidays(window_start.year, window_end.year)]

    products = src.erp_products(as_of)
    prices = src.erp_product_prices(as_of, window_start)
    loyalty_cards = src.crm_loyalty_cards(store_rows, window_start, window_end).cache()
    crm = src.crm_customers(store_rows, loyalty_cards, window_start, as_of).cache()
    crm_current = crm.groupBy("customer_id").agg(F.max_by(F.struct("email", "first_name", "last_name"), "updated_at").alias("c")) \
                     .select("customer_id", "c.*")
    accounts = src.web_accounts(crm_current, window_start, as_of).cache()

    orders = reduce(
        DataFrame.unionByName,
        [build_store_orders(cfg, opened[cfg["store_key"]], window_start, window_end, holidays) for cfg in STORE_CONFIGS],
    )
    orders = src.enrich_orders(orders, src.cashiers_by_store(employee_rows), loyalty_cards, window_end).cache()
    lines = src.order_lines(orders, prices, products).cache()

    tables = {
        **src.pos_tables(orders, lines),
        "web_orders": src.web_orders(orders, lines, accounts),
        "web_accounts": accounts.orderBy("account_id"),
        "crm_customers": crm.orderBy("customer_id", "updated_at"),
        "crm_loyalty_cards": loyalty_cards.orderBy("card_no"),
        "erp_products": products,
        "erp_product_categories": src.erp_product_categories(),
        "erp_product_prices": prices,
        "erp_stores": src.erp_stores(store_rows),
        "hr_employees": src.hr_employees(store_rows),
        "ref_public_holidays": src.ref_public_holidays(window_start, window_end),
    }
    return tables


# File format per source table (the web shop delivers nested JSON, everything else CSV).
FORMATS = {"web_orders": "json"}


def save_df(df: DataFrame, name: str, path: str, fmt: str = "csv") -> None:
    target = f"{path}/{name}"
    writer = df.coalesce(1).write.mode("overwrite")
    if fmt == "csv":
        (writer.option("header", True)
               .option("ignoreLeadingWhiteSpace", False)
               .option("ignoreTrailingWhiteSpace", False)
               .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
               .csv(target))
    elif fmt == "json":
        writer.json(target)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    print(f"✅ Saved {name} as {fmt} to {target}")


def generate_all(spark: SparkSession, volume_path: str, prefix: str = "", as_of: Optional[date] = None) -> Dict[str, DataFrame]:
    """Build all source tables and write them as files to ``volume_path``."""
    tables = build_sources(as_of)
    for name, df in tables.items():
        save_df(df, f"{prefix}{name}", volume_path, FORMATS.get(name, "csv"))
    return tables
