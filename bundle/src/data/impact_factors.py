"""Multiplicative volume factors used to simulate daily order volumes per store.

simulated orders per day = base_volume x random noise x holiday x day-of-week
                           x organic growth x seasonality x launch ramp
"""

import math as _math
from datetime import date
from typing import Iterable, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession, functions as F, types as T


# ---------------------------------------------------------------------------
# Partition-independent deterministic random helpers (hash-based)
# ---------------------------------------------------------------------------

def det_uniform(seed: int, *id_cols: Column) -> Column:
    """Partition-independent uniform [0, 1) value, deterministic per row."""
    h = F.xxhash64(*id_cols, F.lit(seed))
    return F.pmod(h, F.lit(1000003)).cast("double") / F.lit(1000003.0)


def det_normal(seed: int, *id_cols: Column) -> Column:
    """Partition-independent standard-normal value, deterministic per row (Box-Muller)."""
    u1 = F.greatest(det_uniform(seed, *id_cols), F.lit(1e-10))
    u2 = det_uniform(seed + 7919, *id_cols)          # offset by a prime
    return F.sqrt(F.lit(-2.0) * F.log(u1)) * F.cos(F.lit(2.0 * _math.pi) * u2)


def weighted_choice(u: Column, choices: List[tuple]) -> Column:
    """Pick a value from ``[(value, weight), ...]`` using a uniform draw ``u``."""
    total = float(sum(w for _, w in choices))
    expr = None
    cumulative = 0.0
    for value, weight in choices[:-1]:
        cumulative += weight / total
        expr = (F.when(u < F.lit(cumulative), F.lit(value)) if expr is None
                else expr.when(u < F.lit(cumulative), F.lit(value)))
    last = F.lit(choices[-1][0])
    return last if expr is None else expr.otherwise(last)


# ---------------------------------------------------------------------------
# Volume factors
# ---------------------------------------------------------------------------

def add_random_noise_features(df: DataFrame, stddev: float = 0.05, seed: int = 1) -> DataFrame:
    """random_impact_factor ~ N(1, stddev), clamped to [0.1, 2.0]."""
    z = det_normal(seed, F.col("store_key"), F.col("date"))
    factor = F.lit(1.0) + F.lit(float(stddev)) * z
    factor = F.least(F.greatest(factor, F.lit(0.1)), F.lit(2.0))
    return df.withColumn("random_impact_factor", factor.cast("double"))


def add_holiday_features(df: DataFrame, holiday_dates: Iterable[date], closed_on_holidays: bool = True) -> DataFrame:
    """holiday_impact_factor: 0.0 on public holidays when the store is closed, 1.0 otherwise."""
    if not closed_on_holidays:
        return df.withColumn("holiday_impact_factor", F.lit(1.0))

    spark = SparkSession.builder.getOrCreate()
    holidays_df = spark.createDataFrame(
        [(d,) for d in sorted(set(holiday_dates))],
        schema=T.StructType([T.StructField("date", T.DateType(), False)]),
    ).withColumn("_is_holiday", F.lit(True))

    return (
        df.join(F.broadcast(holidays_df), on="date", how="left")
          .withColumn("holiday_impact_factor", F.when(F.col("_is_holiday"), F.lit(0.0)).otherwise(F.lit(1.0)))
          .drop("_is_holiday")
    )


def add_day_of_week_features(
    df: DataFrame,
    mon_factor: float = 0.95,
    tue_factor: float = 1.00,
    wed_factor: float = 1.05,
    thu_factor: float = 1.05,
    fri_factor: float = 1.10,
    sat_factor: float = 1.20,
    sun_factor: float = 1.10,
) -> DataFrame:
    dow = F.date_format("date", "E")
    return df.withColumn(
        "day_of_week_impact_factor",
        F.when(dow == "Mon", F.lit(float(mon_factor)))
         .when(dow == "Tue", F.lit(float(tue_factor)))
         .when(dow == "Wed", F.lit(float(wed_factor)))
         .when(dow == "Thu", F.lit(float(thu_factor)))
         .when(dow == "Fri", F.lit(float(fri_factor)))
         .when(dow == "Sat", F.lit(float(sat_factor)))
         .otherwise(F.lit(float(sun_factor)))
    )


def add_month_over_month_growth_features(df: DataFrame, monthly_growth_pct: float, growth_start: date) -> DataFrame:
    """organic_mom_growth_factor = (1 + pct)^(months since growth_start)."""
    monthly_rate = float(monthly_growth_pct) / 100.0
    months = F.floor(
        F.months_between(F.trunc(F.col("date"), "month"), F.trunc(F.lit(growth_start), "month"))
    ).cast("int")
    return df.withColumn(
        "organic_mom_growth_factor",
        F.when(months < 0, F.lit(1.0)).otherwise(F.pow(F.lit(1.0 + monthly_rate), months)),
    )


def add_seasonality_features(
    df: DataFrame,
    winter_factor: float = 0.95,
    spring_factor: float = 1.00,
    summer_factor: float = 1.10,
    autumn_factor: float = 0.98,
) -> DataFrame:
    month_col = F.month("date")
    return df.withColumn(
        "seasonality_impact_factor",
        F.when(month_col.isin(12, 1, 2), F.lit(float(winter_factor)))
         .when(month_col.isin(3, 4, 5), F.lit(float(spring_factor)))
         .when(month_col.isin(6, 7, 8), F.lit(float(summer_factor)))
         .otherwise(F.lit(float(autumn_factor)))
    )


def add_launch_ramp_features(df: DataFrame, ramp_months: int = 0, opened_on: Optional[date] = None) -> DataFrame:
    """launch_ramp_factor ramps linearly from 1/ramp_months to 1.0 after a store opens."""
    if not ramp_months or ramp_months <= 0 or not opened_on:
        return df.withColumn("launch_ramp_factor", F.lit(1.0))

    months_since_open = F.floor(
        F.months_between(F.trunc(F.col("date"), "month"), F.trunc(F.lit(opened_on), "month"))
    ).cast("int")
    ramp = F.least((months_since_open + F.lit(1)).cast("double") / F.lit(float(ramp_months)), F.lit(1.0))
    return df.withColumn("launch_ramp_factor", F.when(months_since_open < 0, F.lit(1.0)).otherwise(ramp))


FACTOR_COLUMNS = [
    "random_impact_factor",
    "holiday_impact_factor",
    "day_of_week_impact_factor",
    "organic_mom_growth_factor",
    "seasonality_impact_factor",
    "launch_ramp_factor",
]


def add_simulated_volume(df: DataFrame) -> DataFrame:
    """simulated_volume = base_volume x product of all factor columns (rounded down)."""
    total = F.lit(1.0)
    for c in FACTOR_COLUMNS:
        if c in df.columns:
            total = total * F.coalesce(F.col(c), F.lit(1.0))
    return df.withColumn("simulated_volume", (F.col("base_volume").cast("double") * total).cast("int"))


def explode_by_simulated_volume(df: DataFrame, seq_col: str = "order_seq") -> DataFrame:
    """One output row per simulated order; ``seq_col`` is the 1-based order number of the day."""
    vol = F.coalesce(F.col("simulated_volume"), F.lit(0))
    return df.withColumn(
        seq_col, F.explode(F.when(vol >= 1, F.sequence(F.lit(1), vol)))
    )


def quantity_expr(u: Column, u2: Column, p1: float, p2: float, p3: float) -> Column:
    """Quantity per order line: 1, 2, 3 with the given probabilities, otherwise 4 or 5."""
    return (
        F.when(u < p1, F.lit(1))
         .when(u < p1 + p2, F.lit(2))
         .when(u < p1 + p2 + p3, F.lit(3))
         .otherwise(F.floor(u2 * 2 + 4).cast("int"))
    )
