from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from typing import List, Optional

def add_random_noise_features(
    df: DataFrame,
    spark: SparkSession,
    stddev: float = 0.05,          # 5% standard deviation
    seed: Optional[int] = 1,       # for reproducibility
) -> DataFrame:
    """
    Adds one column:
      - random_impact_factor

    This is a multiplicative noise term intended to make the simulated
    sales more realistic. It is centered around 1.0.

    Model
    -----
    - Draw Z ~ N(0, 1) per row using randn()
    - noise = stddev * Z
    - factor_raw = 1.0 + noise
    - Clamp to [0.1, 2.0] to avoid pathological values
    """

    # Draw N(0,1) noise per row
    if seed is not None:
        z = F.randn(seed)
    else:
        z = F.randn()

    noise = F.lit(float(stddev)) * z
    factor_raw = F.lit(1.0) + noise

    # Clamp factor to a safe range
    factor = (
        F.when(factor_raw < F.lit(0.1), F.lit(0.1))   # minimum 0.1
         .when(factor_raw > F.lit(2.0), F.lit(2.0))   # maximum 2.0
         .otherwise(factor_raw)
    )

    df = df.withColumn("random_impact_factor", factor)

    return df


def add_holiday_features(
    df: DataFrame,
    spark: SparkSession
    ) -> DataFrame:
    """
    Adds one column:
      - holiday_impact_factor: 0.0 on holidays, 1.0 otherwise

    The list of holidays is defined directly inside the function (no external CSV needed).
    """

    # Embedded CSV data as a Python multiline string
    holidays_csv = """date,holiday
2010-01-01,New Year’s Day
2010-04-04,Easter
2010-05-31,Memorial Day
2010-07-04,Fourth of July
2010-09-06,Labor Day
2010-11-25,Thanksgiving
2010-12-25,Christmas
2011-01-01,New Year’s Day
2011-04-24,Easter
2011-05-30,Memorial Day
2011-07-04,Fourth of July
2011-09-05,Labor Day
2011-11-24,Thanksgiving
2011-12-25,Christmas
2012-01-01,New Year’s Day
2012-04-08,Easter
2012-05-28,Memorial Day
2012-07-04,Fourth of July
2012-09-03,Labor Day
2012-11-22,Thanksgiving
2012-12-25,Christmas
2013-01-01,New Year’s Day
2013-03-31,Easter
2013-05-27,Memorial Day
2013-07-04,Fourth of July
2013-09-02,Labor Day
2013-11-28,Thanksgiving
2013-12-25,Christmas
2014-01-01,New Year’s Day
2014-04-20,Easter
2014-05-26,Memorial Day
2014-07-04,Fourth of July
2014-09-01,Labor Day
2014-11-27,Thanksgiving
2014-12-25,Christmas
2015-01-01,New Year’s Day
2015-04-05,Easter
2015-05-25,Memorial Day
2015-07-04,Fourth of July
2015-09-07,Labor Day
2015-11-26,Thanksgiving
2015-12-25,Christmas
2016-01-01,New Year’s Day
2016-03-27,Easter
2016-05-30,Memorial Day
2016-07-04,Fourth of July
2016-09-05,Labor Day
2016-11-24,Thanksgiving
2016-12-25,Christmas
2017-01-01,New Year’s Day
2017-04-16,Easter
2017-05-29,Memorial Day
2017-07-04,Fourth of July
2017-09-04,Labor Day
2017-11-23,Thanksgiving
2017-12-25,Christmas
2018-01-01,New Year’s Day
2018-04-01,Easter
2018-05-28,Memorial Day
2018-07-04,Fourth of July
2018-09-03,Labor Day
2018-11-22,Thanksgiving
2018-12-25,Christmas
2019-01-01,New Year’s Day
2019-04-21,Easter
2019-05-27,Memorial Day
2019-07-04,Fourth of July
2019-09-02,Labor Day
2019-11-28,Thanksgiving
2019-12-25,Christmas
2020-01-01,New Year’s Day
2020-04-12,Easter
2020-05-25,Memorial Day
2020-07-04,Fourth of July
2020-09-07,Labor Day
2020-11-26,Thanksgiving
2020-12-25,Christmas
2021-01-01,New Year’s Day
2021-04-04,Easter
2021-05-31,Memorial Day
2021-07-04,Fourth of July
2021-09-06,Labor Day
2021-11-25,Thanksgiving
2021-12-25,Christmas
2022-01-01,New Year’s Day
2022-04-17,Easter
2022-05-30,Memorial Day
2022-07-04,Fourth of July
2022-09-05,Labor Day
2022-11-24,Thanksgiving
2022-12-25,Christmas
2023-01-01,New Year’s Day
2023-04-09,Easter
2023-05-29,Memorial Day
2023-07-04,Fourth of July
2023-09-04,Labor Day
2023-11-23,Thanksgiving
2023-12-25,Christmas
2024-01-01,New Year’s Day
2024-03-31,Easter
2024-05-27,Memorial Day
2024-07-04,Fourth of July
2024-09-02,Labor Day
2024-11-28,Thanksgiving
2024-12-25,Christmas
2025-01-01,New Year’s Day
2025-04-20,Easter
2025-05-26,Memorial Day
2025-07-04,Fourth of July
2025-09-01,Labor Day
2025-11-27,Thanksgiving
2025-12-25,Christmas
"""

    # Split and parse CSV into a list of tuples
    lines = holidays_csv.strip().split("\n")[1:]  # skip header
    holidays_data = [tuple(line.split(",", 1)) for line in lines]  # (date, holiday)

    # Create a Spark DataFrame of holidays
    holidays_df = spark.createDataFrame(holidays_data, ["date", "holiday"]) \
                       .withColumn("date", F.to_date("date"))

    # Prepare the main df
    df = df.withColumn("date", F.to_date("date"))

    # Join to mark holidays
    df = df.join(
        F.broadcast(
            holidays_df
            .select("date")
            .distinct()
            .withColumn("is_holiday", F.lit(True))
        ),
        on="date",
        how="left"
    )

    # Add impact factor: 0.0 on holidays, 1.0 otherwise
    df = df.withColumn(
        "holiday_impact_factor",
        F.when(F.col("is_holiday") == True, F.lit(0.0)).otherwise(F.lit(1.0))
    ).drop("is_holiday")

    return df


def add_day_of_week_features(
    df: DataFrame,
    spark: SparkSession,
    mon_factor: float = 0.95,
    tue_factor: float = 1.00,
    wed_factor: float = 1.05,
    thu_factor: float = 1.05,
    fri_factor: float = 1.10,
    sat_factor: float = 1.20,
    sun_factor: float = 1.10,
) -> DataFrame:
    """
    Adds one column:
      - day_of_week_impact_factor

    Defaults are a rough heuristic for a San Francisco coffee shop.
    """

    # Make sure we’re working with a proper date
    df = df.withColumn("date", F.to_date("date"))

    # 'E' gives abbreviated day name: Mon, Tue, ...
    dow = F.date_format("date", "E")

    df = df.withColumn(
        "day_of_week_impact_factor",
        F.when(dow == "Mon", F.lit(float(mon_factor)))
         .when(dow == "Tue", F.lit(float(tue_factor)))
         .when(dow == "Wed", F.lit(float(wed_factor)))
         .when(dow == "Thu", F.lit(float(thu_factor)))
         .when(dow == "Fri", F.lit(float(fri_factor)))
         .when(dow == "Sat", F.lit(float(sat_factor)))
         .otherwise(F.lit(float(sun_factor)))  # "Sun"
    )

    return df


def add_month_over_month_growth_features(
    df: DataFrame,
    spark: SparkSession,
    monthly_growth_pct: float
) -> DataFrame:
    """
    Adds one column:
      - organic_mom_growth_factor

    df must contain:
      - 'date':       the observation date
      - 'start_date': the business / cafe start date
    """

    # Convert percent to decimal rate
    monthly_rate = float(monthly_growth_pct) / 100.0

    # Ensure proper date types
    df = df.withColumn("date", F.to_date("date"))
    df = df.withColumn("start_date", F.to_date("start_date"))

    # Months since start_date (based on year-month)
    months_since_start = F.floor(
        F.months_between(
            F.trunc(F.col("date"), "month"),
            F.trunc(F.col("start_date"), "month")
        )
    ).cast("int")

    # Organic month-over-month growth factor
    df = df.withColumn(
        "organic_mom_growth_factor",
        F.when(F.col("start_date").isNull(), F.lit(1.0))                # no start_date → neutral
         .when(months_since_start < 0, F.lit(1.0))                      # before start → neutral
         .otherwise(F.pow(F.lit(1.0 + monthly_rate), months_since_start))
    )

    return df


def add_covid_store_features(
    df: DataFrame,
    spark: SparkSession,
    covid_start: str = "2020-03-01",
    covid_drop_depth: float = 0.8,       # fraction lost at peak (0.5 → 50% drop)
    covid_drop_sigma: float = 100.0,     # width of the dip in days
    covid_recovery_rate: float = 0.003,  # speed of exponential recovery
    covid_recovery_start: str = "2023-06-01",
) -> DataFrame:
    """
    Adds one column:
      - covid_impact_factor

    The factor is meant to be multiplied with other impact factors:
        total = base * holiday * weekday * seasonality * organic_growth * covid

    df must contain:
      - 'date' column (string or date).
    """

    # Ensure date type
    df = df.withColumn("date", F.to_date("date"))

    covid_start_date = F.to_date(F.lit(covid_start))
    covid_recovery_start_date = F.to_date(F.lit(covid_recovery_start))

    # Days since covid_start (can be negative)
    days_since_start = F.datediff(F.col("date"), covid_start_date)

    # --- Gaussian drop term ---
    # drop(t) = depth * exp(- (t^2) / (2 * sigma^2))
    depth_lit = F.lit(float(covid_drop_depth))
    sigma_sq = float(covid_drop_sigma) * float(covid_drop_sigma)

    gaussian_drop = depth_lit * F.exp(
        - (F.pow(days_since_start.cast("double"), F.lit(2.0))
           / F.lit(2.0 * sigma_sq))
    )

    # --- Exponential recovery term ---
    # recovery starts only after covid_recovery_start
    days_since_recovery = F.datediff(F.col("date"), covid_recovery_start_date)
    recovery_days_active = F.greatest(days_since_recovery, F.lit(0))  # max(…, 0)

    # recovery(t) = depth * (1 - exp(-k * active_days))
    recovery = depth_lit * (
        F.lit(1.0) - F.exp(-F.lit(float(covid_recovery_rate)) * recovery_days_active.cast("double"))
    )

    # Raw factor: 1 - drop + recovery
    covid_factor_raw = F.lit(1.0) - gaussian_drop + recovery

    # Before covid_start → no effect (factor = 1.0)
    covid_factor = F.when(F.col("date") < covid_start_date, F.lit(1.0)) \
                    .otherwise(covid_factor_raw)

    # Optional: clamp to a reasonable range, e.g. [0.1, 1.2]
    covid_factor = F.when(covid_factor < F.lit(0.1), F.lit(0.1)) \
                    .when(covid_factor > F.lit(1.2), F.lit(1.2)) \
                    .otherwise(covid_factor)

    df = df.withColumn("covid_impact_factor", covid_factor)

    return df


def add_seasonality_features(
    df: DataFrame,
    spark: SparkSession,
    winter_factor: float = 0.95,
    spring_factor: float = 1.00,
    summer_factor: float = 1.10,
    autumn_factor: float = 0.98,
) -> DataFrame:
    """
    Adds one column:
      - seasonality_impact_factor

    Simple season-based factor:
      * Winter: Dec, Jan, Feb
      * Spring: Mar, Apr, May
      * Summer: Jun, Jul, Aug
      * Autumn: Sep, Oct, Nov
    """

    df = df.withColumn("date", F.to_date("date"))
    month_col = F.month("date")

    df = df.withColumn(
        "seasonality_impact_factor",
        F.when(month_col.isin(12, 1, 2),  F.lit(float(winter_factor)))  # winter
         .when(month_col.isin(3, 4, 5),   F.lit(float(spring_factor)))  # spring
         .when(month_col.isin(6, 7, 8),   F.lit(float(summer_factor)))  # summer
         .otherwise(F.lit(float(autumn_factor)))                        # autumn
    )

    return df


def add_total_impact_features(
    df: DataFrame,
    spark: SparkSession,
    factor_cols: Optional[List[str]] = None,
    output_col: str = "total_impact_factor",
    output_volume_col: str = "simulated_volume",
    base_volume_col: str = "base_volume",
) -> DataFrame:
    """
    Adds:
      - output_col (default: 'total_impact_factor'):
          product of all specified impact factor columns
      - output_volume_col (default: 'simulated_volume'), if base_volume column exists:
          base_volume * total_impact_factor

    df should already contain the individual impact columns, e.g.:
      - 'random_impact_factor'
      - 'holiday_impact_factor'
      - 'day_of_week_impact_factor'
      - 'organic_mom_growth_factor'
      - 'covid_impact_factor'
      - 'seasonality_impact_factor'
    """

    # Default set of factor columns you’re using in your pipeline
    default_factor_cols = [
        "random_impact_factor",
        "holiday_impact_factor",
        "day_of_week_impact_factor",
        "organic_mom_growth_factor",
        "covid_impact_factor",
        "seasonality_impact_factor",
    ]

    if factor_cols is None:
        # Use only those default columns that are actually present in df
        factor_cols = [c for c in default_factor_cols if c in df.columns]

    # If no factor columns are found, just create a neutral factor 1.0
    if not factor_cols:
        df = df.withColumn(output_col, F.lit(1.0))
    else:
        # Start with 1.0 and multiply all factors
        total_expr = F.lit(1.0)
        for c in factor_cols:
            total_expr = total_expr * F.col(c)

        df = df.withColumn(output_col, total_expr.cast("integer"))

    # If base_volume exists, compute simulated volume = base_volume * total_impact_factor
    if base_volume_col in df.columns:
        df = df.withColumn(
            output_volume_col,
            F.col(base_volume_col) * F.col(output_col)
        )

    return df

def explode_by_simulated_volume(
    df: DataFrame,
    volume_col: str = "simulated_volume",
) -> DataFrame:
    """
    Explodes the dataset so that each row is repeated `simulated_volume` times.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame, expected to contain `volume_col`.
    volume_col : str, default "simulated_volume"
        Column containing the integer number of times each row should be repeated.

    Behavior
    --------
    - If `simulated_volume` is NULL, it is treated as 0.
    - If `simulated_volume` <= 0, the row produces no output rows.
    - Uses Spark sequence(1, simulated_volume) + explode to generate rows.
      The sequence index is discarded.
    """

    # Ensure volume column exists and is an integer
    df = df.withColumn(
        volume_col,
        F.coalesce(F.col(volume_col), F.lit(0)).cast("int")
    )

    # Create array sequence [1..simulated_volume]
    df = df.withColumn(
        "volume_seq",
        F.expr(f"sequence(1, {volume_col})")
    )

    # Explode → creates one row per element in volume_seq
    df = df.withColumn("_tmp_explode", F.explode("volume_seq")) \
           .drop("volume_seq", "_tmp_explode")

    return df

def remove_helper_columns(df: DataFrame) -> DataFrame:
    """
    Removes helper or intermediate columns used during simulation and feature generation.

    Columns removed (if present):
      simulated_volume,
      total_impact_factor,
      seasonality_impact_factor,
      covid_impact_factor,
      organic_mom_growth_factor,
      day_of_week_impact_factor,
      holiday_impact_factor,
      random_impact_factor,
      covid_progress,
      days_since_covid_start,
      season,
      months_since_start,
      covid_sales_impact_factor,
      base_volume,
      is_weekend,
      day_name,
      day_of_week,
      calendar_week,
      day,
      month,
      year,
      end_date,
      start_date,
      date
    """

    cols_to_remove = [
        "simulated_volume",
        "total_impact_factor",
        "seasonality_impact_factor",
        "covid_impact_factor",
        "organic_mom_growth_factor",
        "day_of_week_impact_factor",
        "holiday_impact_factor",
        "random_impact_factor",
        "covid_progress",
        "days_since_covid_start",
        "season",
        "months_since_start",
        "covid_sales_impact_factor",
        "base_volume",
        "is_weekend",
        "day_name",
        "day_of_week",
        "calendar_week",
        "day",
        "month",
        "year",
        "end_date",
        "start_date",
        "date",
    ]

    existing_cols = [c for c in cols_to_remove if c in df.columns]

    if existing_cols:
        df = df.drop(*existing_cols)

    return df

def add_quantity_sold(
    df: DataFrame,
    p1: float = 0.80,   # 80% chance quantity = 1
    p2: float = 0.15,   # 15% chance quantity = 2
    p3: float = 0.04,   # 4% chance quantity = 3
    p_rare: float = 0.01  # 1% chance of 4–5
) -> DataFrame:
    """
    Adds a column:
      - quantity_sold

    Simulates realistic transaction quantities:
      - ~80% = 1
      - ~15% = 2
      - ~4%  = 3
      - ~1%  = 4 or 5
    """

    # Generate a random value in [0,1)
    rand = F.rand()

    df = df.withColumn(
        "quantity_sold",
        F.when(rand < p1, F.lit(1))
         .when(rand < p1 + p2, F.lit(2))
         .when(rand < p1 + p2 + p3, F.lit(3))
         .otherwise(F.floor(F.rand() * 2 + 4))  # gives 4 or 5
    )

    return df