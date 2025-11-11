from typing import Dict, List
import math

from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T
from pyspark.sql.types import IntegerType, LongType

# ---------------------------------------------------------
# Product dimension key assignment
# ---------------------------------------------------------

def add_dim_product_key(
    df: DataFrame,
) -> DataFrame:
    """
    Adds one column:
      - product_key

    Product choice depends on store_key and random noise.
    Each store has its own product mix (espresso-heavy, bakery-heavy, beans, etc.).

    NOTE: No spark.sparkContext.broadcast (works on serverless / Spark Connect).
    """

    # Product popularity distributions per store
    product_weights_by_store: Dict[int, List] = {
        1: [  # downtown / commuters
            (101, 0.25), (102, 0.25), (103, 0.15),
            (104, 0.10), (105, 0.10), (201, 0.05),
            (301, 0.05), (302, 0.05)
        ],
        2: [  # residential / brunch
            (101, 0.15), (102, 0.25), (103, 0.10),
            (201, 0.15), (202, 0.05), (301, 0.15),
            (302, 0.10), (303, 0.05)
        ],
        3: [  # mall shoppers
            (102, 0.20), (103, 0.15), (201, 0.15),
            (202, 0.10), (401, 0.15), (301, 0.10),
            (302, 0.10), (501, 0.05)
        ],
        4: [  # office / commuters
            (101, 0.30), (104, 0.20), (105, 0.20),
            (102, 0.10), (301, 0.10), (302, 0.05),
            (601, 0.05)
        ],
        5: [  # trendy / social
            (102, 0.20), (103, 0.10), (201, 0.15),
            (202, 0.15), (203, 0.10), (401, 0.10),
            (501, 0.10), (302, 0.10)
        ],
        6: [  # specialty coffee beans only
            (9001, 0.20),  # Ethiopia Sidamo
            (9002, 0.20),  # Colombia Supremo
            (9003, 0.15),  # Brazil Cerrado
            (9004, 0.15),  # Guatemala Antigua
            (9005, 0.10),  # Kenya AA
            (9006, 0.10),  # Costa Rica Tarrazu
            (9007, 0.05),  # House Blend
            (9008, 0.05),  # Decaf Specialty Blend
        ],
    }

    default_mix = [
        (101, 0.20),
        (102, 0.25),
        (103, 0.15),
        (201, 0.15),
        (301, 0.15),
        (302, 0.10),
    ]

    # Use closure instead of broadcast (dict is tiny)
    def _choose_product(store_key: int, r: float) -> int:
        if store_key is None:
            dist = default_mix
        else:
            dist = product_weights_by_store.get(store_key, default_mix)

        cum = 0.0
        for product_id, w in dist:
            cum += float(w)
            if r <= cum:
                return int(product_id)
        return int(dist[-1][0])

    choose_product_udf = F.udf(_choose_product, IntegerType())

    # One random draw per row for product assignment
    df = df.withColumn("product_key", choose_product_udf(F.col("store_key"), F.rand(1234)))

    return df


# ---------------------------------------------------------
# Customer dimension key assignment
# ---------------------------------------------------------

def add_dim_customer_key(
    df: DataFrame,
    max_customers_per_store: int = 2000,
    n_loyal_customers: int = 80,
    loyal_share: float = 0.65,
) -> DataFrame:
    """
    Adds one column:
      - customer_key

    Model:
      - Each store has up to `max_customers_per_store` customers.
      - IDs: base_id + local_id
           base_id = store_key * 100000
           local_id in [1, max_customers_per_store]
      - With probability `loyal_share` we pick from the top `n_loyal_customers`
        (frequent buyers), otherwise from the long tail.

    Implemented without sparkContext / broadcast so it works on serverless.
    """

    max_n = int(max_customers_per_store)
    n_loyal = int(n_loyal_customers)
    loyal_share = float(loyal_share)

    def _choose_customer(store_key: int, r_loyal: float, r_idx: float) -> int:
        if store_key is None:
            store_key = 0

        base_id = int(store_key) * 100000

        if r_loyal < loyal_share:
            # Loyal customer group [1 .. n_loyal]
            idx = int(math.floor(r_idx * n_loyal)) + 1
            if idx > n_loyal:
                idx = n_loyal
        else:
            # Long tail [n_loyal+1 .. max_n]
            tail_size = max_n - n_loyal
            if tail_size <= 0:
                idx = n_loyal
            else:
                idx_tail = int(math.floor(r_idx * tail_size))  # 0 .. tail_size-1
                if idx_tail >= tail_size:
                    idx_tail = tail_size - 1
                idx = n_loyal + 1 + idx_tail

        return base_id + idx

    choose_customer_udf = F.udf(_choose_customer, LongType())

    # Two random streams: one for loyalty decision, one for which customer
    df = df.withColumn("rand_loyal", F.rand(4321)) \
           .withColumn("rand_idx", F.rand(9876))

    df = df.withColumn(
        "customer_key",
        choose_customer_udf(F.col("store_key"), F.col("rand_loyal"), F.col("rand_idx"))
    )

    # Drop helper columns
    df = df.drop("rand_loyal", "rand_idx")

    return df


def create_dim_product(spark) -> DataFrame:
    """
    Static product dimension for Sunny Bay Roastery.
    Keys match the product_key values used in add_dim_product_key.
    """

    products = [
        # --- In-store drinks (100-range) ---
        (101, "Single Espresso",              "Drink", "Espresso",     False, True,  True,  3.50),
        (102, "Sunny Bay Latte",              "Drink", "Milk Coffee",  False, True,  True,  4.50),
        (103, "Cappuccino",                   "Drink", "Milk Coffee",  False, True,  True,  4.40),
        (104, "Flat White",                   "Drink", "Milk Coffee",  False, True,  True,  4.60),
        (105, "Americano",                    "Drink", "Black Coffee", False, True,  True,  3.80),

        # --- Food / pastry (200-range) ---
        (201, "Butter Croissant",             "Food",  "Pastry",       False, True,  False, 3.20),
        (202, "Banana Bread Slice",           "Food",  "Cake",         False, True,  False, 3.80),
        (203, "Avocado Toast",                "Food",  "Brunch",       False, True,  False, 9.50),

        # --- In-store brewed / cold drinks (300-range, 400, 500, 600) ---
        (301, "Drip Coffee",                  "Drink", "Brewed Coffee", False, True, True,  3.20),
        (302, "Iced Latte",                   "Drink", "Iced Coffee",   False, True, True,  4.90),
        (303, "Matcha Latte",                 "Drink", "Specialty",     False, True, True,  5.10),
        (401, "Seasonal Signature Drink",     "Drink", "Seasonal",      False, True, True,  5.50),
        (501, "Cold Brew Bottle 355ml",       "Drink", "Cold Brew",     False, True, True,  5.90),
        (601, "Office Coffee Box 3L",         "Drink", "Bulk / Office", False, True, False, 24.00),

        # --- Online specialty beans (9000-range) ---
        (9001, "Ethiopia Sidamo Beans 250g",  "Beans", "Single Origin", True,  False, True, 16.00),
        (9002, "Colombia Supremo Beans 250g", "Beans", "Single Origin", True,  False, True, 15.50),
        (9003, "Brazil Cerrado Beans 250g",   "Beans", "Single Origin", True,  False, True, 15.00),
        (9004, "Guatemala Antigua Beans 250g","Beans", "Single Origin", True,  False, True, 16.50),
        (9005, "Kenya AA Beans 250g",         "Beans", "Single Origin", True,  False, True, 17.00),
        (9006, "Costa Rica Tarrazu 250g",     "Beans", "Single Origin", True,  False, True, 16.50),
        (9007, "Sunny Bay House Blend 1kg",   "Beans", "Blend",         True,  False, True, 34.00),
        (9008, "Decaf Specialty Blend 250g",  "Beans", "Blend / Decaf", True,  False, True, 16.00),
    ]

    columns = [
        "product_key",
        "product_name",
        "product_category",
        "product_subcategory",
        "is_beans",
        "available_in_store",
        "available_online",
        "list_price_usd",
    ]

    dim_product = spark.createDataFrame(products, schema=columns)

    return dim_product

def create_dim_customer(
    spark,
    store_keys = [1, 2, 3, 4, 5, 6],
    max_customers_per_store: int = 2000,
) -> DataFrame:
    """
    Static customer dimension matching add_dim_customer_key:
      customer_key = store_key * 100000 + local_id

    Very simple, rule-based attributes that fit the Sunny Bay story.
    """

    # 1) Build base grid: (store_key, local_id)
    stores_df = spark.createDataFrame(
        [(int(k),) for k in store_keys],
        schema=T.StructType([T.StructField("store_key", T.IntegerType(), False)])
    )

    local_ids_df = (
        spark.range(1, max_customers_per_store + 1)
             .withColumnRenamed("id", "local_id")
    )

    base = stores_df.crossJoin(local_ids_df)

    # 2) Derive customer_key
    dim_customer = base.withColumn(
        "customer_key",
        (F.col("store_key") * F.lit(100000) + F.col("local_id")).cast("long")
    )

    # 3) Add simple attributes:
    # - loyalty_segment: first 80 per store are "Loyalist", then "Regular", then "Occasional"
    dim_customer = dim_customer.withColumn(
        "loyalty_segment",
        F.when(F.col("local_id") <= 80, "Loyalist")
         .when(F.col("local_id") <= 500, "Regular")
         .otherwise("Occasional")
    )

    # - channel_preference:
    #   * store 6 = online shop customers
    #   * others = mainly in-store
    dim_customer = dim_customer.withColumn(
        "channel_preference",
        F.when(F.col("store_key") == 6, "Online")
         .otherwise("In-store")
    )

    # - home_barista_flag:
    #   * all online store customers are home baristas
    #   * plus some of the top loyal customers in physical stores
    dim_customer = dim_customer.withColumn(
        "is_home_barista",
        F.when(F.col("store_key") == 6, F.lit(True))
         .when((F.col("store_key") <= 5) & (F.col("local_id") <= 150), F.lit(True))
         .otherwise(F.lit(False))
    )

    # - city: Sunny Bay is SF-based, but online customers can be "Various"
    dim_customer = dim_customer.withColumn(
        "city",
        F.when(F.col("store_key") == 6, "Various / Online")
         .otherwise("San Francisco")
    )

    # - customer_since_year: spread between 2010 and 2023
    dim_customer = dim_customer.withColumn(
        "customer_since_year",
        (F.lit(2010) + (F.col("local_id") % 14)).cast("int")  # 2010–2023
    )

    # Reorder / select columns
    dim_customer = dim_customer.select(
        "customer_key",
        "store_key",
        "loyalty_segment",
        "channel_preference",
        "is_home_barista",
        "city",
        "customer_since_year",
    )

    return dim_customer


def create_dim_store(spark) -> DataFrame:
    """
    Store dimension:
      - 1–5 = San Francisco physical stores
      - 6   = Online shop
    """

    stores = [
        (1, "Sunny Bay – Market Street",     "Retail Cafe", "San Francisco", "Downtown / Financial District", "2010-03-15", None, False),
        (2, "Sunny Bay – Mission",           "Retail Cafe", "San Francisco", "Mission District",              "2012-05-01", None, False),
        (3, "Sunny Bay – Westfield Mall",    "Retail Cafe", "San Francisco", "Union Square / Mall",           "2015-09-10", None, False),
        (4, "Sunny Bay – SoMa Offices",      "Retail Cafe", "San Francisco", "SoMa / Office Hub",             "2017-01-20", None, False),
        (5, "Sunny Bay – Hayes Valley",      "Retail Cafe", "San Francisco", "Hayes Valley",                  "2018-06-05", None, False),
        (6, "Sunny Bay Online",              "Online Shop", "San Francisco", "E-commerce / Home Barista",     "2020-04-01", None, True),
    ]

    columns = [
        "store_key",
        "store_name",
        "store_type",
        "city",
        "neighborhood_or_channel",
        "open_date",
        "close_date",
        "is_online",
    ]

    dim_store = spark.createDataFrame(stores, schema=columns)

    return dim_store

def create_dim_date(
    spark,
    start_date: str = "2010-01-01",
    end_date: str = "2025-12-31",
    covid_start: str = "2020-03-01",
    covid_end: str = "2021-06-30",
    season_weights: dict = None,
    dow_weights: dict = None,
    us_public_holidays: list = None,
) -> DataFrame:
    """
    Date dimension for Sunny Bay Roastery.

    - One row per calendar day between start_date and end_date.
    - Includes:
        date_key, year, month, day, calendar_week,
        day_of_week (1=Mon..7=Sun, ISO-like), day_name, is_weekend,
        season, is_covid_period, is_covid_first_half,
        season_weight, day_of_week_weight, covid_weight,
        is_us_public_holiday, holiday_weight, demand_factor
    """

    # ------------------------------------------------------------------
    # Defaults if not provided
    # ------------------------------------------------------------------
    if season_weights is None:
        season_weights = {
            "winter": 1.10,
            "spring": 1.00,
            "summer": 0.95,
            "autumn": 1.15,
        }

    if dow_weights is None:
        # 1=Mon..7=Sun (we’ll build it that way below)
        dow_weights = {
            1: 0.95,  # Mon
            2: 1.00,  # Tue
            3: 1.00,  # Wed
            4: 1.05,  # Thu
            5: 1.20,  # Fri
            6: 1.40,  # Sat
            7: 1.10,  # Sun
        }

    if us_public_holidays is None:
        us_public_holidays = []  # ["2010-01-01", "2010-07-04", ...]

    # ------------------------------------------------------------------
    # 1) Base date list
    # ------------------------------------------------------------------
    dates = (
        spark.range(1)  # dummy
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(start_date)),
                    F.to_date(F.lit(end_date)),
                    F.expr("interval 1 day")
                )
            ).alias("date")
        )
    )

    dim_date = (
        dates
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("calendar_week", F.weekofyear("date"))
    )

    # ------------------------------------------------------------------
    # 1a) Day-of-week (no datetime patterns)
    # Spark dayofweek(): 1=Sunday .. 7=Saturday
    # We convert to ISO-like: 1=Mon .. 7=Sun
    # ------------------------------------------------------------------
    dim_date = dim_date.withColumn("spark_dow", F.dayofweek("date"))
    dim_date = dim_date.withColumn(
        "day_of_week",
        ((F.col("spark_dow") + 5) % 7) + 1  # 1=Mon..7=Sun
    )

    # Day name derived from our own day_of_week (no format strings)
    dim_date = dim_date.withColumn(
        "day_name",
        F.when(F.col("day_of_week") == 1, "Mon")
         .when(F.col("day_of_week") == 2, "Tue")
         .when(F.col("day_of_week") == 3, "Wed")
         .when(F.col("day_of_week") == 4, "Thu")
         .when(F.col("day_of_week") == 5, "Fri")
         .when(F.col("day_of_week") == 6, "Sat")
         .otherwise("Sun")
    )

    dim_date = dim_date.withColumn("is_weekend", F.col("day_of_week").isin(6, 7))

    # ------------------------------------------------------------------
    # 2) Season
    # ------------------------------------------------------------------
    dim_date = dim_date.withColumn(
        "season",
        F.when(F.col("month").isin(12, 1, 2), "winter")
         .when(F.col("month").isin(3, 4, 5), "spring")
         .when(F.col("month").isin(6, 7, 8), "summer")
         .otherwise("autumn")
    )

    # ------------------------------------------------------------------
    # 3) COVID flags
    # ------------------------------------------------------------------
    covid_start_col = F.to_date(F.lit(covid_start))
    covid_end_col   = F.to_date(F.lit(covid_end))
    covid_mid_col   = F.date_add(
        covid_start_col,
        (F.datediff(covid_end_col, covid_start_col) / 2).cast("int")
    )

    dim_date = (
        dim_date
        .withColumn(
            "is_covid_period",
            (F.col("date") >= covid_start_col) & (F.col("date") <= covid_end_col)
        )
        .withColumn(
            "is_covid_first_half",
            (F.col("date") >= covid_start_col) & (F.col("date") <= covid_mid_col)
        )
    )

    # ------------------------------------------------------------------
    # 4) season_weight
    # ------------------------------------------------------------------
    season_weight_map = F.create_map(
        *[F.lit(x) for kv in season_weights.items() for x in kv]
    )
    dim_date = dim_date.withColumn(
        "season_weight",
        season_weight_map[F.col("season")].cast("double")
    )

    # ------------------------------------------------------------------
    # 5) day_of_week_weight
    # ------------------------------------------------------------------
    dow_weight_map = F.create_map(
        *[F.lit(x) for kv in dow_weights.items() for x in kv]
    )
    dim_date = dim_date.withColumn(
        "day_of_week_weight",
        dow_weight_map[F.col("day_of_week")].cast("double")
    )

    # ------------------------------------------------------------------
    # 6) US public holidays + holiday_weight
    #    Handle empty list safely (no F.array() on empty)
    # ------------------------------------------------------------------
    if len(us_public_holidays) > 0:
        holiday_array = F.array([F.to_date(F.lit(d)) for d in us_public_holidays])
        dim_date = dim_date.withColumn(
            "is_us_public_holiday",
            F.array_contains(holiday_array, F.col("date"))
        )
    else:
        dim_date = dim_date.withColumn("is_us_public_holiday", F.lit(False))

    dim_date = dim_date.withColumn(
        "holiday_weight",
        F.when(F.col("is_us_public_holiday"), F.lit(1.5)).otherwise(F.lit(1.0))
    )

    # ------------------------------------------------------------------
    # 7) covid_weight & final demand_factor
    # ------------------------------------------------------------------
    dim_date = dim_date.withColumn(
        "covid_weight",
        F.when(F.col("is_covid_period"), F.lit(0.6)).otherwise(F.lit(1.0))
    )

    dim_date = dim_date.withColumn(
        "demand_factor",
        F.col("season_weight")
        * F.col("day_of_week_weight")
        * F.col("covid_weight")
        * F.col("holiday_weight")
    )

    # ------------------------------------------------------------------
    # Final column order (drop helper spark_dow)
    # ------------------------------------------------------------------
    dim_date = dim_date.select(
        "date_key",
        "date",
        "year",
        "month",
        "day",
        "calendar_week",
        "day_of_week",
        "day_name",
        "is_weekend",
        "season",
        "is_covid_period",
        "is_covid_first_half",
        "is_us_public_holiday",
        "season_weight",
        "day_of_week_weight",
        "holiday_weight",
        "covid_weight",
        "demand_factor",
    )

    return dim_date