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
        # product_key, name, category, subcategory, is_beans, in_store, online, list_price, cost_of_goods
        (101, "Single Espresso",              "Drink", "Espresso",     False, True,  True,  3.50, 0.60),
        (102, "Sunny Bay Latte",              "Drink", "Milk Coffee",  False, True,  True,  4.50, 1.10),
        (103, "Cappuccino",                   "Drink", "Milk Coffee",  False, True,  True,  4.40, 1.00),
        (104, "Flat White",                   "Drink", "Milk Coffee",  False, True,  True,  4.60, 1.20),
        (105, "Americano",                    "Drink", "Black Coffee", False, True,  True,  3.80, 0.50),

        (201, "Butter Croissant",             "Food",  "Pastry",       False, True,  False, 3.20, 1.20),
        (202, "Banana Bread Slice",           "Food",  "Cake",         False, True,  False, 3.80, 1.40),
        (203, "Avocado Toast",                "Food",  "Brunch",       False, True,  False, 9.50, 3.20),

        (301, "Drip Coffee",                  "Drink", "Brewed Coffee", False, True, True,  3.20, 0.40),
        (302, "Iced Latte",                   "Drink", "Iced Coffee",   False, True, True,  4.90, 1.30),
        (303, "Matcha Latte",                 "Drink", "Specialty",     False, True, True,  5.10, 1.80),
        (401, "Seasonal Signature Drink",     "Drink", "Seasonal",      False, True, True,  5.50, 1.90),
        (501, "Cold Brew Bottle 355ml",       "Drink", "Cold Brew",     False, True, True,  5.90, 2.00),
        (601, "Office Coffee Box 3L",         "Drink", "Bulk / Office", False, True, False, 24.00, 6.00),

        (9001, "Ethiopia Sidamo Beans 250g",  "Beans", "Single Origin", True,  False, True, 16.00, 7.00),
        (9002, "Colombia Supremo Beans 250g", "Beans", "Single Origin", True,  False, True, 15.50, 6.50),
        (9003, "Brazil Cerrado Beans 250g",   "Beans", "Single Origin", True,  False, True, 15.00, 6.00),
        (9004, "Guatemala Antigua Beans 250g","Beans", "Single Origin", True,  False, True, 16.50, 7.20),
        (9005, "Kenya AA Beans 250g",         "Beans", "Single Origin", True,  False, True, 17.00, 7.40),
        (9006, "Costa Rica Tarrazu 250g",     "Beans", "Single Origin", True,  False, True, 16.50, 7.10),
        (9007, "Sunny Bay House Blend 1kg",   "Beans", "Blend",         True,  False, True, 34.00, 14.00),
        (9008, "Decaf Specialty Blend 250g",  "Beans", "Blend / Decaf", True,  False, True, 16.00, 7.00),
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
        "cost_of_goods_usd",
    ]

    return spark.createDataFrame(products, schema=columns)

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

    # Reorder / select columns
    dim_customer = dim_customer.select(
        "customer_key",
        # "store_key",
        "loyalty_segment",
        "channel_preference",
        "is_home_barista",
        "city"
    )

    return dim_customer


def create_dim_store(spark) -> DataFrame:
    """
    Dimension: Store (Sunny Bay Roastery)
    Added: tax_rate (per store)
    """

    # ------------------------------------------------------------------
    # Static store data + tax_rate
    # ------------------------------------------------------------------
    stores = [
        # store_key, name, type, city, neighborhood, open, close, is_online,
        # area, seating, employees, manager, tax_rate
        (1, "Sunny Bay – Market Street",  "Retail Cafe", "San Francisco",
         "Downtown / Financial District", "2010-03-15", None, False,
         120.0, 45, 18, "Alice Chen", 0.08625),

        (2, "Sunny Bay – Mission",        "Retail Cafe", "San Francisco",
         "Mission District",              "2012-05-01", None, False,
         100.0, 38, 15, "Carlos Ramirez", 0.08625),

        (3, "Sunny Bay – Westfield Mall", "Retail Cafe", "San Francisco",
         "Union Square / Mall",           "2015-09-10", None, False,
         80.0, 25, 10, "Julia Tan", 0.08625),

        (4, "Sunny Bay – SoMa Offices",   "Retail Cafe", "San Francisco",
         "SoMa / Office Hub",             "2017-01-20", None, False,
         110.0, 40, 14, "Kevin O’Neill", 0.08625),

        (5, "Sunny Bay – Hayes Valley",   "Retail Cafe", "San Francisco",
         "Hayes Valley",                  "2018-06-05", None, False,
         90.0, 30, 12, "Priya Desai", 0.08625),

        # Online shop uses destination-based tax — store has no fixed rate
        (6, "Sunny Bay Online",           "Online Shop", "San Francisco",
         "E-commerce / Home Barista",     "2020-04-01", None, True,
         None, None, 25, "Digital Team", 0.0),
    ]

    # ------------------------------------------------------------------
    # Schema including tax_rate
    # ------------------------------------------------------------------
    schema = T.StructType([
        T.StructField("store_key", T.IntegerType(), False),
        T.StructField("store_name", T.StringType(), False),
        T.StructField("store_type", T.StringType(), False),
        T.StructField("city", T.StringType(), False),
        T.StructField("neighborhood_or_channel", T.StringType(), False),
        T.StructField("open_date", T.StringType(), True),
        T.StructField("close_date", T.StringType(), True),
        T.StructField("is_online", T.BooleanType(), False),
        T.StructField("store_area_sqm", T.DoubleType(), True),
        T.StructField("seating_capacity", T.IntegerType(), True),
        T.StructField("num_employees", T.IntegerType(), True),
        T.StructField("store_manager", T.StringType(), True),
        T.StructField("tax_rate", T.DoubleType(), True),
    ])

    dim_store = spark.createDataFrame(stores, schema=schema)

    # ------------------------------------------------------------------
    # Final column order
    # ------------------------------------------------------------------
    dim_store = dim_store.select(
        "store_key",
        "store_name",
        "store_type",
        "city",
        "neighborhood_or_channel",
        "is_online",
        "store_area_sqm",
        "seating_capacity",
        "num_employees",
        "store_manager",
        "tax_rate"
    )

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
    Simplified Date dimension for Sunny Bay Roastery.

    - One row per calendar day between start_date and end_date.
    - Includes:
        date_key, year, month, day, calendar_week,
        day_of_week (1=Mon..7=Sun, ISO-like), day_name, is_weekend,
        season, is_us_public_holiday
    """

    if season_weights is None:
        season_weights = {
            "winter": 1.10,
            "spring": 1.00,
            "summer": 0.95,
            "autumn": 1.15,
        }

    if dow_weights is None:
        dow_weights = {
            1: 0.95,
            2: 1.00,
            3: 1.00,
            4: 1.05,
            5: 1.20,
            6: 1.40,
            7: 1.10,
        }

    if us_public_holidays is None:
        us_public_holidays = []

    # ------------------------------------------------------------------
    # 1) Base date list
    # ------------------------------------------------------------------
    dates = (
        spark.range(1)
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
    # Day-of-week
    # ------------------------------------------------------------------
    dim_date = dim_date.withColumn("spark_dow", F.dayofweek("date"))
    dim_date = dim_date.withColumn(
        "day_of_week",
        ((F.col("spark_dow") + 5) % 7) + 1  # 1=Mon..7=Sun
    )

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
    # Season
    # ------------------------------------------------------------------
    dim_date = dim_date.withColumn(
        "season",
        F.when(F.col("month").isin(12, 1, 2), "winter")
         .when(F.col("month").isin(3, 4, 5), "spring")
         .when(F.col("month").isin(6, 7, 8), "summer")
         .otherwise("autumn")
    )

    # ------------------------------------------------------------------
    # US public holidays
    # ------------------------------------------------------------------
    if len(us_public_holidays) > 0:
        holiday_array = F.array([F.to_date(F.lit(d)) for d in us_public_holidays])
        dim_date = dim_date.withColumn(
            "is_us_public_holiday",
            F.array_contains(holiday_array, F.col("date"))
        )
    else:
        dim_date = dim_date.withColumn("is_us_public_holiday", F.lit(False))

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
        "is_us_public_holiday"
    )

    return dim_date