"""Spark builders for the (normalized) operational source systems of Sunny Bay Roastery.

The raw files written to the bronze Volume imitate what you would extract from
real operational systems:

    POS        pos_transactions, pos_transaction_lines        (cafés, CSV)
    Webshop    web_orders (nested JSON), web_accounts         (online shop)
    CRM        crm_customers (with history versions), crm_loyalty_cards
    ERP        erp_products, erp_product_categories, erp_product_prices, erp_stores
    HR         hr_employees
    Reference  ref_public_holidays

The data contains a few deliberate "real world" quirks that the SQL labs use:
duplicate CRM versions, messy e-mail addresses, guest orders, voided
transactions, loyalty members without purchases, products that were never
sold, test items scanned at the register and 20 lines with a negative quantity.
All randomness is hash based, so the output is reproducible for a given date.
"""

from datetime import date, timedelta
from typing import Dict, List

from pyspark.sql import Column, DataFrame, SparkSession, Window, functions as F

import reference_data as ref
from impact_factors import det_uniform, quantity_expr, weighted_choice


# ---------------------------------------------------------------------------
# Customer population parameters
# ---------------------------------------------------------------------------

ACTIVE_CUSTOMERS_PER_STORE = 2000   # customers that can appear on orders
DORMANT_CUSTOMERS_PER_STORE = 300   # registered, but never buy anything
LOYAL_CUSTOMERS = 150               # the "regulars" with a high visit share
LOYAL_ORDER_SHARE = 0.45
ONLINE_LOYAL_CUSTOMERS = 120
ONLINE_LOYAL_ORDER_SHARE = 0.35
ONLINE_ACCOUNT_BASE = ref.ONLINE_STORE_ID * 100000   # web accounts 600001 ...
LOYALTY_PROGRAM_LAUNCH = date(2016, 3, 1)
OMNICHANNEL_SHARE = 0.15            # web accounts that also exist in the CRM


def _spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _pick(u: Column, values: List[str]) -> Column:
    """Pick one element of a Python list with a uniform draw ``u``."""
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (F.floor(u * len(values)) + 1).cast("int"))


def _ascii(c: Column) -> Column:
    """Transliterate German umlauts for e-mail addresses."""
    for src, dst in (("ä", "ae"), ("ö", "oe"), ("ü", "ue"), ("ß", "ss"), ("Ä", "Ae"), ("Ö", "Oe"), ("Ü", "Ue")):
        c = F.regexp_replace(c, src, dst)
    return c


def _random_date(u: Column, start: Column, end: Column) -> Column:
    """Uniformly distributed date between ``start`` and ``end`` (inclusive)."""
    span = F.greatest(F.datediff(end, start), F.lit(0))
    return F.date_add(start, F.floor(u * (span + 1)).cast("int"))


def _store_opened_on(store_rows: List[tuple]) -> Column:
    expr = None
    for row in store_rows:
        cond = F.col("home_store_id") == row[0]
        expr = F.when(cond, F.lit(row[5])) if expr is None else expr.when(cond, F.lit(row[5]))
    return expr


# ---------------------------------------------------------------------------
# ERP / HR / Reference
# ---------------------------------------------------------------------------

def erp_stores(store_rows: List[tuple]) -> DataFrame:
    schema = (
        "store_id INT, store_name STRING, store_type STRING, city STRING, neighborhood_or_channel STRING, "
        "opened_on DATE, is_online BOOLEAN, store_area_sqm DOUBLE, seating_capacity INT, "
        "manager_employee_id INT, country_name STRING, country_iso2 STRING, country_iso3 STRING, "
        "state_province STRING, state_iso2 STRING, county_district STRING, postal_code STRING, "
        "street_address STRING, latitude DOUBLE, longitude DOUBLE"
    )
    return _spark().createDataFrame(store_rows, schema)


def erp_product_categories() -> DataFrame:
    return _spark().createDataFrame(
        ref.PRODUCT_CATEGORIES, "category_id INT, parent_category_id INT, category_name STRING"
    )


def erp_products(as_of: date) -> DataFrame:
    schema = (
        "product_id INT, sku STRING, product_name STRING, category_id INT, is_beans STRING, "
        "channel_code STRING, status_code STRING, tax_class STRING, launched_on DATE"
    )
    return _spark().createDataFrame(ref.products(as_of), schema)


def erp_product_prices(as_of: date, window_start: date) -> DataFrame:
    return _spark().createDataFrame(
        ref.product_prices(as_of, window_start),
        "product_id INT, valid_from DATE, list_price_eur DOUBLE, cost_of_goods_eur DOUBLE",
    )


def hr_employees(store_rows: List[tuple]) -> DataFrame:
    schema = (
        "employee_id INT, first_name STRING, last_name STRING, job_title STRING, "
        "store_id INT, manager_id INT, hired_on DATE"
    )
    return _spark().createDataFrame(ref.employees(store_rows), schema)


def ref_public_holidays(window_start: date, window_end: date) -> DataFrame:
    rows = ref.german_public_holidays(window_start.year, window_end.year + 1)
    return _spark().createDataFrame(rows, "holiday_date DATE, holiday_name STRING")


# ---------------------------------------------------------------------------
# CRM: customers (with history versions) and loyalty cards
# ---------------------------------------------------------------------------

def _crm_population(store_rows: List[tuple]) -> DataFrame:
    """One row per CRM customer (cafés 1-5): active and dormant customers."""
    spark = _spark()
    cafe_ids = [r[0] for r in store_rows if not r[6]]
    stores_df = spark.createDataFrame([(s,) for s in cafe_ids], "home_store_id INT")
    n = ACTIVE_CUSTOMERS_PER_STORE + DORMANT_CUSTOMERS_PER_STORE
    idx_df = spark.range(1, n + 1).withColumnRenamed("id", "local_idx")
    df = stores_df.crossJoin(idx_df)
    return (
        df.withColumn("customer_id", (F.col("home_store_id") * 100000 + F.col("local_idx")).cast("int"))
          .withColumn("is_dormant", F.col("local_idx") > ACTIVE_CUSTOMERS_PER_STORE)
          .withColumn("is_loyal", F.col("local_idx") <= LOYAL_CUSTOMERS)
          .withColumn("store_opened_on", _store_opened_on(store_rows))
    )


def _with_person(df: DataFrame, id_col: str, seed: int) -> DataFrame:
    """Add gender_code, first_name, last_name and a clean e-mail address."""
    u = lambda s: det_uniform(seed + s, F.col(id_col))  # noqa: E731
    gender = weighted_choice(u(1), [("F", .48), ("M", .46), ("D", .01), ("X", .05)])
    first = (
        F.when(gender == "F", _pick(u(2), ref.FIRST_NAMES_F))
         .when(gender == "M", _pick(u(2), ref.FIRST_NAMES_M))
         .otherwise(_pick(u(2), ref.FIRST_NAMES_F + ref.FIRST_NAMES_M))
    )
    last = _pick(u(3), ref.LAST_NAMES)
    return (
        df.withColumn("gender_code", F.when(gender != "X", gender))
          .withColumn("first_name", first)
          .withColumn("last_name", last)
          .withColumn(
              "clean_email",
              F.concat(F.lower(_ascii(F.col("first_name"))), F.lit("."), F.lower(_ascii(F.col("last_name"))),
                       F.lit("."), F.col(id_col).cast("string"), F.lit("@example.com")),
          )
    )


def crm_loyalty_cards(store_rows: List[tuple], window_start: date, window_end: date) -> DataFrame:
    """Loyalty cards. Some cards get replaced, some members leave the program."""
    pop = _crm_population(store_rows)
    u = lambda s: det_uniform(500 + s, F.col("customer_id"))  # noqa: E731

    member_p = (F.when(F.col("is_dormant"), F.lit(0.60))
                 .when(F.col("is_loyal"), F.lit(0.95))
                 .otherwise(F.lit(0.65)))
    tier = (
        F.when(F.col("is_loyal"), weighted_choice(u(2), [("G", .5), ("S", .4), ("B", .1)]))
         .when(F.col("is_dormant"), F.lit("B"))
         .otherwise(weighted_choice(u(2), [("B", .70), ("S", .25), ("G", .05)]))
    )
    earliest = F.greatest(F.lit(LOYALTY_PROGRAM_LAUNCH), F.col("store_opened_on"))
    w_start, w_end = F.lit(window_start), F.lit(window_end)
    # loyal members enrolled before the window, regular ones mostly before, dormant ones inside it
    enrolled = (
        F.when(F.col("is_dormant"), _random_date(u(3), F.greatest(earliest, w_start), w_end))
         .when(F.col("is_loyal") | (u(4) < 0.75),
               _random_date(u(3), earliest, F.greatest(earliest, F.date_sub(w_start, 1))))
         .otherwise(_random_date(u(3), F.greatest(earliest, w_start), w_end))
    )
    members = (
        pop.where(u(1) < member_p)
           .withColumn("tier_code", tier)
           .withColumn("enrolled_on", enrolled)
           .withColumn("event_u", u(5))
           .withColumn("event_date", _random_date(u(6), F.greatest(F.col("enrolled_on"), w_start), w_end))
    )

    def card_no(seq: int) -> Column:
        return F.concat(F.lit("SB"), (F.lit(40000000) + F.col("customer_id") * 3 + seq).cast("string"))

    # ~4 % lose their card and get a new one, ~3 % leave the program
    replaced = F.col("event_u") < 0.04
    quit_program = (F.col("event_u") >= 0.04) & (F.col("event_u") < 0.07)

    first_card = members.select(
        card_no(1).alias("card_no"), "customer_id", "tier_code", "enrolled_on",
        F.when(replaced | quit_program, F.col("event_date")).alias("cancelled_on"),
    )
    second_card = members.where(replaced).select(
        card_no(2).alias("card_no"), "customer_id", "tier_code",
        F.col("event_date").alias("enrolled_on"), F.lit(None).cast("date").alias("cancelled_on"),
    )
    return first_card.unionByName(second_card)


def crm_customers(store_rows: List[tuple], loyalty_cards: DataFrame, window_start: date, as_of: date) -> DataFrame:
    """CRM customer extract. ~10 % of the customers have an older version (history row)."""
    pop = _with_person(_crm_population(store_rows), "customer_id", 100)
    u = lambda s: det_uniform(200 + s, F.col("customer_id"))  # noqa: E731

    first_enrolment = loyalty_cards.groupBy("customer_id").agg(F.min("enrolled_on").alias("first_enrolled_on"))
    pop = pop.join(first_enrolment, "customer_id", "left")

    created = F.coalesce(
        F.col("first_enrolled_on"),
        F.when(F.col("is_dormant"), _random_date(u(1), F.greatest(F.col("store_opened_on"), F.lit(window_start)), F.lit(as_of)))
         .otherwise(_random_date(u(1), F.col("store_opened_on"), F.lit(as_of))),
    )
    city_by_store = None
    for row in store_rows:
        cond = F.col("home_store_id") == row[0]
        city_by_store = F.when(cond, F.lit(row[3])) if city_by_store is None else city_by_store.when(cond, F.lit(row[3]))

    # messy e-mails: some with capital letters, some with leading/trailing blanks
    messy_email = (
        F.when(u(2) < 0.06, F.concat(F.initcap(_ascii(F.col("first_name"))), F.lit("."), F.initcap(_ascii(F.col("last_name"))),
                                     F.lit("."), F.col("customer_id").cast("string"), F.lit("@Example.com")))
         .when(u(2) < 0.09, F.concat(F.lit(" "), F.col("clean_email")))
         .when(u(2) < 0.11, F.concat(F.col("clean_email"), F.lit("  ")))
         .otherwise(F.col("clean_email"))
    )

    current = (
        pop.withColumn("created_at", F.to_timestamp(created))
           .withColumn("email", messy_email)
           .withColumn("birth_date", F.date_add(F.lit(date(1955, 1, 1)), F.floor(u(3) * 18000).cast("int")))
           .withColumn("city", city_by_store)
           .withColumn("is_home_barista", F.when(u(4) < 0.2, "Y").otherwise("N"))
           .withColumn("has_history", u(5) < 0.10)
    )
    # the current version was updated some time after creation
    current = current.withColumn(
        "updated_at",
        F.when(
            F.col("has_history"),
            F.to_timestamp(_random_date(u(6), F.date_add(F.to_date("created_at"), 1), F.lit(as_of))),
        ).otherwise(F.col("created_at"))
        + F.make_dt_interval(F.lit(0), F.floor(u(7) * 14 + 8).cast("int"), F.floor(u(8) * 60).cast("int")),
    )

    cols = ["customer_id", "first_name", "last_name", "email", "birth_date", "gender_code",
            "home_store_id", "city", "is_home_barista", "created_at", "updated_at"]

    # older version: different e-mail provider, sometimes another last name or city
    history = (
        current.where("has_history")
               .withColumn("email", F.regexp_replace(F.lower(F.trim("email")), "@example\\.com$", "@example.org"))
               .withColumn("last_name", F.when(u(9) < 0.3, _pick(u(10), ref.LAST_NAMES)).otherwise(F.col("last_name")))
               .withColumn("city", F.when(u(11) < 0.3, F.lit("Lüneburg")).otherwise(F.col("city")))
               .withColumn("updated_at", F.col("created_at"))
    )
    return current.select(*cols).unionByName(history.select(*cols))


def web_accounts(crm_current: DataFrame, window_start: date, as_of: date) -> DataFrame:
    """Webshop accounts. ~15 % of them belong to people who are also in the CRM (same e-mail)."""
    spark = _spark()
    n = ACTIVE_CUSTOMERS_PER_STORE + DORMANT_CUSTOMERS_PER_STORE
    df = (spark.range(1, n + 1).withColumnRenamed("id", "local_idx")
               .withColumn("account_id", (F.lit(ONLINE_ACCOUNT_BASE) + F.col("local_idx")).cast("int")))
    df = _with_person(df, "account_id", 300)
    u = lambda s: det_uniform(400 + s, F.col("account_id"))  # noqa: E731

    is_dormant = F.col("local_idx") > ACTIVE_CUSTOMERS_PER_STORE
    omni_customer = ((F.col("local_idx") % 5) + 1) * 100000 + F.col("local_idx")
    df = df.withColumn("omni_customer_id",
                       F.when(~is_dormant & (u(1) < OMNICHANNEL_SHARE), omni_customer.cast("int")))

    crm = crm_current.select(F.col("customer_id").alias("omni_customer_id"),
                             F.col("email").alias("crm_email"),
                             F.col("first_name").alias("crm_first_name"),
                             F.col("last_name").alias("crm_last_name"))
    df = df.join(crm, "omni_customer_id", "left")

    city_idx = (F.floor(u(2) * len(ref.ONLINE_CITIES)) + 1).cast("int")
    cities = F.array(*[F.lit(c[0]) for c in ref.ONLINE_CITIES])
    return df.select(
        "account_id",
        # web shop stores e-mails in lower case - the CRM does not
        F.coalesce(F.lower(F.trim("crm_email")), F.col("clean_email")).alias("email"),
        F.coalesce("crm_first_name", "first_name").alias("first_name"),
        F.coalesce("crm_last_name", "last_name").alias("last_name"),
        F.element_at(cities, city_idx).alias("city"),
        F.to_timestamp(
            F.when(is_dormant, _random_date(u(3), F.lit(window_start), F.lit(as_of)))
             .otherwise(_random_date(u(3), F.lit(date(2021, 1, 1)), F.lit(window_start)))
        ).alias("created_at"),
        F.when(u(4) < 0.4, F.lit(True)).otherwise(F.lit(False)).alias("newsletter_opt_in"),
    )


# ---------------------------------------------------------------------------
# Sales: orders -> POS (cafés) and web orders (online shop)
# ---------------------------------------------------------------------------

def _order_u(seed: int) -> Column:
    return det_uniform(seed, F.col("store_key"), F.col("date"), F.col("order_seq"))


def enrich_orders(orders: DataFrame, cashiers: Dict[int, List[int]], loyalty_cards: DataFrame,
                  window_end: date) -> DataFrame:
    """Add customer, timestamp, payment, status and loyalty card to each simulated order."""
    online = F.col("store_key") == ref.ONLINE_STORE_ID
    u = _order_u

    n_loyal = F.when(online, F.lit(ONLINE_LOYAL_CUSTOMERS)).otherwise(F.lit(LOYAL_CUSTOMERS))
    loyal_share = F.when(online, F.lit(ONLINE_LOYAL_ORDER_SHARE)).otherwise(F.lit(LOYAL_ORDER_SHARE))
    local_idx = F.when(
        u(11) < loyal_share, F.floor(u(12) * n_loyal) + 1
    ).otherwise(n_loyal + 1 + F.floor(u(12) * (F.lit(ACTIVE_CUSTOMERS_PER_STORE) - n_loyal)))
    customer_ref = (F.col("store_key") * 100000 + local_idx).cast("int")

    # cafés open 7:00-19:00 with a peak around lunch, the web shop is open 24/7
    hour = F.when(online, F.floor(u(14) * 16 + u(15) * 8)).otherwise(F.floor(F.lit(7) + (u(14) + u(15)) * 6))
    ts = F.make_timestamp(F.year("date"), F.month("date"), F.dayofmonth("date"),
                          hour.cast("int"), F.floor(u(16) * 60).cast("int"), F.floor(u(17) * 60).cast("int"))

    cashier_map = F.create_map(*[x for sid, ids in cashiers.items()
                                 for x in (F.lit(sid), F.array(*[F.lit(i) for i in ids]))])
    cashier_arr = F.element_at(cashier_map, F.col("store_key"))
    code = None
    for sid, c in ref.STORE_CODES.items():
        cond = F.col("store_key") == sid
        code = F.when(cond, F.lit(c)) if code is None else code.when(cond, F.lit(c))

    df = (
        orders
        .withColumn("customer_ref", customer_ref)
        .withColumn("is_guest_checkout", online & (u(13) < 0.08))
        .withColumn("order_ts", ts)
        .withColumn("order_id", F.concat(code, F.lit("-"), F.date_format("date", "yyyyMMdd"), F.lit("-"),
                                         F.lpad(F.col("order_seq").cast("string"), 4, "0")))
        .withColumn("register_no", (F.floor(u(18) * 3) + 1).cast("int"))
        .withColumn("cashier_id", F.element_at(cashier_arr, (F.floor(u(19) * F.size(cashier_arr)) + 1).cast("int")))
        .withColumn(
            "payment",
            F.when(online, weighted_choice(u(20), [("paypal", .45), ("klarna", .25), ("credit_card", .30)]))
             .otherwise(weighted_choice(u(20), [("B", .30), ("G", .35), ("K", .15), ("M", .20)])),
        )
        .withColumn(
            "status",
            F.when(online & (u(21) < 0.02), F.lit("cancelled"))
             .when(online & (F.col("date") >= F.lit(window_end - timedelta(days=3))), F.lit("shipped"))
             .when(online, F.lit("delivered"))
             .when(u(21) < 0.005, F.lit("V"))
             .otherwise(F.lit("C")),
        )
        .withColumn("card_scanned", u(22) < 0.95)
        .withColumn("voucher_code", F.when(online & (u(23) < 0.08), F.lit("WELCOME10")))
    )

    # the loyalty card that was valid on the order date (cafés only)
    cards = loyalty_cards.select(
        F.col("customer_id").alias("card_customer_id"), "card_no", "tier_code", "enrolled_on", "cancelled_on"
    )
    df = df.join(
        F.broadcast(cards),
        (~online)
        & F.col("card_scanned")
        & (F.col("customer_ref") == F.col("card_customer_id"))
        & (F.col("date") >= F.col("enrolled_on"))
        & (F.col("cancelled_on").isNull() | (F.col("date") < F.col("cancelled_on"))),
        "left",
    )
    return df.withColumn(
        "discount_pct",
        F.when(F.col("voucher_code").isNotNull(), F.lit(0.10))
         .when(F.col("tier_code") == "G", F.lit(0.10))
         .when(F.col("tier_code") == "S", F.lit(0.05))
         .otherwise(F.lit(0.0)),
    ).drop("card_customer_id", "enrolled_on", "cancelled_on", "card_scanned")


def order_lines(orders: DataFrame, prices: DataFrame, products: DataFrame) -> DataFrame:
    """Explode orders into 1-3 product lines and price them with the list price valid on the order date."""
    online = F.col("store_key") == ref.ONLINE_STORE_ID
    u = _order_u
    n_lines = F.when(online, weighted_choice(u(30), [(1, .45), (2, .35), (3, .20)])) \
               .otherwise(weighted_choice(u(30), [(1, .60), (2, .28), (3, .12)]))
    lines = orders.withColumn("raw_line", F.explode(F.sequence(F.lit(1), n_lines)))

    ul = lambda s: det_uniform(s, F.col("store_key"), F.col("date"), F.col("order_seq"), F.col("raw_line"))  # noqa: E731
    product = None
    for sid, weights in ref.PRODUCT_WEIGHTS_BY_STORE.items():
        cond = F.col("store_key") == sid
        choice = weighted_choice(ul(31), weights)
        product = F.when(cond, choice) if product is None else product.when(cond, choice)
    lines = lines.withColumn("product_id", product.cast("int"))

    # the same product only once per order
    order_key = ["store_key", "date", "order_seq"]
    lines = (lines.withColumn("_rn", F.row_number().over(Window.partitionBy(*order_key, "product_id").orderBy("raw_line")))
                  .where("_rn = 1")
                  .withColumn("line_no", F.row_number().over(Window.partitionBy(*order_key).orderBy("raw_line")))
                  .drop("_rn"))

    lines = lines.withColumn(
        "quantity",
        F.when(online, quantity_expr(ul(32), ul(33), .70, .22, .06))
         .otherwise(quantity_expr(ul(32), ul(33), .85, .11, .03)),
    )

    price_hist = (
        prices.withColumn("valid_to", F.date_sub(F.lead("valid_from").over(Window.partitionBy("product_id").orderBy("valid_from")), 1))
              .join(products.select("product_id", "sku"), "product_id")
    )
    lines = lines.join(
        F.broadcast(price_hist),
        (lines.product_id == price_hist.product_id)
        & (F.col("date") >= F.col("valid_from"))
        & (F.col("valid_to").isNull() | (F.col("date") <= F.col("valid_to"))),
        "inner",
    ).drop(price_hist.product_id)

    return lines.withColumn("unit_price", F.col("list_price_eur")).withColumn(
        "discount_amount",
        F.when(F.col("discount_pct") > 0, F.round(F.col("quantity") * F.col("unit_price") * F.col("discount_pct"), 2)),
    )


def _inject_data_quality_issues(cafe_lines: DataFrame, cafe_orders: DataFrame) -> DataFrame:
    """20 lines with quantity -1 and a handful of register test scans with an unknown SKU."""
    h = F.xxhash64("order_id", "line_no", F.lit(77))
    ranked = cafe_lines.withColumn("_r", F.row_number().over(Window.orderBy(h)))
    lines = ranked.withColumn("quantity", F.when(F.col("_r") <= 20, F.lit(-1)).otherwise(F.col("quantity"))).drop("_r")

    test_orders = (
        cafe_orders.where("status = 'C'")
                   .withColumn("_r", F.row_number().over(Window.orderBy(F.xxhash64("order_id", F.lit(99)))))
                   .where("_r <= 12")
                   .select("order_id")
    )
    max_line = lines.groupBy("order_id").agg(F.max("line_no").alias("max_line"))
    test_lines = test_orders.join(max_line, "order_id").select(
        "order_id",
        (F.col("max_line") + 1).alias("line_no"),
        F.lit("SB-9999").alias("sku"),
        F.lit(1).alias("quantity"),
        F.lit(0.0).alias("unit_price"),
        F.lit(None).cast("double").alias("discount_amount"),
    )
    return lines.unionByName(test_lines)


def pos_tables(orders: DataFrame, lines: DataFrame) -> Dict[str, DataFrame]:
    cafe_orders = orders.where(F.col("store_key") != ref.ONLINE_STORE_ID)
    transactions = cafe_orders.select(
        F.col("order_id").alias("transaction_id"),
        F.col("store_key").alias("store_id"),
        "register_no",
        "cashier_id",
        F.col("order_ts").alias("transaction_ts"),
        F.col("card_no").alias("loyalty_card_no"),
        F.col("payment").alias("payment_code"),
        F.col("status").alias("status_code"),
    )
    cafe_lines = lines.where(F.col("store_key") != ref.ONLINE_STORE_ID).select(
        "order_id", "line_no", "sku", "quantity", "unit_price", "discount_amount"
    )
    cafe_lines = _inject_data_quality_issues(cafe_lines, cafe_orders)
    transaction_lines = cafe_lines.select(
        F.col("order_id").alias("transaction_id"), "line_no", "sku", "quantity", "unit_price", "discount_amount"
    )
    return {
        "pos_transactions": transactions.orderBy("transaction_id"),
        "pos_transaction_lines": transaction_lines.orderBy("transaction_id", "line_no"),
    }


def web_orders(orders: DataFrame, lines: DataFrame, accounts: DataFrame) -> DataFrame:
    """Nested web shop orders: one JSON document per order with an array of items."""
    online_orders = orders.where(F.col("store_key") == ref.ONLINE_STORE_ID)
    items = (
        lines.where(F.col("store_key") == ref.ONLINE_STORE_ID)
             .groupBy("order_id")
             .agg(F.sort_array(F.collect_list(F.struct(
                 F.col("line_no").alias("_pos"),
                 F.col("sku"),
                 F.col("quantity").alias("qty"),
                 F.col("unit_price"),
                 F.col("discount_amount").alias("discount"),
             ))).alias("_items"))
             .select("order_id", F.transform("_items", lambda x: x.dropFields("_pos")).alias("items"))
    )
    acct = accounts.select(F.col("account_id").alias("_acct"), F.col("city").alias("_acct_city"))
    df = (online_orders.join(items, "order_id")
                       .withColumn("account_id", F.when(~F.col("is_guest_checkout"), F.col("customer_ref")))
                       .join(acct, F.col("account_id") == F.col("_acct"), "left"))

    u = _order_u
    city_states = {c: s for c, s, _ in ref.ONLINE_CITIES}
    city_prefix = {c: p for c, _, p in ref.ONLINE_CITIES}
    guest_city = _pick(u(40), [c for c, _, _ in ref.ONLINE_CITIES])
    city = F.coalesce(F.col("_acct_city"), guest_city)
    state_expr = prefix_expr = None
    for c in city_states:
        cond = city == c
        state_expr = F.when(cond, F.lit(city_states[c])) if state_expr is None else state_expr.when(cond, F.lit(city_states[c]))
        prefix_expr = F.when(cond, F.lit(city_prefix[c])) if prefix_expr is None else prefix_expr.when(cond, F.lit(city_prefix[c]))

    return df.select(
        "order_id",
        "account_id",
        F.date_format("order_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("order_ts"),
        "status",
        F.struct(F.col("payment").alias("method"),
                 F.concat(F.lit("PSP-"), F.substring(F.sha2(F.col("order_id"), 256), 1, 10)).alias("psp_reference")
                 ).alias("payment"),
        F.struct(city.alias("city"), state_expr.alias("state"),
                 F.concat(prefix_expr, F.lpad(F.floor(u(41) * 1000).cast("string"), 3, "0")).alias("postal_code"),
                 F.lit("DE").alias("country")).alias("shipping"),
        "voucher_code",
        "items",
    ).orderBy("order_id")


def cashiers_by_store(employee_rows: List[tuple]) -> Dict[int, List[int]]:
    result: Dict[int, List[int]] = {}
    for emp_id, _f, _l, title, store_id, _m, _h in employee_rows:
        if store_id is not None:
            result.setdefault(store_id, []).append(emp_id)
    return result


def as_of_dates(as_of: date):
    """Return (window_start, window_end): the rolling 24-month sales window ending yesterday."""
    window_end = as_of - timedelta(days=1)
    window_start = ref.minus_years(as_of, 2)
    return window_start, window_end
