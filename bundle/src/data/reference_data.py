"""Static master / reference data for the Sunny Bay Roastery source systems.

Everything in this module is plain Python (no Spark) so it is easy to read and
to reuse. The Spark builders in ``source_systems.py`` turn these lists into
DataFrames and the raw files that land in the bronze Volume.

Sunny Bay Roastery was founded in 2013 in Hamburg and runs cafés in five German
cities plus an online shop. All prices are in EUR.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def first_of_month(d: date, months_ahead: int = 0) -> date:
    """Return the first day of the month ``months_ahead`` months after ``d``."""
    month_index = d.year * 12 + (d.month - 1) + months_ahead
    return date(month_index // 12, month_index % 12 + 1, 1)


def minus_years(d: date, years: int) -> date:
    """Subtract whole years, mapping Feb 29 to Feb 28 when needed."""
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d.replace(year=d.year - years, day=28)


# ---------------------------------------------------------------------------
# German public holidays (only those that apply in ALL 16 Bundesländer)
# ---------------------------------------------------------------------------

def easter_sunday(year: int) -> date:
    """Gregorian Easter Sunday (anonymous Gregorian algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def german_public_holidays(start_year: int, end_year: int) -> List[Tuple[date, str]]:
    """Nationwide German public holidays (gesetzliche Feiertage in allen Bundesländern)."""
    holidays: List[Tuple[date, str]] = []
    for year in range(start_year, end_year + 1):
        easter = easter_sunday(year)
        holidays += [
            (date(year, 1, 1), "Neujahr"),
            (easter - timedelta(days=2), "Karfreitag"),
            (easter + timedelta(days=1), "Ostermontag"),
            (date(year, 5, 1), "Tag der Arbeit"),
            (easter + timedelta(days=39), "Christi Himmelfahrt"),
            (easter + timedelta(days=50), "Pfingstmontag"),
            (date(year, 10, 3), "Tag der Deutschen Einheit"),
            (date(year, 12, 25), "1. Weihnachtstag"),
            (date(year, 12, 26), "2. Weihnachtstag"),
        ]
    return sorted(holidays)


# ---------------------------------------------------------------------------
# Stores (ERP)
# ---------------------------------------------------------------------------

ONLINE_STORE_ID = 6

# store_id -> short code used in POS transaction ids
STORE_CODES: Dict[int, str] = {1: "HH01", 2: "BE01", 3: "BY01", 4: "NW01", 5: "HE01", 6: "WEB"}

STORE_COLUMNS = [
    "store_id", "store_name", "store_type", "city", "neighborhood_or_channel",
    "opened_on", "is_online", "store_area_sqm", "seating_capacity", "manager_employee_id",
    "country_name", "country_iso2", "country_iso3", "state_province", "state_iso2",
    "county_district", "postal_code", "street_address", "latitude", "longitude",
]


def stores(frankfurt_opened_on: date) -> List[tuple]:
    """Store master data. Frankfurt opens inside the generated sales window."""
    return [
        (1, "Sunny Bay – Hamburg Schanze", "Retail Cafe", "Hamburg", "Sternschanze",
         date(2013, 4, 2), False, 120.0, 45, 1101,
         "Germany", "DE", "DEU", "Hamburg", "HH", "Bezirk Altona", "20357", "Schulterblatt 58",
         53.5636, 9.9636),
        (2, "Sunny Bay – Berlin Kreuzberg", "Retail Cafe", "Berlin", "Kreuzberg",
         date(2015, 9, 1), False, 100.0, 38, 1102,
         "Germany", "DE", "DEU", "Berlin", "BE", "Friedrichshain-Kreuzberg", "10999", "Oranienstraße 21",
         52.4995, 13.4180),
        (3, "Sunny Bay – München Maxvorstadt", "Retail Cafe", "München", "Maxvorstadt",
         date(2017, 5, 15), False, 80.0, 25, 1103,
         "Germany", "DE", "DEU", "Bayern", "BY", "Kreisfreie Stadt München", "80799", "Amalienstraße 45",
         48.1508, 11.5797),
        (4, "Sunny Bay – Köln Ehrenfeld", "Retail Cafe", "Köln", "Ehrenfeld",
         date(2019, 3, 1), False, 90.0, 30, 1104,
         "Germany", "DE", "DEU", "Nordrhein-Westfalen", "NW", "Kreisfreie Stadt Köln", "50823", "Venloer Straße 310",
         50.9497, 6.9180),
        (5, "Sunny Bay – Frankfurt Bankenviertel", "Retail Cafe", "Frankfurt am Main", "Bankenviertel / Office Hub",
         frankfurt_opened_on, False, 110.0, 40, 1105,
         "Germany", "DE", "DEU", "Hessen", "HE", "Kreisfreie Stadt Frankfurt am Main", "60311", "Neue Mainzer Straße 12",
         50.1125, 8.6720),
        (6, "Sunny Bay Online", "Online Shop", "Hamburg", "E-Commerce / Home Barista",
         date(2021, 1, 1), True, None, None, 1106,
         "Germany", "DE", "DEU", "Hamburg", "HH", "Bezirk Hamburg-Mitte", "20457", "Am Sandtorkai 30",
         53.5430, 9.9930),
    ]


# ---------------------------------------------------------------------------
# Product categories (ERP) – a small 3-level tree: root -> category -> subcategory
# ---------------------------------------------------------------------------

PRODUCT_CATEGORY_COLUMNS = ["category_id", "parent_category_id", "category_name"]

PRODUCT_CATEGORIES: List[tuple] = [
    (1, None, "All Products"),
    (10, 1, "Drink"),
    (20, 1, "Food"),
    (30, 1, "Beans"),
    (11, 10, "Espresso"),
    (12, 10, "Milk Coffee"),
    (13, 10, "Black Coffee"),
    (14, 10, "Brewed Coffee"),
    (15, 10, "Iced Coffee"),
    (16, 10, "Specialty"),
    (17, 10, "Seasonal"),
    (18, 10, "Cold Brew"),
    (19, 10, "Bulk / Office"),
    (21, 20, "Pastry"),
    (22, 20, "Cake"),
    (23, 20, "Brunch"),
    (31, 30, "Single Origin"),
    (32, 30, "Blend"),
    (33, 30, "Blend / Decaf"),
]


# ---------------------------------------------------------------------------
# Products (ERP)
# ---------------------------------------------------------------------------
# channel_code: S = café only, O = online only, B = both
# status_code:  A = active, D = discontinued
# tax_class:    S = standard VAT (19 %), R = reduced VAT (7 %)

PRODUCT_COLUMNS = [
    "product_id", "sku", "product_name", "category_id", "is_beans",
    "channel_code", "status_code", "tax_class", "launched_on",
]

# product_id, name, category_id, is_beans, channel, status, tax_class, current list price, current COGS
_PRODUCTS: List[tuple] = [
    (101, "Single Espresso",             11, "N", "S", "A", "S",  2.60, 0.45),
    (102, "Sunny Bay Latte",             12, "N", "S", "A", "S",  4.20, 0.95),
    (103, "Cappuccino",                  12, "N", "S", "A", "S",  3.90, 0.85),
    (104, "Flat White",                  12, "N", "S", "A", "S",  4.30, 1.00),
    (105, "Americano",                   13, "N", "S", "A", "S",  3.20, 0.40),
    (201, "Butter Croissant",            21, "N", "S", "A", "S",  2.90, 1.00),
    (202, "Franzbrötchen",               21, "N", "S", "A", "S",  3.20, 1.10),
    (203, "Avocado Toast",               23, "N", "S", "A", "S",  8.90, 2.90),
    (301, "Filterkaffee",                14, "N", "S", "A", "S",  2.90, 0.35),
    (302, "Iced Latte",                  15, "N", "S", "A", "S",  4.60, 1.15),
    (303, "Matcha Latte",                16, "N", "S", "A", "S",  4.80, 1.60),
    (401, "Seasonal Signature Drink",    17, "N", "S", "A", "S",  5.20, 1.70),
    (402, "Pumpkin Spice Latte",         17, "N", "S", "D", "S",  5.40, 1.80),
    (501, "Cold Brew Bottle 330ml",      18, "N", "B", "A", "S",  4.90, 1.70),
    (601, "Office Coffee Box 3L",        19, "N", "S", "A", "S", 22.00, 5.50),
    (701, "Iced Matcha Tonic",           16, "N", "S", "A", "S",  5.50, 1.90),
    (9001, "Ethiopia Sidamo Beans 250g", 31, "Y", "B", "A", "R", 14.90, 6.20),
    (9002, "Colombia Supremo Beans 250g", 31, "Y", "B", "A", "R", 14.50, 5.90),
    (9003, "Brazil Cerrado Beans 250g",  31, "Y", "B", "A", "R", 13.90, 5.50),
    (9004, "Guatemala Antigua Beans 250g", 31, "Y", "O", "A", "R", 15.50, 6.60),
    (9005, "Kenya AA Beans 250g",        31, "Y", "B", "A", "R", 15.90, 6.80),
    (9006, "Costa Rica Tarrazu 250g",    31, "Y", "O", "A", "R", 15.50, 6.50),
    (9007, "Sunny Bay House Blend 1kg",  32, "Y", "B", "A", "R", 32.00, 13.00),
    (9008, "Decaf Specialty Blend 250g", 33, "Y", "B", "A", "R", 14.90, 6.40),
    (9009, "Panama Geisha Beans 100g",   31, "Y", "O", "A", "R", 24.90, 12.00),
]

# Products that are listed in the ERP but have not been sold in the sales window
# (discontinued before the window, or launching in the near future).
UPCOMING_PRODUCTS = {701: 14, 9009: 30}   # product_id -> launch in N days after as_of
DISCONTINUED_PRODUCTS = {402}


def _launched_on(product_id: int, as_of: date) -> date:
    if product_id in UPCOMING_PRODUCTS:
        return as_of + timedelta(days=UPCOMING_PRODUCTS[product_id])
    if product_id == 402:
        return date(2019, 9, 1)
    if product_id >= 9000:
        return date(2020, 11, 1)
    if product_id in (203, 303, 401, 501, 601):
        return date(2018, 3, 1)
    return date(2013, 4, 2)


def products(as_of: date) -> List[tuple]:
    rows = []
    for pid, name, cat, is_beans, channel, status, tax, _price, _cogs in _PRODUCTS:
        rows.append((pid, f"SB-{pid:04d}", name, cat, is_beans, channel, status, tax, _launched_on(pid, as_of)))
    return rows


def _round_to(value: float, step: float) -> float:
    return round(round(value / step) * step, 2)


PRODUCT_PRICE_COLUMNS = ["product_id", "valid_from", "list_price_eur", "cost_of_goods_eur"]


def product_prices(as_of: date, window_start: date) -> List[tuple]:
    """Price history. Each row is valid from ``valid_from`` until the next row of the same product.

    * Drinks and food get one price increase inside the sales window.
    * Beans get two increases (coffee bean prices are more volatile).
    * New products only have their launch price, the discontinued one only its old price.
    """
    drink_change = first_of_month(window_start, 9)
    bean_change_1 = first_of_month(window_start, 4)
    bean_change_2 = first_of_month(window_start, 16)

    rows = []
    for pid, _name, _cat, is_beans, _ch, _st, _tax, price, cogs in _PRODUCTS:
        launched = _launched_on(pid, as_of)
        if pid in UPCOMING_PRODUCTS or pid in DISCONTINUED_PRODUCTS:
            rows.append((pid, launched, price, cogs))
        elif is_beans == "Y":
            rows.append((pid, launched, _round_to(price / 1.12, 0.10), _round_to(cogs / 1.15, 0.05)))
            rows.append((pid, bean_change_1, _round_to(price / 1.05, 0.10), _round_to(cogs / 1.06, 0.05)))
            rows.append((pid, bean_change_2, price, cogs))
        else:
            rows.append((pid, launched, _round_to(price / 1.07, 0.10), _round_to(cogs / 1.08, 0.05)))
            rows.append((pid, drink_change, price, cogs))
    return rows


# Product mix per store: (product_id, share of order lines)
PRODUCT_WEIGHTS_BY_STORE: Dict[int, List[Tuple[int, float]]] = {
    1: [(101, .20), (102, .22), (103, .14), (104, .10), (105, .08), (201, .05), (202, .06),
        (301, .05), (302, .05), (501, .02), (9007, .02), (9001, .01)],
    2: [(101, .13), (102, .22), (103, .10), (201, .12), (202, .06), (301, .13), (302, .10),
        (303, .08), (9003, .03), (9008, .03)],
    3: [(102, .20), (103, .15), (201, .15), (202, .05), (401, .15), (301, .10), (302, .10),
        (501, .05), (9002, .03), (9005, .02)],
    4: [(102, .20), (103, .10), (201, .15), (202, .10), (203, .10), (401, .10), (501, .10),
        (302, .10), (9007, .05)],
    5: [(101, .28), (104, .18), (105, .20), (102, .10), (301, .10), (302, .05), (601, .06), (203, .03)],
    6: [(9001, .18), (9002, .17), (9003, .14), (9004, .13), (9005, .10), (9006, .10), (9007, .08),
        (9008, .05), (501, .05)],
}


# ---------------------------------------------------------------------------
# Employees (HR) – an org chart with a variable depth
# ---------------------------------------------------------------------------

EMPLOYEE_COLUMNS = ["employee_id", "first_name", "last_name", "job_title", "store_id", "manager_id", "hired_on"]

FIRST_NAMES_F = ["Anna", "Lena", "Marie", "Sophie", "Laura", "Julia", "Lea", "Hannah", "Emma", "Mia",
                 "Clara", "Johanna", "Katharina", "Sarah", "Lisa", "Paula", "Nele", "Greta", "Ida", "Frieda",
                 "Aylin", "Elif", "Zoe", "Amelie", "Charlotte"]
FIRST_NAMES_M = ["Lukas", "Felix", "Jonas", "Leon", "Paul", "Finn", "Max", "Elias", "Noah", "Ben",
                 "Tim", "Hendrik", "Moritz", "Niklas", "David", "Tobias", "Florian", "Emil", "Theo", "Anton",
                 "Can", "Mehmet", "Luca", "Matteo", "Oskar"]
LAST_NAMES = ["Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Schulz",
              "Hoffmann", "Schäfer", "Koch", "Bauer", "Richter", "Klein", "Wolf", "Schröder", "Neumann",
              "Schwarz", "Zimmermann", "Braun", "Krüger", "Hofmann", "Hartmann", "Lange", "Schmitt",
              "Werner", "Krause", "Meier", "Lehmann", "Yilmaz", "Kaya", "Nowak", "Kowalski", "Rossi"]


def employees(store_rows: List[tuple]) -> List[tuple]:
    """Build a deterministic org chart.

    Founder (CEO) -> Heads of Retail / E-Commerce / Finance / Roastery
    -> store managers -> shift leads -> baristas.
    """
    import random

    rng = random.Random(2013)

    def name(i: int) -> Tuple[str, str]:
        first_pool = FIRST_NAMES_F if i % 2 == 0 else FIRST_NAMES_M
        return rng.choice(first_pool), rng.choice(LAST_NAMES)

    rows: List[tuple] = [
        (1000, "Benno", "Bohne", "Founder & CEO", None, None, date(2013, 1, 1)),
        (1001, "Katrin", "Fuchs", "Head of Finance", None, 1000, date(2014, 2, 1)),
        (1002, "Jonas", "Petersen", "Head of Retail", None, 1000, date(2014, 6, 1)),
        (1003, "Aylin", "Demir", "Head of E-Commerce", None, 1000, date(2020, 9, 1)),
        (1004, "Lars", "Jensen", "Head Roaster", None, 1000, date(2013, 3, 1)),
        (1010, "Mila", "Hansen", "Financial Analyst", None, 1001, date(2021, 4, 1)),
        (1011, "Theo", "Brandt", "Roaster", None, 1004, date(2016, 8, 1)),
    ]

    next_id = 2000
    for store in store_rows:
        store_id, opened_on, manager_id = store[0], store[5], store[9]
        first, last = name(manager_id)
        if store_id == ONLINE_STORE_ID:
            rows.append((manager_id, first, last, "E-Commerce Lead", store_id, 1003, opened_on))
            for title in ("Fulfilment Specialist", "Fulfilment Specialist", "Customer Service Agent"):
                first, last = name(next_id)
                rows.append((next_id, first, last, title, store_id, manager_id, opened_on))
                next_id += 1
            continue

        rows.append((manager_id, first, last, "Store Manager", store_id, 1002, opened_on))
        shift_lead_id = next_id
        first, last = name(next_id)
        rows.append((shift_lead_id, first, last, "Shift Lead", store_id, manager_id, opened_on))
        next_id += 1
        n_baristas = 4 + store_id % 3
        for _ in range(n_baristas):
            first, last = name(next_id)
            rows.append((next_id, first, last, "Barista", store_id, shift_lead_id, opened_on))
            next_id += 1
    return rows


# ---------------------------------------------------------------------------
# Cities for online customers / shipping addresses
# ---------------------------------------------------------------------------

# city, state (Bundesland), postal code prefix
ONLINE_CITIES: List[Tuple[str, str, str]] = [
    ("Hamburg", "HH", "20"), ("Berlin", "BE", "10"), ("München", "BY", "80"), ("Köln", "NW", "50"),
    ("Frankfurt am Main", "HE", "60"), ("Stuttgart", "BW", "70"), ("Düsseldorf", "NW", "40"),
    ("Leipzig", "SN", "04"), ("Dresden", "SN", "01"), ("Hannover", "NI", "30"), ("Bremen", "HB", "28"),
    ("Nürnberg", "BY", "90"), ("Freiburg im Breisgau", "BW", "79"), ("Münster", "NW", "48"),
    ("Kiel", "SH", "24"), ("Mainz", "RP", "55"),
]
