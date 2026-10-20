"""Order header and order items generators (vectorized).

Seasonality model
-----------------
Order volume is not uniform across the calendar. Three effects are
layered on top of each other, all applied at day level:

    monthly factor  x  yearly growth  x  day-of-week factor

plus event windows (Black Friday / Cyber Monday, the pre-Christmas
run-up, the dead week between Christmas and New Year).

The shape aims at a realistic European e-commerce year:
November is the peak, February is the trough - customers have spent
their money over the holidays and the January clearance, so the
"broke month" shows up clearly in the charts.

Discounts follow their own calendar, which is deliberately NOT the
same as the volume calendar: January and February are heavy discount
months with low volume (clearance), November is heavy on both.
That decoupling is what makes revenue and margin move independently,
the way they do in a real shop.
"""

import numpy as np
import pandas as pd

from generators.config import START_DATE, END_DATE
from generators.customers import SEGMENT_ORDER_RATES

# ---------------------------------------------------------------
# seasonality
# ---------------------------------------------------------------

# relative order volume per calendar month (1.00 = an average month)
MONTH_FACTOR = {
    1: 0.66,   # January  - post-holiday slump, but clearance keeps some traffic
    2: 0.58,   # February - the weakest month of the year
    3: 0.85,
    4: 0.92,
    5: 0.96,
    6: 0.94,
    7: 1.02,   # July     - summer sale
    8: 0.88,   # August   - holidays, people are away
    9: 1.00,
    10: 1.10,
    11: 1.85,  # November - Black Friday
    12: 1.50,  # December - Christmas
}

# year-on-year growth of the business
YEAR_GROWTH = {
    2023: 1.00,
    2024: 1.18,
    2025: 1.38,
}

# Monday=0 ... Sunday=6
DOW_FACTOR = {
    0: 1.05, 1: 1.02, 2: 1.00, 3: 1.02,
    4: 0.98, 5: 0.90, 6: 1.03,
}

# ---------------------------------------------------------------
# discounts
# ---------------------------------------------------------------

DISCOUNT_RATES = np.array([0.00, 0.05, 0.10, 0.20, 0.30, 0.40])

# probability of each discount rate, per discount regime
DISCOUNT_REGIMES = {
    "regular":          [0.72, 0.15, 0.08, 0.04, 0.01, 0.00],
    "winter_clearance": [0.35, 0.20, 0.20, 0.15, 0.07, 0.03],
    "summer_sale":      [0.45, 0.20, 0.18, 0.12, 0.04, 0.01],
    "black_friday":     [0.20, 0.15, 0.22, 0.23, 0.13, 0.07],
    "christmas":        [0.40, 0.22, 0.20, 0.13, 0.04, 0.01],
}

MONTH_REGIME = {
    1: "winter_clearance",
    2: "winter_clearance",
    3: "regular",
    4: "regular",
    5: "regular",
    6: "regular",
    7: "summer_sale",
    8: "summer_sale",
    9: "regular",
    10: "regular",
    11: "black_friday",
    12: "christmas",
}

_REGIME_NAMES = list(DISCOUNT_REGIMES.keys())
_REGIME_INDEX = {name: i for i, name in enumerate(_REGIME_NAMES)}
_REGIME_CUM = np.cumsum(
    np.array([DISCOUNT_REGIMES[name] for name in _REGIME_NAMES]),
    axis=1,
)


def _daily_weights():
    """Probability of an order falling on each calendar day."""
    days = pd.date_range(START_DATE, END_DATE, freq="D")

    w = np.array([
        MONTH_FACTOR[d.month]
        * YEAR_GROWTH[d.year]
        * DOW_FACTOR[d.dayofweek]
        for d in days
    ])

    # event windows, applied on top of the monthly shape
    for i, d in enumerate(days):
        # Black Friday week through Cyber Monday
        if d.month == 11 and 22 <= d.day <= 30:
            w[i] *= 2.4
        # pre-Christmas run-up
        elif d.month == 12 and 1 <= d.day <= 20:
            w[i] *= 1.25
        # dead week between Christmas and New Year
        elif d.month == 12 and d.day >= 25:
            w[i] *= 0.45
        # first half of January: clearance traffic, still weak
        elif d.month == 1 and d.day <= 15:
            w[i] *= 1.15

    return days, w / w.sum()


def generate_orders(customers, n_orders):
    customer_weights = customers[
        "customer_segment"
    ].map(SEGMENT_ORDER_RATES).values.astype(float)

    customer_weights = (
        customer_weights /
        customer_weights.sum()
    )

    customer_ids = np.random.choice(
        customers["customer_id"].values,
        size=n_orders,
        p=customer_weights
    )

    # --- seasonal timestamps -----------------------------------
    days, day_probs = _daily_weights()

    day_idx = np.random.choice(
        len(days), size=n_orders, p=day_probs
    )

    # shopping happens during the day, not uniformly at night:
    # peak late morning and evening
    hour_weights = np.array([
        0.5, 0.3, 0.2, 0.2, 0.3, 0.6,   # 00-05
        1.2, 2.2, 3.4, 4.6, 5.6, 6.0,   # 06-11
        5.8, 5.4, 5.2, 5.4, 5.8, 6.4,   # 12-17
        7.2, 8.0, 8.6, 7.6, 5.4, 2.6,   # 18-23
    ])
    hour = np.random.choice(
        np.arange(24),
        size=n_orders,
        p=hour_weights / hour_weights.sum(),
    )

    timestamps = (
        pd.to_datetime(days.values[day_idx])
        + pd.to_timedelta(hour, unit="h")
        + pd.to_timedelta(
            np.random.randint(0, 3600, n_orders), unit="s"
        )
    )

    statuses = np.random.choice(
        [
            "completed",
            "cancelled",
            "refunded",
            "partially_refunded",
            "pending"
        ],
        size=n_orders,
        p=[0.82, 0.07, 0.05, 0.03, 0.03]
    )

    order_ids = (
        "O" +
        pd.Series(np.arange(1, n_orders + 1))
        .astype("str")
        .str.zfill(7)
    )

    orders = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": customer_ids,
        "order_timestamp": timestamps,
        "order_status": statuses,
        "currency": "EUR",
        "payment_method": np.random.choice(
            [
                "Credit Card",
                "Debit Card",
                "PayPal",
                "Apple Pay",
                "Google Pay",
                "Bank Transfer"
            ],
            n_orders
        ),
        "sales_channel": np.random.choice(
            ["Web", "Mobile App", "Marketplace"],
            n_orders,
            p=[0.55, 0.35, 0.10]
        ),
        "coupon_code": np.random.choice(
            [
                None,
                "WELCOME10",
                "SUMMER20",
                "BLACKFRIDAY",
                "VIP15"
            ],
            n_orders,
            p=[0.65, 0.10, 0.08, 0.10, 0.07]
        )
    })

    return orders


def generate_order_items(orders, products):
    n_orders = len(orders)

    # 1. How many items in each order (drawn all at once)
    n_items_per_order = np.random.choice(
        [1, 2, 3, 4, 5],
        size=n_orders,
        p=[0.45, 0.30, 0.15, 0.07, 0.03]
    )

    total_items = int(n_items_per_order.sum())

    # 2. Repeat each order_id by its item count
    order_ids = np.repeat(
        orders["order_id"].values,
        n_items_per_order
    )

    # month of each line, inherited from its order -> drives discounts
    order_months = pd.to_datetime(
        orders["order_timestamp"]
    ).dt.month.values
    line_months = np.repeat(order_months, n_items_per_order)

    # 3. Pick a random product for every item row
    #    (with replacement; duplicates within an order
    #    are dropped below -- the vectorized equivalent
    #    of replace=False, losing ~0.1% of rows)
    product_idx = np.random.randint(
        0,
        len(products),
        size=total_items
    )

    product_ids = products["product_id"].values[product_idx]
    unit_prices = products["list_price"].values[product_idx]

    # 4. Quantities, and seasonal discounts
    quantities = np.random.choice(
        [1, 2, 3, 4],
        size=total_items,
        p=[0.70, 0.20, 0.08, 0.02]
    )

    # each line follows the discount regime of its month:
    # inverse-CDF sampling, vectorized across regimes
    regime_of_line = np.array([
        _REGIME_INDEX[MONTH_REGIME[m]] for m in line_months
    ])
    u = np.random.random(total_items)
    rate_idx = (u[:, None] > _REGIME_CUM[regime_of_line]).sum(axis=1)
    discount_rates = DISCOUNT_RATES[rate_idx]

    # 5. Money columns as pure array arithmetic
    gross = quantities * unit_prices
    discounts = gross * discount_rates
    taxes = (gross - discounts) * 0.19
    totals = gross - discounts + taxes

    items = pd.DataFrame({
        "order_id": order_ids,
        "product_id": product_ids,
        "quantity": quantities,
        "unit_price": np.round(unit_prices, 2),
        "discount_amount": np.round(discounts, 2),
        "tax_amount": np.round(taxes, 2),
        "total_amount": np.round(totals, 2)
    })

    # 6. Enforce distinct products within an order
    items = items.drop_duplicates(
        subset=["order_id", "product_id"]
    ).reset_index(drop=True)

    # 7. Sequential IDs assigned after dedup
    items.insert(
        0,
        "order_item_id",
        "OI" +
        pd.Series(np.arange(1, len(items) + 1))
        .astype("str")
        .str.zfill(8)
    )

    return items
