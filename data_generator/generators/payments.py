"""Payment transactions generator (vectorized).

One payment per order. Amount = sum of the order's item
totals; payment status derived from the order status so
the datasets stay consistent with each other.
"""

import numpy as np
import pandas as pd

# order_status -> payment_status
STATUS_MAP = {
    "completed": "captured",
    "partially_refunded": "partially_refunded",
    "refunded": "refunded",
    "cancelled": "failed",
    "pending": "pending",
}

PAYMENT_PROVIDERS = {
    "Credit Card": ["Adyen", "Stripe"],
    "Debit Card": ["Adyen", "Stripe"],
    "PayPal": ["PayPal"],
    "Apple Pay": ["Adyen", "Stripe"],
    "Google Pay": ["Adyen", "Stripe"],
    "Bank Transfer": ["Klarna", "SEPA Direct"],
}


def generate_payments(orders, order_items):
    n = len(orders)

    # 1. Amount per order = sum of its item totals.
    #    Orders whose items were all deduped away (rare)
    #    or with no items get 0.0 -> filled below.
    amounts = (
        order_items
        .groupby("order_id")["total_amount"]
        .sum()
    )

    payments = orders[
        ["order_id", "customer_id", "order_timestamp",
         "order_status", "payment_method"]
    ].copy()

    payments["amount"] = (
        payments["order_id"].map(amounts).fillna(0.0).round(2)
    )

    # 2. Payment status derived from order status
    payments["payment_status"] = (
        payments["order_status"].map(STATUS_MAP)
    )

    # 3. Payment timestamp = order timestamp + 0-120 s
    payments["payment_timestamp"] = (
        payments["order_timestamp"] +
        pd.to_timedelta(
            np.random.randint(0, 121, n),
            unit="s"
        )
    )

    # 4. Provider consistent with the payment method
    rng_pick = np.random.randint(0, 2, n)
    payments["payment_provider"] = [
        PAYMENT_PROVIDERS[m][pick % len(PAYMENT_PROVIDERS[m])]
        for m, pick in zip(
            payments["payment_method"].values,
            rng_pick
        )
    ]

    # 5. IDs and final column layout
    payments.insert(
        0,
        "payment_id",
        "PAY" +
        pd.Series(np.arange(1, n + 1))
        .astype(str)
        .str.zfill(8)
    )

    payments["currency"] = "EUR"

    payments = payments[[
        "payment_id",
        "order_id",
        "customer_id",
        "amount",
        "currency",
        "payment_method",
        "payment_provider",
        "payment_status",
        "payment_timestamp",
    ]]

    return payments
