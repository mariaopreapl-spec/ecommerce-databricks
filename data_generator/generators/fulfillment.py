"""Fulfillment generators: shipments and returns (vectorized).

Shipments: one per shipped order (orders that reached the customer:
completed, refunded, partially_refunded). Cancelled and pending
orders have no shipment.

Returns: driven by order status -- all items of fully refunded
orders plus a share of items from partially refunded orders.
Return dates always follow the delivery date of the shipment.
"""

import numpy as np
import pandas as pd

CARRIERS = ["DHL", "DPD", "GLS", "Hermes", "UPS"]
CARRIER_WEIGHTS = [0.35, 0.20, 0.15, 0.15, 0.15]

RETURN_REASONS = [
    "Damaged item",
    "Wrong item delivered",
    "Not as described",
    "Changed mind",
    "Better price elsewhere",
    "Arrived too late",
]
RETURN_REASON_WEIGHTS = [0.15, 0.10, 0.20, 0.35, 0.10, 0.10]

SHIPPED_STATUSES = ["completed", "refunded", "partially_refunded"]


def generate_shipments(orders):
    shipped = orders[
        orders["order_status"].isin(SHIPPED_STATUSES)
    ].reset_index(drop=True)

    n = len(shipped)

    shipped_at = (
        shipped["order_timestamp"] +
        pd.to_timedelta(
            np.random.randint(4, 72, n), unit="h"
        )
    )

    delivered_at = (
        shipped_at +
        pd.to_timedelta(
            np.random.randint(24, 168, n), unit="h"
        )
    )

    shipments = pd.DataFrame({
        "shipment_id": (
            "SH" +
            pd.Series(np.arange(1, n + 1))
            .astype("str")
            .str.zfill(8)
        ),
        "order_id": shipped["order_id"].values,
        "carrier": np.random.choice(
            CARRIERS, size=n, p=CARRIER_WEIGHTS
        ),
        "tracking_number": (
            "TRK" +
            pd.Series(
                np.random.randint(10**9, 10**10, n, dtype=np.int64)
            ).astype("str")
        ),
        "shipped_at": shipped_at.values,
        "delivered_at": delivered_at.values,
        "shipping_cost": np.round(
            np.random.uniform(2.99, 12.99, n), 2
        ),
        "shipment_status": "delivered",
    })

    return shipments


def generate_returns(orders, order_items, shipments):
    # Items eligible for return, joined with order status
    items = order_items.merge(
        orders[["order_id", "order_status"]],
        on="order_id",
        how="inner",
    )

    full_refund = items[items["order_status"] == "refunded"]

    partial_pool = items[
        items["order_status"] == "partially_refunded"
    ]
    # roughly half the items of a partially refunded order
    partial = partial_pool.sample(frac=0.5)

    returned = pd.concat(
        [full_refund, partial], ignore_index=True
    )

    # Return date follows the delivery date of the shipment
    returned = returned.merge(
        shipments[["order_id", "delivered_at"]],
        on="order_id",
        how="left",
    )

    n = len(returned)

    return_date = (
        returned["delivered_at"] +
        pd.to_timedelta(
            np.random.randint(24, 30 * 24, n), unit="h"
        )
    )

    returns = pd.DataFrame({
        "return_id": (
            "R" +
            pd.Series(np.arange(1, n + 1))
            .astype("str")
            .str.zfill(7)
        ),
        "order_id": returned["order_id"].values,
        "order_item_id": returned["order_item_id"].values,
        "product_id": returned["product_id"].values,
        "quantity_returned": returned["quantity"].values,
        "return_date": return_date.values,
        "return_reason": np.random.choice(
            RETURN_REASONS, size=n, p=RETURN_REASON_WEIGHTS
        ),
        "refund_amount": returned["total_amount"].values,
        "return_status": np.random.choice(
            ["refunded", "processing", "rejected"],
            size=n,
            p=[0.85, 0.10, 0.05],
        ),
    })

    return returns
