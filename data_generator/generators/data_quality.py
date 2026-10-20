"""Data quality issue injection.

Applied to the clean generated data right before saving, so the
raw layer contains realistic problems to detect and fix in the
medallion pipeline (bronze -> silver). Every injection is done
with the global numpy seed, so the dirty data is reproducible.

Issue catalogue:
    customers   : full-row duplicates, missing email, invalid DOB
    products    : negative cost, missing category, duplicate product
    orders      : duplicate order, unknown customer, future
                  timestamp, invalid status value
    order_items : unknown product, quantity <= 0, wrong total_amount
    payments    : orphan payment (no such order), amount mismatch,
                  failed payment marked as captured
"""

import numpy as np
import pandas as pd

DEFAULT_RATE = 0.005  # 0.5% per issue type


def _pick_indices(df, rate, exclude=None):
    """Random row positions to corrupt, optionally excluding some."""
    n = max(1, int(len(df) * rate))
    candidates = df.index.difference(exclude) if exclude is not None else df.index
    return np.random.choice(candidates, size=min(n, len(candidates)), replace=False)


def _add_duplicates(df, rate):
    dup = df.sample(n=max(1, int(len(df) * rate)))
    return pd.concat([df, dup], ignore_index=True), len(dup)


def inject_customers_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Full-row duplicates
    df, report["duplicate_rows"] = _add_duplicates(df, rate)

    # 2. Missing email
    idx = _pick_indices(df, rate)
    df.loc[idx, "email"] = None
    report["missing_email"] = len(idx)

    # 3. Invalid date of birth (in the future or impossibly old)
    idx = _pick_indices(df, rate)
    half = len(idx) // 2
    df.loc[idx[:half], "date_of_birth"] = pd.Timestamp("2031-05-15").date()
    df.loc[idx[half:], "date_of_birth"] = pd.Timestamp("1888-01-01").date()
    report["invalid_dob"] = len(idx)

    return df, report


def inject_products_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Duplicate products
    df, report["duplicate_rows"] = _add_duplicates(df, rate)

    # 2. Negative cost
    idx = _pick_indices(df, rate)
    df.loc[idx, "cost"] = -df.loc[idx, "cost"].abs()
    report["negative_cost"] = len(idx)

    # 3. Missing category
    idx = _pick_indices(df, rate)
    df.loc[idx, "category"] = None
    report["missing_category"] = len(idx)

    return df, report


def inject_orders_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Duplicate orders
    df, report["duplicate_rows"] = _add_duplicates(df, rate)

    # 2. Unknown customer (FK violation)
    idx = _pick_indices(df, rate)
    df.loc[idx, "customer_id"] = "C999999"
    report["unknown_customer"] = len(idx)

    # 3. Future timestamps
    idx2 = _pick_indices(df, rate, exclude=pd.Index(idx))
    df.loc[idx2, "order_timestamp"] = (
        pd.Timestamp("2027-06-01") +
        pd.to_timedelta(np.random.randint(0, 10_000, len(idx2)), unit="m")
    )
    report["future_timestamp"] = len(idx2)

    # 4. Invalid status values (casing / unknown value)
    idx3 = _pick_indices(df, rate, exclude=pd.Index(idx).union(idx2))
    half = len(idx3) // 2
    df.loc[idx3[:half], "order_status"] = "COMPLETED"
    df.loc[idx3[half:], "order_status"] = "unknown"
    report["invalid_status"] = len(idx3)

    return df, report


def inject_order_items_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Unknown product (FK violation)
    idx = _pick_indices(df, rate)
    df.loc[idx, "product_id"] = "P99999"
    report["unknown_product"] = len(idx)

    # 2. Non-positive quantity
    idx2 = _pick_indices(df, rate, exclude=pd.Index(idx))
    half = len(idx2) // 2
    df.loc[idx2[:half], "quantity"] = 0
    df.loc[idx2[half:], "quantity"] = -1
    report["bad_quantity"] = len(idx2)

    # 3. Wrong total_amount (random corruption factor)
    idx3 = _pick_indices(df, rate, exclude=pd.Index(idx).union(idx2))
    df.loc[idx3, "total_amount"] = (
        df.loc[idx3, "total_amount"] *
        np.random.uniform(1.5, 3.0, len(idx3))
    ).round(2)
    report["wrong_total"] = len(idx3)

    return df, report


def inject_payments_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Orphan payments: order_id that doesn't exist
    n_orphans = max(1, int(len(df) * rate))
    orphans = df.sample(n=n_orphans).copy()
    orphans["payment_id"] = [
        f"PAY9{i:07d}" for i in range(1, n_orphans + 1)
    ]
    orphans["order_id"] = [
        f"O9{i:06d}" for i in range(1, n_orphans + 1)
    ]
    df = pd.concat([df, orphans], ignore_index=True)
    report["orphan_payments"] = n_orphans

    # 2. Amount mismatch vs order items
    idx = _pick_indices(df, rate)
    df.loc[idx, "amount"] = (
        df.loc[idx, "amount"] +
        np.random.uniform(5, 200, len(idx))
    ).round(2)
    report["amount_mismatch"] = len(idx)

    # 3. Failed payments marked as captured
    failed_idx = df.index[df["payment_status"] == "failed"]
    n_flip = min(max(1, int(len(df) * rate)), len(failed_idx))
    flip = np.random.choice(failed_idx, size=n_flip, replace=False)
    df.loc[flip, "payment_status"] = "captured"
    report["failed_marked_captured"] = n_flip

    return df, report


def inject_shipments_issues(df, orders, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. delivered_at earlier than shipped_at (swapped)
    idx = _pick_indices(df, rate)
    tmp = df.loc[idx, "shipped_at"].copy()
    df.loc[idx, "shipped_at"] = df.loc[idx, "delivered_at"].values
    df.loc[idx, "delivered_at"] = tmp.values
    report["delivered_before_shipped"] = len(idx)

    # 2. Unknown carrier
    idx2 = _pick_indices(df, rate, exclude=pd.Index(idx))
    df.loc[idx2, "carrier"] = "XX Express"
    report["unknown_carrier"] = len(idx2)

    # 3. Missing shipments: drop rows so some completed
    #    orders end up with no shipment at all
    completed_ids = set(
        orders.loc[
            orders["order_status"] == "completed", "order_id"
        ]
    )
    droppable = df.index[
        df["order_id"].isin(completed_ids)
    ].difference(idx).difference(idx2)
    n_drop = max(1, int(len(df) * rate))
    drop_idx = np.random.choice(
        droppable, size=min(n_drop, len(droppable)), replace=False
    )
    df = df.drop(index=drop_idx).reset_index(drop=True)
    report["missing_shipment"] = len(drop_idx)

    return df, report


def inject_returns_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Duplicate returns (same item returned twice)
    df, report["duplicate_rows"] = _add_duplicates(df, rate)

    # 2. Return date before delivery (impossible timeline):
    #    shift far into the past, before any delivery
    idx = _pick_indices(df, rate)
    df.loc[idx, "return_date"] = (
        pd.Timestamp("2022-06-01") +
        pd.to_timedelta(
            np.random.randint(0, 10_000, len(idx)), unit="m"
        )
    )
    report["return_before_delivery"] = len(idx)

    # 3. Refund higher than the item total
    idx2 = _pick_indices(df, rate, exclude=pd.Index(idx))
    df.loc[idx2, "refund_amount"] = (
        df.loc[idx2, "refund_amount"] *
        np.random.uniform(2.0, 5.0, len(idx2))
    ).round(2)
    report["refund_too_high"] = len(idx2)

    return df, report


def inject_web_events_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Duplicate events (double-fired tracking)
    df, report["duplicate_rows"] = _add_duplicates(df, rate)

    # 2. Missing customer (anonymous rows where id is required)
    idx = _pick_indices(df, rate)
    df.loc[idx, "customer_id"] = None
    report["missing_customer"] = len(idx)

    # 3. Product events pointing to unknown products
    prod_rows = df.index[df["event_type"] == "product_view"]
    n_bad = max(1, int(len(df) * rate))
    idx2 = np.random.choice(
        prod_rows.difference(idx),
        size=min(n_bad, len(prod_rows)),
        replace=False,
    )
    df.loc[idx2, "product_id"] = "P99999"
    report["unknown_product"] = len(idx2)

    return df, report


def inject_reviews_issues(df, rate=DEFAULT_RATE):
    df = df.copy()
    report = {}

    # 1. Duplicate reviews
    df, report["duplicate_rows"] = _add_duplicates(df, rate)

    # 2. Rating outside the 1-5 domain
    idx = _pick_indices(df, rate)
    half = len(idx) // 2
    df.loc[idx[:half], "rating"] = 0
    df.loc[idx[half:], "rating"] = 6
    report["rating_out_of_range"] = len(idx)

    # 3. Unknown product (review for nothing in the catalog)
    idx2 = _pick_indices(df, rate, exclude=pd.Index(idx))
    df.loc[idx2, "product_id"] = "P99999"
    report["unknown_product"] = len(idx2)

    return df, report


def print_report(name, report):
    total = sum(report.values())
    details = ", ".join(f"{k}={v}" for k, v in report.items())
    print(f"  [dirty] {name}: {total} issues injected ({details})")
