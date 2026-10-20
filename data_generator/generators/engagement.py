"""Engagement generators: web events and reviews (vectorized).

Web events: clickstream-style data (page views, product views,
cart actions, searches, checkouts) tied to customers, sessions
and -- for product events -- products.

Reviews: tied to actual purchases (customer_id, product_id,
order_id from completed orders). Only a subset of the product
catalog is reviewable, so many products deliberately have no
reviews at all -- realistic, and a nice gotcha for joins in
the medallion pipeline.
"""

import numpy as np
import pandas as pd

from generators.config import START_DATE, END_DATE

EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "begin_checkout",
]
EVENT_WEIGHTS = [0.38, 0.30, 0.12, 0.12, 0.03, 0.05]

# event types that carry a product_id
PRODUCT_EVENTS = {
    "product_view", "add_to_cart", "remove_from_cart"
}

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.58, 0.36, 0.06]

TRAFFIC_SOURCES = [
    "organic", "paid_search", "social",
    "email", "direct", "referral",
]
SOURCE_WEIGHTS = [0.30, 0.20, 0.15, 0.12, 0.18, 0.05]

REVIEW_TITLES = [
    "Great product", "Does the job", "Not what I expected",
    "Excellent quality", "Decent for the price", "Disappointed",
    "Would buy again", "Five stars", "Just okay", "Love it",
]

# share of the catalog that can receive reviews at all
REVIEWABLE_PRODUCT_SHARE = 0.65


def generate_web_events(customers, products, n_events):
    customer_ids = np.random.choice(
        customers["customer_id"].values, size=n_events
    )

    event_types = np.random.choice(
        EVENT_TYPES, size=n_events, p=EVENT_WEIGHTS
    )

    # product_id only for product-related events
    product_ids = np.random.choice(
        products["product_id"].values, size=n_events
    ).astype(object)
    has_product = np.isin(event_types, list(PRODUCT_EVENTS))
    product_ids[~has_product] = None

    timestamps = (
        START_DATE +
        pd.to_timedelta(
            np.random.randint(
                0,
                int((END_DATE - START_DATE).total_seconds()),
                n_events,
            ),
            unit="s",
        )
    )

    # sessions: ~4 events per session on average
    n_sessions = max(1, n_events // 4)
    session_ids = (
        "S" +
        pd.Series(
            np.random.randint(1, n_sessions + 1, n_events)
        ).astype("str").str.zfill(8)
    )

    events = pd.DataFrame({
        "event_id": (
            "E" +
            pd.Series(np.arange(1, n_events + 1))
            .astype("str")
            .str.zfill(9)
        ),
        "session_id": session_ids,
        "customer_id": customer_ids,
        "event_type": event_types,
        "product_id": product_ids,
        "event_timestamp": timestamps,
        "device": np.random.choice(
            DEVICES, size=n_events, p=DEVICE_WEIGHTS
        ),
        "traffic_source": np.random.choice(
            TRAFFIC_SOURCES, size=n_events, p=SOURCE_WEIGHTS
        ),
    })

    return events


def generate_reviews(orders, order_items, n_reviews):
    # Reviews come from real purchases in completed orders
    completed = orders[
        orders["order_status"] == "completed"
    ][["order_id", "customer_id", "order_timestamp"]]

    purchases = order_items[
        ["order_id", "product_id"]
    ].merge(completed, on="order_id", how="inner")

    # Only a subset of the catalog is reviewable ->
    # a meaningful share of products has no reviews
    all_products = purchases["product_id"].unique()
    n_reviewable = int(len(all_products) * REVIEWABLE_PRODUCT_SHARE)
    reviewable = np.random.choice(
        all_products, size=n_reviewable, replace=False
    )
    purchases = purchases[
        purchases["product_id"].isin(reviewable)
    ]

    # One review max per (customer, product)
    sample = purchases.sample(
        n=min(n_reviews * 2, len(purchases))
    ).drop_duplicates(
        subset=["customer_id", "product_id"]
    ).head(n_reviews).reset_index(drop=True)

    n = len(sample)

    # Ratings skewed positive, like most review platforms
    ratings = np.random.choice(
        [1, 2, 3, 4, 5],
        size=n,
        p=[0.06, 0.07, 0.14, 0.30, 0.43],
    )

    review_date = (
        sample["order_timestamp"] +
        pd.to_timedelta(
            np.random.randint(3 * 24, 45 * 24, n), unit="h"
        )
    )

    reviews = pd.DataFrame({
        "review_id": (
            "REV" +
            pd.Series(np.arange(1, n + 1))
            .astype("str")
            .str.zfill(7)
        ),
        "product_id": sample["product_id"].values,
        "customer_id": sample["customer_id"].values,
        "order_id": sample["order_id"].values,
        "rating": ratings,
        "review_title": np.random.choice(
            REVIEW_TITLES, size=n
        ),
        "review_date": review_date.values,
        "verified_purchase": True,
        "helpful_votes": np.random.poisson(2, n),
    })

    return reviews
