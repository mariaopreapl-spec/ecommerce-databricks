"""Synthetic e-commerce dataset generator.

Entry point: runs all generators in dependency order and
saves the outputs. Fixed seed -> fully reproducible.
"""

import os

from generators.config import (
    N_CUSTOMERS, N_PRODUCTS, N_ORDERS, N_WEB_EVENTS, N_REVIEWS,
    DATA_DIR,
)
from generators.customers import generate_customers
from generators.products import generate_products
from generators.orders import generate_orders, generate_order_items
from generators.payments import generate_payments
from generators.fulfillment import generate_shipments, generate_returns
from generators.engagement import generate_web_events, generate_reviews
from generators.data_quality import (
    inject_customers_issues,
    inject_products_issues,
    inject_orders_issues,
    inject_order_items_issues,
    inject_payments_issues,
    inject_shipments_issues,
    inject_returns_issues,
    inject_web_events_issues,
    inject_reviews_issues,
    print_report,
)


def generate_all_data():
    data_dir = DATA_DIR
    os.makedirs(data_dir, exist_ok=True)

    df_customers = generate_customers(N_CUSTOMERS)
    df_products = generate_products(N_PRODUCTS)
    df_orders = generate_orders(df_customers, N_ORDERS)
    df_order_items = generate_order_items(df_orders, df_products)
    df_payments = generate_payments(df_orders, df_order_items)
    df_shipments = generate_shipments(df_orders)
    df_returns = generate_returns(df_orders, df_order_items, df_shipments)
    df_web_events = generate_web_events(df_customers, df_products, N_WEB_EVENTS)
    df_reviews = generate_reviews(df_orders, df_order_items, N_REVIEWS)

    # Inject data quality issues into the raw layer
    print("Injecting data quality issues...")
    df_customers, rep = inject_customers_issues(df_customers)
    print_report("customers", rep)
    df_products, rep = inject_products_issues(df_products)
    print_report("products", rep)
    df_orders, rep = inject_orders_issues(df_orders)
    print_report("orders", rep)
    df_order_items, rep = inject_order_items_issues(df_order_items)
    print_report("order_items", rep)
    df_payments, rep = inject_payments_issues(df_payments)
    print_report("payments", rep)
    df_shipments, rep = inject_shipments_issues(df_shipments, df_orders)
    print_report("shipments", rep)
    df_returns, rep = inject_returns_issues(df_returns)
    print_report("returns", rep)
    df_web_events, rep = inject_web_events_issues(df_web_events)
    print_report("web_events", rep)
    df_reviews, rep = inject_reviews_issues(df_reviews)
    print_report("reviews", rep)

    df_customers.to_csv(
        os.path.join(data_dir, "customers.csv"),
        index=False
    )
    df_products.to_csv(
        os.path.join(data_dir, "products.csv"),
        index=False
    )
    df_orders.to_csv(
        os.path.join(data_dir, "orders.csv"),
        index=False
    )
    df_order_items.to_csv(
        os.path.join(data_dir, "order_items.csv"),
        index=False
    )
    df_payments.to_csv(
        os.path.join(data_dir, "payments.csv"),
        index=False
    )
    df_shipments.to_csv(
        os.path.join(data_dir, "shipments.csv"),
        index=False
    )
    df_returns.to_csv(
        os.path.join(data_dir, "returns.csv"),
        index=False
    )
    df_web_events.to_csv(
        os.path.join(data_dir, "web_events.csv"),
        index=False
    )
    df_reviews.to_csv(
        os.path.join(data_dir, "reviews.csv"),
        index=False
    )

    print(f"Generated {len(df_customers)} customers")
    print(f"Generated {len(df_products)} products")
    print(f"Generated {len(df_orders)} orders")
    print(f"Generated {len(df_order_items)} order items")
    print(f"Generated {len(df_payments)} payments")
    print(f"Generated {len(df_shipments)} shipments")
    print(f"Generated {len(df_returns)} returns")
    print(f"Generated {len(df_web_events)} web events")
    print(f"Generated {len(df_reviews)} reviews")


if __name__ == "__main__":
    generate_all_data()
