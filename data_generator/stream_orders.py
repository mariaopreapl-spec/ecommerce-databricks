"""Real-time order stream simulator (file-based, zero cost).

Continuously generates orders as small JSON files dropped into a
landing folder -- the classic pattern for Databricks Auto Loader
(cloudFiles) streaming ingestion. Each event is a nested JSON
document: order header with an embedded list of items, so the
pipeline gets to demonstrate schema inference, flatten/explode
and streaming semantics without any cloud infrastructure.

Usage:
    python stream_orders.py                  # 1 file / 3 s, forever
    python stream_orders.py --interval 1     # faster
    python stream_orders.py --max-files 20   # stop after 20 files
Stop anytime with Ctrl+C.

On Databricks, point it at the volumes so Auto Loader sees the files:
    python stream_orders.py --max-files 20 \
        --data-dir    /Volumes/ecommerce/ecommerce_raw/ecommerce_raw_landing/rawdata \
        --landing-dir /Volumes/ecommerce/ecommerce_raw/ecommerce_raw_landing/stream_landing
Needs only numpy and pandas (no Faker), so it runs on any cluster.

IDs use the "OS" / "OIS" prefixes so streamed orders never collide
with the batch-generated ones ("O" / "OI").
"""

import argparse
import glob
import json
import os
import time
import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# Same defaults as generators/config.py (repo_root/data/raw), defined
# here so the script does not need Faker when run on Databricks.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATA_DIR = os.path.join(REPO_ROOT, "data", "raw")
DEFAULT_LANDING_DIR = os.path.join(DEFAULT_DATA_DIR, "stream_landing")

PAYMENT_METHODS = [
    "Credit Card", "Debit Card", "PayPal",
    "Apple Pay", "Google Pay", "Bank Transfer",
]
SALES_CHANNELS = ["Web", "Mobile App", "Marketplace"]
CHANNEL_WEIGHTS = [0.55, 0.35, 0.10]

# small, realistic imperfections in the live stream too
DIRTY_RATE = 0.02


def _find_csvs(data_dir, table):
    """CSV files of a table: flat layout (data/raw/customers.csv) or
    one folder per table (rawdata/customers/*.csv), increments included."""
    files = glob.glob(os.path.join(data_dir, f"{table}*.csv"))
    files += glob.glob(os.path.join(data_dir, table, "*.csv"))
    if not files:
        raise FileNotFoundError(f"No {table} CSV found under {data_dir}")
    return files


def load_master_data(data_dir):
    customers = pd.concat(
        pd.read_csv(f, usecols=["customer_id"])
        for f in _find_csvs(data_dir, "customers")
    )
    products = pd.concat(
        pd.read_csv(f, usecols=["product_id", "list_price"])
        for f in _find_csvs(data_dir, "products")
    )
    # dirty batch data contains duplicates and negative/missing values
    products = products.dropna().drop_duplicates("product_id")
    products = products[products["list_price"] > 0]
    return customers["customer_id"].dropna().unique(), products


def generate_order(customer_ids, products, seq):
    now = datetime.now(timezone.utc)

    n_items = np.random.choice(
        [1, 2, 3, 4, 5], p=[0.45, 0.30, 0.15, 0.07, 0.03]
    )
    picked = products.sample(n=n_items)

    items = []
    total = 0.0
    for j, (_, prod) in enumerate(picked.iterrows(), start=1):
        qty = int(np.random.choice(
            [1, 2, 3, 4], p=[0.70, 0.20, 0.08, 0.02]
        ))
        unit_price = float(prod["list_price"])
        discount_rate = float(np.random.choice(
            [0, 0.05, 0.10, 0.20], p=[0.60, 0.20, 0.15, 0.05]
        ))
        gross = qty * unit_price
        discount = gross * discount_rate
        tax = (gross - discount) * 0.19
        line_total = round(gross - discount + tax, 2)
        total += line_total

        items.append({
            "line_number": j,
            "product_id": prod["product_id"],
            "quantity": qty,
            "unit_price": round(unit_price, 2),
            "discount_amount": round(discount, 2),
            "tax_amount": round(tax, 2),
            "total_amount": line_total,
        })

    order = {
        "order_id": f"OS{now.strftime('%Y%m%d%H%M%S')}{seq:04d}",
        "customer_id": str(np.random.choice(customer_ids)),
        "order_timestamp": now.isoformat(),
        "order_status": "pending",
        "currency": "EUR",
        "payment_method": str(np.random.choice(PAYMENT_METHODS)),
        "sales_channel": str(np.random.choice(
            SALES_CHANNELS, p=CHANNEL_WEIGHTS
        )),
        "items": items,
        "order_total": round(total, 2),
        "event_source": "order_service_v2",
        "ingested_at": now.isoformat(),
    }

    # occasional realistic stream issues
    roll = np.random.random()
    if roll < DIRTY_RATE / 2:
        order["customer_id"] = None            # missing customer
    elif roll < DIRTY_RATE:
        order["order_total"] = round(
            order["order_total"] * 1.5, 2
        )                                       # total mismatch

    return order


def write_atomic(path, lines):
    """Write-then-rename so Auto Loader never reads a half-written
    file. Falls back to a direct write on file systems without
    rename support."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            f.writelines(lines)
        os.rename(tmp, path)
    except OSError:
        if os.path.exists(tmp):
            os.remove(tmp)
        with open(path, "w") as f:
            f.writelines(lines)


def stream(interval, orders_per_file, max_files, data_dir, landing_dir):
    os.makedirs(landing_dir, exist_ok=True)
    customer_ids, products = load_master_data(data_dir)
    print(f"Landing folder: {landing_dir}")
    print(f"{len(customer_ids)} customers, "
          f"{len(products)} products loaded")
    print("Streaming... Ctrl+C to stop\n")

    file_count = 0
    seq = 0
    try:
        while True:
            batch = []
            for _ in range(orders_per_file):
                seq += 1
                batch.append(
                    generate_order(customer_ids, products, seq)
                )

            fname = (
                f"orders_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
                f"_{uuid.uuid4().hex[:6]}.json"
            )
            fpath = os.path.join(landing_dir, fname)
            write_atomic(fpath, [json.dumps(o) + "\n" for o in batch])

            file_count += 1
            totals = sum(o["order_total"] for o in batch)
            print(
                f"[{file_count}] {fname} | "
                f"{orders_per_file} orders | "
                f"EUR {totals:,.2f}"
            )

            if max_files and file_count >= max_files:
                print("\nDone (max files reached).")
                break

            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stream orders as JSON files for Auto Loader."
    )
    parser.add_argument(
        "--interval", type=float, default=3.0,
        help="seconds between files (default 3)",
    )
    parser.add_argument(
        "--orders-per-file", type=int, default=5,
        help="orders per JSON file (default 5)",
    )
    parser.add_argument(
        "--max-files", type=int, default=None,
        help="stop after N files (default: run forever)",
    )
    parser.add_argument(
        "--data-dir", default=DEFAULT_DATA_DIR,
        help="where customers/products CSVs live (default data/raw)",
    )
    parser.add_argument(
        "--landing-dir", default=DEFAULT_LANDING_DIR,
        help="where JSON files are written (default data/raw/stream_landing)",
    )
    args = parser.parse_args()
    stream(args.interval, args.orders_per_file, args.max_files,
           args.data_dir, args.landing_dir)
