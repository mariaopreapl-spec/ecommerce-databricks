"""Incremental customer batch generator.

Simulates new customers signing up after the initial full load, so the
pipeline can demonstrate incremental ingestion (Auto Loader picks up only
the new file) and dimension updates in the silver/gold layers.

The next customer_id is derived from the data already on disk (the full
load plus every earlier increment), so IDs never collide or leave gaps.

Usage:
    python generate_increment.py            # 9 new customers
    python generate_increment.py --n 50     # 50 new customers
"""

import argparse
import glob
import os
from datetime import datetime

import numpy as np
import pandas as pd
from faker import Faker

from generators.config import DATA_DIR
from generators.customers import generate_customers


def next_customer_number():
    """Highest existing numeric customer_id + 1."""
    files = [os.path.join(DATA_DIR, "customers.csv")]
    files += glob.glob(os.path.join(DATA_DIR, "customers_increment_*.csv"))
    files = [f for f in files if os.path.exists(f)]
    if not files:
        raise FileNotFoundError(
            "No customers found in data/raw - run generate_all.py first."
        )
    ids = pd.concat(
        pd.read_csv(f, usecols=["customer_id"]) for f in files
    )["customer_id"]
    # dirty data contains fake IDs like C999999 (unknown customer
    # in orders), but those never appear in the customers file itself
    numbers = ids.dropna().str.extract(r"^C(\d+)$")[0].dropna().astype(int)
    return int(numbers.max()) + 1


def main(n_new):
    now = datetime.now()
    batch_tag = now.strftime("%Y%m%d_%H%M%S")

    # different seed per batch, otherwise every increment would
    # contain the same "new" people
    seed = int(now.strftime("%Y%m%d%H%M%S")) % (2**32)
    np.random.seed(seed)
    Faker.seed(seed)

    start = next_customer_number()
    df_new = generate_customers(n_new)
    numbers = range(start, start + n_new)
    df_new["customer_id"] = [f"C{n:06d}" for n in numbers]
    df_new["email"] = [f"customer{n}@example.com" for n in numbers]

    out = os.path.join(DATA_DIR, f"customers_increment_{batch_tag}.csv")
    df_new.to_csv(out, index=False)
    print(f"Wrote {len(df_new)} new customers -> {out}")
    print(df_new[["customer_id", "first_name", "country"]])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--n", type=int, default=9,
                        help="number of new customers (default 9)")
    main(parser.parse_args().n)
