"""Shared configuration: seed, date range, volumes.

Single source of truth for all generators.
"""

import os

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42

np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

START_DATE = pd.Timestamp("2023-01-01")
END_DATE = pd.Timestamp("2025-12-31")

N_CUSTOMERS = 60_000
N_PRODUCTS = 3_000
N_ORDERS = 700_000
N_WEB_EVENTS = 1_000_000
N_REVIEWS = 100_000

# Output folders (repo_root/data/raw), independent of the working directory
REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
DATA_DIR = os.path.join(REPO_ROOT, "data", "raw")
STREAM_LANDING_DIR = os.path.join(DATA_DIR, "stream_landing")
