"""Product catalog generator."""

import numpy as np
import pandas as pd

from generators.config import START_DATE, END_DATE

CATEGORY_CONFIG = {
    "Electronics": {
        "subcategories": [
            "Audio",
            "Computers",
            "Accessories",
            "Smartphones"
        ],
        "price_range": (50, 1500)
    },

    "Fashion": {
        "subcategories": [
            "Shoes",
            "Clothing",
            "Accessories"
        ],
        "price_range": (15, 400)
    },

    "Beauty": {
        "subcategories": [
            "Skincare",
            "Makeup",
            "Haircare"
        ],
        "price_range": (10, 200)
    },

    "Home": {
        "subcategories": [
            "Furniture",
            "Kitchen",
            "Decor"
        ],
        "price_range": (15, 1200)
    },

    "Sports": {
        "subcategories": [
            "Fitness",
            "Outdoor",
            "Team Sports"
        ],
        "price_range": (10, 600)
    },

    "Books": {
        "subcategories": [
            "Fiction",
            "Business",
            "Technology"
        ],
        "price_range": (5, 80)
    },

    "Toys": {
        "subcategories": [
            "Games",
            "Educational",
            "Outdoor"
        ],
        "price_range": (5, 250)
    },

    "Garden": {
        "subcategories": [
            "Tools",
            "Plants",
            "Outdoor Furniture"
        ],
        "price_range": (10, 800)
    },

    "Automotive": {
        "subcategories": [
            "Accessories",
            "Tools",
            "Electronics"
        ],
        "price_range": (20, 2000)
    },

    "Pet Supplies": {
        "subcategories": [
            "Food",
            "Toys",
            "Accessories"
        ],
        "price_range": (5, 300)
    }
}


def generate_products(n):
    rows = []

    categories = list(CATEGORY_CONFIG.keys())

    for i in range(n):
        category = np.random.choice(categories)

        config = CATEGORY_CONFIG[category]

        subcategory = np.random.choice(
            config["subcategories"]
        )

        min_price, max_price = config["price_range"]

        list_price = round(
            np.random.uniform(min_price, max_price),
            2
        )

        cost = round(
            list_price * np.random.uniform(0.45, 0.75),
            2
        )

        launch_date = START_DATE + pd.Timedelta(
            days=np.random.randint(
                0,
                (END_DATE - START_DATE).days
            )
        )

        rows.append({
            "product_id": f"P{i+1:05d}",
            "product_name": (
                f"{subcategory} Product {i+1:05d}"
            ),
            "description": (
                f"High-quality {subcategory.lower()} "
                f"product from the {category.lower()} category."
            ),
            "category": category,
            "subcategory": subcategory,
            "brand": f"Brand_{np.random.randint(1, 301):03d}",
            "supplier_id": f"SUP{np.random.randint(1, 101):04d}",
            "cost": cost,
            "list_price": list_price,
            "launch_date": launch_date.date(),
            "weight_kg": round(
                np.random.lognormal(0, 0.6),
                3
            ),
            "rating": round(
                np.clip(
                    np.random.normal(4.1, 0.5),
                    1,
                    5
                ),
                2
            )
        })

    return pd.DataFrame(rows)
