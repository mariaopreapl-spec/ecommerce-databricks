"""Customer master data generator."""

import numpy as np
import pandas as pd

from generators.config import fake, START_DATE, END_DATE

COUNTRIES = {
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse"],
    "Italy": ["Rome", "Milan", "Naples", "Turin"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville"],
    "Romania": ["Bucharest", "Cluj-Napoca", "Timisoara", "Iasi"],
    "Netherlands": ["Amsterdam", "Rotterdam", "Utrecht"],
    "Poland": ["Warsaw", "Krakow", "Wroclaw"],
    "Austria": ["Vienna", "Graz", "Salzburg"],
    "Belgium": ["Brussels", "Antwerp", "Ghent"],
    "Czechia": ["Prague", "Brno", "Ostrava"],
}

country_names = list(COUNTRIES.keys())

country_weights = [
    0.25,
    0.18,
    0.12,
    0.12,
    0.10,
    0.07,
    0.06,
    0.04,
    0.03,
    0.03,
]

segments = [
    "Genius",
    "Regular",
]

segment_weights = [
    0.60,
    0.40,
]

acquisition_channels = [
    "Marketplace",
    "Mobile App",
    "Website",
]

# Used later by the orders generator
SEGMENT_ORDER_RATES = {
    "Genius": 18,
    "Regular": 15,
}


def generate_customers(n):
    rows = []

    for i in range(n):
        country = np.random.choice(
            country_names,
            p=country_weights
        )

        city = np.random.choice(COUNTRIES[country])

        segment = np.random.choice(
            segments,
            p=segment_weights
        )

        signup_date = START_DATE + pd.Timedelta(
            days=np.random.randint(
                0,
                (END_DATE - START_DATE).days
            )
        )

        rows.append({
            "customer_id": f"C{i+1:06d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"customer{i+1}@example.com",
            "date_of_birth": fake.date_of_birth(
                minimum_age=18,
                maximum_age=75
            ),
            "gender": np.random.choice(
                ["F", "M", "Other"],
                p=[0.49, 0.49, 0.02]
            ),
            "country": country,
            "city": city,
            "signup_date": signup_date.date(),
            "customer_segment": segment,
            "acquisition_channel": np.random.choice(
                acquisition_channels
            )
        })

    return pd.DataFrame(rows)
