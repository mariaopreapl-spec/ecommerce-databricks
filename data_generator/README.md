# Synthetic data generator

Python generators that produce a realistic, **reproducible** European e-commerce dataset (fixed seed 42). Real-world problems are injected on purpose, so the medallion pipeline has real cleaning work to do in the bronze → silver step.

Three ingestion patterns are covered:

| Script | Pattern | Output |
|---|---|---|
| `generate_all.py` | Historical **full load** (batch) | 9 CSV tables in `data/raw/` |
| `generate_increment.py` | **Incremental** batch of new customers | `customers_increment_<timestamp>.csv` |
| `stream_orders.py` | **Real-time stream** for Auto Loader | Nested JSON files in `data/raw/stream_landing/` |

## Quick start

```bash
pip install -r requirements.txt

python data_generator/generate_all.py                 # full load, ~3.5M rows, a few minutes
python data_generator/generate_increment.py --n 20    # 20 new customers
python data_generator/stream_orders.py --max-files 20 # stream 20 JSON files, then stop
```

The scripts can be run from any directory. All output goes to `data/raw/` at the repo root.

## Data model

Period: **2023-01-01 → 2025-12-31**. Currency EUR, 19% VAT, 10 European countries.

| Table | Rows | Grain | Key relationships |
|---|---:|---|---|
| `customers` | 60,000 | one per customer | segment (Genius / Regular), country, acquisition channel |
| `products` | 3,000 | one per product | 10 categories, 31 subcategories, cost and list price |
| `orders` | 700,000 | one per order | → customers |
| `order_items` | ~1.35M | one per order line | → orders, → products |
| `payments` | 700,000 | one per order | → orders, status derived from order status |
| `shipments` | ~627,000 | one per shipped order | → orders |
| `returns` | ~88,000 | one per returned line | → orders, order_items, products |
| `web_events` | 1,000,000 | one per clickstream event | → customers, products, sessions |
| `reviews` | 100,000 | one per review | → products, customers, orders |

Row counts are before data-quality injection (duplicates add ~0.5%).

## Built-in realism

**Seasonality.** Daily order volume is `monthly factor × yearly growth × day-of-week factor`, plus event windows: Black Friday through Cyber Monday (×2.4), the pre-Christmas run-up, and the dead week between Christmas and New Year. November is the peak and February the trough. The business grows 18% in 2024 and 38% in 2025 versus 2023. Orders follow a realistic intraday curve that peaks in the evening.

**Discounts on their own calendar.** January and February are heavy-discount, low-volume clearance months. November is heavy on both. Because volume and discounts are decoupled, revenue and margin move independently, as they do in a real shop.

**Behavior.** Genius customers order more often than Regular ones. Basket size, quantities, return reasons and delivery times follow skewed distributions rather than uniform ones.

## Injected data-quality issues

`generators/data_quality.py` corrupts about **0.5% of rows per issue type** after generation. The corruption is reproducible thanks to the seed.

| Table | Issues |
|---|---|
| customers | duplicate rows, missing email, invalid date of birth (future / 1888) |
| products | duplicate rows, negative cost, missing category |
| orders | duplicate rows, unknown customer, future timestamp, invalid status |
| order_items | unknown product, quantity ≤ 0, wrong `total_amount` |
| payments | orphan payment, amount mismatch, failed payment marked as captured |
| shipments | delivered before shipped, unknown carrier, missing shipment |
| returns | duplicate rows, return before delivery, refund higher than paid |
| web_events | duplicate rows, missing customer, unknown product |
| reviews | duplicate rows, rating out of range, unknown product |

The stream simulator adds its own 2% of issues (missing customer, order total mismatch).

## Streaming simulator

`stream_orders.py` drops small **newline-delimited JSON** files into a landing folder. This is the input pattern for Databricks **Auto Loader** (`cloudFiles`). Each event is nested (an order header with an embedded `items` array), so the pipeline has to infer the schema and explode the items. Files are written to a temp name first and then renamed, so Auto Loader never reads a half-written file. Streamed IDs use the `OS` prefix, so they never collide with batch orders (`O`).

```bash
python data_generator/stream_orders.py                          # 1 file every 3 s, until Ctrl+C
python data_generator/stream_orders.py --interval 1 --orders-per-file 20
```

On Databricks it writes straight into the volumes, so Auto Loader sees the files immediately. It needs only NumPy and pandas, without Faker:

```bash
python stream_orders.py --max-files 20 \
    --data-dir    /Volumes/ecommerce/ecommerce_raw/ecommerce_raw_landing/rawdata \
    --landing-dir /Volumes/ecommerce/ecommerce_raw/ecommerce_raw_landing/stream_landing
```

The customer and product IDs come from the batch data, so streamed orders join to the same dimensions. Both layouts work: flat (`data/raw/customers.csv`) and one folder per table (`rawdata/customers/*.csv`).

## Code layout

```
data_generator/
├── generate_all.py          # entry point: full load in dependency order
├── generate_increment.py    # incremental customer batches
├── stream_orders.py         # Auto Loader stream simulator
└── generators/
    ├── config.py            # seed, date range, volumes, output paths
    ├── customers.py
    ├── products.py
    ├── orders.py            # orders + order_items, seasonality model
    ├── payments.py
    ├── fulfillment.py       # shipments + returns
    ├── engagement.py        # web events + reviews
    └── data_quality.py      # issue injection + report
```

Volumes and the date range are set in `generators/config.py`. Generation is vectorized with NumPy and pandas.
