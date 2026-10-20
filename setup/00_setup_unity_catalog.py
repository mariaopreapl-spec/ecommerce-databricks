# Databricks notebook source
# MAGIC %md
# MAGIC # Setup - Unity Catalog objects
# MAGIC Creates everything the project needs, in one run. Safe to run again.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |---|---|
# MAGIC | catalog `ecommerce` | the whole project |
# MAGIC | schemas `ecommerce_raw`, `bronze`, `silver`, `gold` | one per layer |
# MAGIC | volume `ecommerce_raw.ecommerce_raw_landing` | landing zone for source files |
# MAGIC | volume `bronze.bronzevolume` | bronze Delta files and Auto Loader checkpoints |
# MAGIC | folders `rawdata/<table>/`, `stream_landing/` | one folder per source, plus the stream drop zone |

# COMMAND ----------

CATALOG = "ecommerce"
SCHEMAS = ["ecommerce_raw", "bronze", "silver", "gold"]
TABLES = [
    "customers", "products", "orders", "order_items", "payments",
    "shipments", "returns", "web_events", "reviews",
]

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    raise RuntimeError(
        "Could not create the catalog with SQL. Create it in the UI instead: "
        "Catalog -> + -> Create catalog -> name 'ecommerce', then run this "
        f"notebook again.\n{e}"
    )

for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ecommerce_raw.ecommerce_raw_landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.bronzevolume")

# COMMAND ----------

LANDING = f"/Volumes/{CATALOG}/ecommerce_raw/ecommerce_raw_landing"

for table in TABLES:
    dbutils.fs.mkdirs(f"{LANDING}/rawdata/{table}")
dbutils.fs.mkdirs(f"{LANDING}/stream_landing")

display(dbutils.fs.ls(f"{LANDING}/rawdata"))
