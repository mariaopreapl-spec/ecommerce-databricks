# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate the live order stream
# MAGIC Runs `data_generator/stream_orders.py` on Databricks and writes the JSON order events straight into the landing volume, where Auto Loader picks them up.
# MAGIC
# MAGIC The customers and products CSVs must already be uploaded to `rawdata/`, because the streamed orders reuse their IDs.
# MAGIC Run it again whenever you want new orders to arrive.

# COMMAND ----------

dbutils.widgets.text("files", "20", "Number of JSON files")
dbutils.widgets.text("orders_per_file", "5", "Orders per file")

# COMMAND ----------

import os
import subprocess
import sys

LANDING = "/Volumes/ecommerce/ecommerce_raw/ecommerce_raw_landing"

# the notebook runs from its own folder inside the repo
script = os.path.abspath(os.path.join("..", "data_generator", "stream_orders.py"))

result = subprocess.run(
    [
        sys.executable, script,
        "--max-files", dbutils.widgets.get("files"),
        "--orders-per-file", dbutils.widgets.get("orders_per_file"),
        "--interval", "0.5",
        "--data-dir", f"{LANDING}/rawdata",
        "--landing-dir", f"{LANDING}/stream_landing",
    ],
    capture_output=True, text=True,
)
print(result.stdout[-3000:])
if result.returncode != 0:
    raise RuntimeError(result.stderr)

# COMMAND ----------

display(dbutils.fs.ls(f"{LANDING}/stream_landing"))
