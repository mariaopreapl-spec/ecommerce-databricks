# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - source list
# MAGIC Defines the 9 source tables and publishes them as a **task value** (`output_key`).
# MAGIC The job's **For each** task loops over this list and runs `02_bronze_ingestion` once per table, passing the name into the `src` parameter.

# COMMAND ----------

src_array = [

            {"src": "customers"},
            {"src": "products"},
            {"src": "orders"},
            {"src": "order_items"},
            {"src": "payments"},
            {"src": "shipments"},
            {"src": "returns"},
            {"src": "web_events"},
            {"src": "reviews"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "output_key", value = src_array)
