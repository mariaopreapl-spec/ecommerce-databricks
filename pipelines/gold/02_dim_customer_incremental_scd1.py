# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - incremental dimension load (SCD Type 1)
# MAGIC Generic, parameter-driven pattern for loading a dimension incrementally:
# MAGIC 1. Find the last load watermark (`MAX(cdc_col)` in the target) or use a back-dated refresh date.
# MAGIC 2. Read only the newer source rows.
# MAGIC 3. Split them into **existing** records (keep their surrogate key, update `updated_date`) and **new** records (assign the next surrogate keys).
# MAGIC 4. **MERGE** into the target Delta table.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetching Parameters & Creating Variables**

# COMMAND ----------

# Catalog Name
catalog_name = "ecommerce"

# Key Cols List
key_cols = "['customer_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "_ingested_at"

# Back-Dated Refresh
backdated_refresh = ""

# Source Object
source_object = "silver_customers"

# Source Schema
source_schema = "silver"

# Target Object
target_object = "dim_customers_cdc"

# Target Schema
target_schema = "gold"

# Surrogate Key Name
surrogate_key = "customer_dsk"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Last Load Date

# COMMAND ----------

# No backdated refresh
if len(backdated_refresh) == 0:

    # If table exists in the destination
    if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):
        last_load = spark.sql(f"SELECT MAX({cdc_col}) FROM {catalog_name}.{target_schema}.{target_object}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"
# Yes backdated refresh
else:
    last_load = backdated_refresh

# Test the last_load
last_load

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM ecommerce.{source_schema}.{source_object} WHERE {cdc_col} >= '{last_load}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Old vs New Records

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):

    # Key Columns String for Incremental Load
    key_cols_string_inc = ''.join(key_cols_list)

    df_tgt = spark.sql(f"SELECT {key_cols_string_inc}, {surrogate_key}, created_date, updated_date FROM {catalog_name}.{target_schema}.{target_object}")

else:
    
    # Key Columns String for Initial Load
    key_cols_string_init = [f"'' as {i}" for i in key_cols_list]
    key_cols_string_init = ''.join(key_cols_string_init)
    
    df_tgt = spark.sql(f"SELECT {key_cols_string_init}, CAST(0 AS bigint) as {surrogate_key}, CAST('1900-01-01 00:00:00' as timestamp) as created_date, CAST('1900-01-01 00:00:00' as timestamp) as updated_date WHERE 1=0")

# COMMAND ----------

# MAGIC %md
# MAGIC **JOIN CONDITION**

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = tgt.{i}" for i in key_cols_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_tgt.createOrReplaceTempView("tgt")

# Fix join_condition to use correct alias
join_condition_fixed = ' AND '.join([f"src.{i} = tgt.{i}" for i in key_cols_list])

df_join = spark.sql(f"""
            SELECT src.*,
                tgt.{surrogate_key},
                tgt.created_date,
                tgt.updated_date
            FROM src
            LEFT JOIN tgt
            ON {join_condition_fixed}
            """)

# COMMAND ----------

# Old Records
df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

# New Records
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

df_old_enriched = df_old.withColumn('updated_date', current_timestamp())

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):
    max_surrogate_key = spark.sql(f"SELECT MAX({surrogate_key}) FROM {catalog_name}.{target_schema}.{target_object}")\
                            .collect()[0][0]
    df_new_enriched = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                .withColumn('created_date', current_timestamp())\
                .withColumn('updated_date', current_timestamp())
else:
    max_surrogate_key = 0
    df_new_enriched = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                .withColumn('created_date', current_timestamp())\
                .withColumn('updated_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unioning Old and New Records

# COMMAND ----------

df_union = df_old_enriched.unionByName(df_new_enriched)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):

    dlt_object = DeltaTable.forName(spark, f"{catalog_name}.{target_schema}.{target_object}")

    dlt_object.alias("tgt").merge(df_union.alias("src"), f"tgt.{surrogate_key} = src.{surrogate_key}")\
        .whenMatchedUpdateAll(condition = f'src.{cdc_col} >= tgt.{cdc_col}')\
        .whenNotMatchedInsertAll()\
        .execute()

else:

    df_union.write.format("delta")\
        .mode('append')\
        .saveAsTable(f"{catalog_name}.{target_schema}.{target_object}")
