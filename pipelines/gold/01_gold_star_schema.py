# Databricks notebook source
# Gold Layer - batch notebook (no DLT)
#
# Reads the silver tables produced by the DLT pipeline and writes the
# dimensional model + reporting marts into ecommerce.gold.
#
#   1. Star schema: dim_customer, dim_product, dim_date,
#      fact_sales (grain = order line), fact_payments, fact_returns
#   2. Reporting marts built on top of the star schema
#
# Run this after the silver pipeline (chain them in a Job).
# Every table is written with mode("overwrite") -> the notebook is
# idempotent: running it twice gives exactly the same result.

from pyspark.sql.functions import *
from datetime import date

SILVER = "ecommerce.silver"
GOLD = "ecommerce.gold"

DATE_FROM = date(2023, 1, 1)
DATE_TO = date(2026, 12, 31)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# --------------------------------------------
# helpers
# --------------------------------------------

def silver(table_name):
    """Read a table from the silver schema."""
    return spark.read.table(f"{SILVER}.{table_name}")

def gold(table_name):
    """Read a table already written to gold (used by the marts)."""
    return spark.read.table(f"{GOLD}.{table_name}")

def save(df, table_name):
    """Write a gold table and report the row count."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.{table_name}")
    )
    print(f"{GOLD}.{table_name}: {spark.table(f'{GOLD}.{table_name}').count()} rows")

def sk(column):
    """Deterministic surrogate key from a natural key."""
    return substring(sha2(col(column).cast("string"), 256), 1, 16)

def safe_div(numerator, denominator):
    """Division that returns NULL instead of failing on zero."""
    return when(denominator == 0, None).otherwise(numerator / denominator)

# COMMAND ----------

# ============================================
# DIMENSIONS
# ============================================

age = floor(datediff(current_date(), col("date_of_birth")) / 365.25)

dim_customer = (
    silver("silver_customers")
    .select(
        sk("customer_id").alias("customer_sk"),
        "customer_id",
        "first_name",
        "last_name",
        concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
        "email",
        "gender",
        "date_of_birth",
        age.alias("age"),
        "country",
        "city",
        "signup_date",
        "customer_segment",
        "acquisition_channel",
    )
    .withColumn(
        "age_band",
        when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 45, "35-44")
        .when(col("age") < 60, "45-59")
        .otherwise("60+"),
    )
)

save(dim_customer, "dim_customer")

# COMMAND ----------

dim_product = (
    silver("silver_products")
    .select(
        sk("product_id").alias("product_sk"),
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "supplier_id",
        "cost",
        "list_price",
        "launch_date",
        "weight_kg",
        col("rating").alias("catalog_rating"),
    )
    .withColumn("unit_margin", round(col("list_price") - col("cost"), 2))
    .withColumn(
        "margin_pct",
        round(safe_div(col("list_price") - col("cost"), col("list_price")), 4),
    )
    .withColumn(
        "price_tier",
        when(col("list_price") < 50, "Budget")
        .when(col("list_price") < 200, "Mid")
        .when(col("list_price") < 800, "Premium")
        .otherwise("Luxury"),
    )
)

save(dim_product, "dim_product")

# COMMAND ----------

n_days = (DATE_TO - DATE_FROM).days + 1

dim_date = (
    spark.range(0, n_days)
    .select(
        date_add(
            lit(DATE_FROM.isoformat()).cast("date"), col("id").cast("int")
        ).alias("date")
    )
    .select(
        date_format(col("date"), "yyyyMMdd").cast("int").alias("date_sk"),
        "date",
        year("date").alias("year"),
        quarter("date").alias("quarter"),
        concat(lit("Q"), quarter("date")).alias("quarter_name"),
        month("date").alias("month"),
        date_format(col("date"), "MMM").alias("month_name"),
        date_trunc("month", col("date")).alias("month_start"),
        weekofyear("date").alias("week_number"),
        date_trunc("week", col("date")).alias("week_start"),
        dayofweek("date").alias("day_of_week"),
        date_format(col("date"), "EEEE").alias("day_name"),
        dayofweek("date").isin(1, 7).alias("is_weekend"),
    )
)

save(dim_date, "dim_date")

# COMMAND ----------

# ============================================
# FACTS
# ============================================

items = silver("silver_order_items").select(
    "order_item_id", "order_id", "product_id",
    "quantity", "unit_price", "discount_amount",
    "tax_amount", "total_amount",
)

orders = silver("silver_orders").select(
    "order_id", "customer_id", "order_timestamp", "order_status",
    "sales_channel", "payment_method", "coupon_code", "currency",
)

dim_c_keys = gold("dim_customer").select("customer_id", "customer_sk")
dim_p_keys = gold("dim_product").select("product_id", "product_sk", "cost")

fact_sales = (
    items
    .join(orders, on="order_id", how="inner")
    .join(dim_c_keys, on="customer_id", how="inner")
    .join(dim_p_keys, on="product_id", how="inner")
    .withColumn(
        "date_sk",
        date_format(col("order_timestamp"), "yyyyMMdd").cast("int"),
    )
    .withColumn("gross_amount", round(col("quantity") * col("unit_price"), 2))
    .withColumn(
        "net_revenue",
        round(col("quantity") * col("unit_price") - col("discount_amount"), 2),
    )
    .withColumn("cost_amount", round(col("quantity") * col("cost"), 2))
    .withColumn(
        "margin_amount",
        round(
            col("quantity") * col("unit_price")
            - col("discount_amount")
            - col("quantity") * col("cost"),
            2,
        ),
    )
    .select(
        # keys
        "order_item_id", "order_id", "customer_sk", "product_sk",
        "date_sk", "order_timestamp",
        # degenerate dimensions
        "order_status", "sales_channel", "payment_method",
        "coupon_code", "currency",
        # measures
        "quantity", "unit_price", "gross_amount", "discount_amount",
        "tax_amount", "net_revenue", "total_amount",
        "cost_amount", "margin_amount",
    )
)

save(fact_sales, "fact_sales")

# COMMAND ----------

fact_payments = (
    silver("silver_payments")
    .select(
        "payment_id", "order_id", "customer_id", "payment_timestamp",
        "payment_method", "payment_provider", "payment_status",
        "currency", "amount",
    )
    .join(dim_c_keys, on="customer_id", how="inner")
    .withColumn(
        "date_sk",
        date_format(col("payment_timestamp"), "yyyyMMdd").cast("int"),
    )
    .select(
        "payment_id", "order_id", "customer_sk", "date_sk",
        "payment_timestamp", "payment_method", "payment_provider",
        "payment_status", "currency", "amount",
    )
)

save(fact_payments, "fact_payments")

# COMMAND ----------

fact_returns = (
    silver("silver_returns")
    .select(
        "return_id", "order_id", "order_item_id", "product_id",
        "return_date", "return_reason", "return_status",
        "quantity_returned", "refund_amount",
    )
    .join(
        gold("dim_product").select("product_id", "product_sk"),
        on="product_id",
        how="inner",
    )
    .withColumn(
        "date_sk", date_format(col("return_date"), "yyyyMMdd").cast("int")
    )
    .select(
        "return_id", "order_id", "order_item_id", "product_sk", "date_sk",
        "return_date", "return_reason", "return_status",
        "quantity_returned", "refund_amount",
    )
)

save(fact_returns, "fact_returns")

# COMMAND ----------

events = silver("silver_web_events").select(
    "event_id", "session_id", "customer_id", "event_type",
    "event_timestamp", "device", "traffic_source", "product_id",
)

sessions = (
    events
    .groupBy("session_id")
    .agg(
        # a session belongs to one customer / device / source:
        # take the first value seen
        first("customer_id", ignorenulls=True).alias("customer_id"),
        first("device", ignorenulls=True).alias("device"),
        first("traffic_source", ignorenulls=True).alias("traffic_source"),
        min("event_timestamp").alias("session_start"),
        max("event_timestamp").alias("session_end"),
        count(lit(1)).alias("event_count"),
        sum(when(col("event_type") == "page_view", 1)
            .otherwise(0)).alias("page_views"),
        sum(when(col("event_type") == "product_view", 1)
            .otherwise(0)).alias("product_views"),
        sum(when(col("event_type") == "search", 1)
            .otherwise(0)).alias("searches"),
        sum(when(col("event_type") == "add_to_cart", 1)
            .otherwise(0)).alias("add_to_carts"),
        sum(when(col("event_type") == "remove_from_cart", 1)
            .otherwise(0)).alias("cart_removals"),
        sum(when(col("event_type") == "begin_checkout", 1)
            .otherwise(0)).alias("checkouts"),
        countDistinct("product_id").alias("distinct_products_viewed"),
    )
    .withColumn(
        "duration_seconds",
        unix_timestamp("session_end") - unix_timestamp("session_start"),
    )
    .withColumn("is_bounce", col("event_count") == 1)
    .withColumn("reached_cart", col("add_to_carts") > 0)
    .withColumn("reached_checkout", col("checkouts") > 0)
)

# --- conversion: did this customer order during the session window? ---

completed_orders = (
    gold("fact_sales")
    .filter(col("order_status") == "completed")
    .select("order_id", "customer_sk", "order_timestamp")
    .distinct()
)

# bring customer_sk onto the sessions so the join is on the surrogate key
sessions_keyed = sessions.join(
    gold("dim_customer").select("customer_id", "customer_sk"),
    on="customer_id",
    how="left",
)

conversions = (
    sessions_keyed.alias("s")
    .join(
        completed_orders.alias("o"),
        (col("s.customer_sk") == col("o.customer_sk"))
        & (col("o.order_timestamp") >= col("s.session_start"))
        & (col("o.order_timestamp") <= col("s.session_end")),
        how="inner",
    )
    .groupBy(col("s.session_id").alias("session_id"))
    .agg(
        # if several orders fall in the window, keep the first one
        min(col("o.order_timestamp")).alias("conversion_timestamp"),
        first(col("o.order_id")).alias("order_id"),
    )
)

fact_sessions = (
    sessions_keyed
    .join(conversions, on="session_id", how="left")
    .withColumn(
        "date_sk",
        date_format(col("session_start"), "yyyyMMdd").cast("int"),
    )
    .withColumn("converted", col("order_id").isNotNull())
    .select(
        # keys
        "session_id",
        "customer_sk",
        "date_sk",
        "order_id",
        # attributes
        "device",
        "traffic_source",
        "session_start",
        "session_end",
        "duration_seconds",
        # measures
        "event_count",
        "page_views",
        "product_views",
        "searches",
        "add_to_carts",
        "cart_removals",
        "checkouts",
        "distinct_products_viewed",
        # flags
        "is_bounce",
        "reached_cart",
        "reached_checkout",
        "converted",
    )
)

save(fact_sessions, "fact_sessions")

# COMMAND ----------

# ============================================
# REPORTING MARTS
# ============================================

f = gold("fact_sales").filter(col("order_status") == "completed")
d = gold("dim_date").select(
    "date_sk", "year", "quarter_name", "month_name", "month_start"
)

mart_sales_summary = (
    f.join(d, on="date_sk", how="inner")
    .join(
        gold("dim_customer").select("customer_sk", "country"),
        on="customer_sk", how="inner",
    )
    .join(
        gold("dim_product").select("product_sk", "category"),
        on="product_sk", how="inner",
    )
    .groupBy(
        "month_start", "year",
        col("quarter_name").alias("quarter"),
        col("month_name").alias("month"),
        "country", "category", "sales_channel",
    )
    .agg(
        countDistinct("order_id").alias("orders"),
        count(lit(1)).alias("order_lines"),
        sum("quantity").alias("units_sold"),
        countDistinct("customer_sk").alias("active_customers"),
        round(sum("net_revenue"), 2).alias("revenue"),
        round(sum("discount_amount"), 2).alias("discounts"),
        round(sum("margin_amount"), 2).alias("margin"),
    )
    .withColumn(
        "margin_pct", round(safe_div(col("margin"), col("revenue")), 4)
    )
    .withColumn(
        "avg_order_value", round(safe_div(col("revenue"), col("orders")), 2)
    )
)

save(mart_sales_summary, "mart_sales_summary")

# COMMAND ----------

sales_by_product = (
    gold("fact_sales")
    .filter(col("order_status") == "completed")
    .groupBy("product_sk")
    .agg(
        countDistinct("order_id").alias("orders"),
        sum("quantity").alias("units_sold"),
        round(sum("net_revenue"), 2).alias("revenue"),
        round(sum("margin_amount"), 2).alias("margin"),
    )
)

returns_by_product = (
    gold("fact_returns")
    .groupBy("product_sk")
    .agg(
        sum("quantity_returned").alias("units_returned"),
        round(sum("refund_amount"), 2).alias("refunded"),
    )
)

reviews_by_product = (
    silver("silver_reviews")
    .groupBy("product_id")
    .agg(
        count(lit(1)).alias("review_count"),
        round(avg("rating"), 2).alias("avg_rating"),
    )
)

mart_product_performance = (
    gold("dim_product")
    .select(
        "product_sk", "product_id", "product_name", "category",
        "subcategory", "brand", "price_tier", "list_price",
    )
    .join(sales_by_product, on="product_sk", how="left")
    .join(returns_by_product, on="product_sk", how="left")
    .join(reviews_by_product, on="product_id", how="left")
    .withColumn("has_no_reviews", col("review_count").isNull())
    .withColumn("orders", coalesce(col("orders"), lit(0)))
    .withColumn("units_sold", coalesce(col("units_sold"), lit(0)))
    .withColumn("revenue", coalesce(col("revenue"), lit(0.0)))
    .withColumn("margin", coalesce(col("margin"), lit(0.0)))
    .withColumn("units_returned", coalesce(col("units_returned"), lit(0)))
    .withColumn("review_count", coalesce(col("review_count"), lit(0)))
    .withColumn(
        "return_rate",
        round(safe_div(col("units_returned"), col("units_sold")), 4),
    )
)

save(mart_product_performance, "mart_product_performance")

# COMMAND ----------

sales_by_customer = (
    gold("fact_sales")
    .filter(col("order_status") == "completed")
    .groupBy("customer_sk")
    .agg(
        countDistinct("order_id").alias("lifetime_orders"),
        round(sum("net_revenue"), 2).alias("lifetime_revenue"),
        min("order_timestamp").alias("first_order_at"),
        max("order_timestamp").alias("last_order_at"),
    )
)

mart_customer_360 = (
    gold("dim_customer")
    .select(
        "customer_sk", "customer_id", "full_name", "country", "city",
        "age_band", "customer_segment", "acquisition_channel", "signup_date",
    )
    .join(sales_by_customer, on="customer_sk", how="left")
    .withColumn(
        "avg_order_value",
        round(safe_div(col("lifetime_revenue"), col("lifetime_orders")), 2),
    )
    .withColumn(
        "days_since_last_order",
        datediff(current_date(), col("last_order_at")),
    )
    .withColumn(
        "lifecycle_stage",
        when(col("lifetime_orders").isNull(), "Never purchased")
        .when(col("days_since_last_order") <= 90, "Active")
        .when(col("days_since_last_order") <= 365, "Lapsing")
        .otherwise("Churned"),
    )
    .withColumn("lifetime_orders", coalesce(col("lifetime_orders"), lit(0)))
    .withColumn(
        "lifetime_revenue", coalesce(col("lifetime_revenue"), lit(0.0))
    )
)

save(mart_customer_360, "mart_customer_360")

# COMMAND ----------

weekly_events = (
    silver("silver_web_events")
    .withColumn("week_start", date_trunc("week", col("event_timestamp")))
    .groupBy("week_start", "traffic_source", "device")
    .agg(
        countDistinct("session_id").alias("sessions"),
        countDistinct("customer_id").alias("users"),
        sum(when(col("event_type") == "product_view", 1).otherwise(0))
            .alias("product_views"),
        sum(when(col("event_type") == "add_to_cart", 1).otherwise(0))
            .alias("add_to_carts"),
        sum(when(col("event_type") == "begin_checkout", 1).otherwise(0))
            .alias("checkouts"),
    )
)

weekly_sales = (
    gold("fact_sales")
    .filter(col("order_status") == "completed")
    .withColumn("week_start", date_trunc("week", col("order_timestamp")))
    .groupBy("week_start")
    .agg(
        countDistinct("order_id").alias("orders_that_week"),
        round(sum("net_revenue"), 2).alias("revenue_that_week"),
    )
)

mart_web_funnel_weekly = (
    weekly_events
    .join(weekly_sales, on="week_start", how="left")
    .withColumn(
        "view_to_cart_rate",
        round(safe_div(col("add_to_carts"), col("product_views")), 4),
    )
    .withColumn(
        "cart_to_checkout_rate",
        round(safe_div(col("checkouts"), col("add_to_carts")), 4),
    )
)

save(mart_web_funnel_weekly, "mart_web_funnel_weekly")

# COMMAND ----------

# ============================================
# CONSTRAINTS (informational - Power BI reads them)
# ============================================

spark.sql(f"ALTER TABLE {GOLD}.dim_customer "
    f"ALTER COLUMN customer_sk SET NOT NULL")

spark.sql(f"ALTER TABLE {GOLD}.dim_product "
    f"ALTER COLUMN product_sk SET NOT NULL")

spark.sql(f"ALTER TABLE {GOLD}.dim_date "
    f"ALTER COLUMN date_sk SET NOT NULL")

constraints = [
    # primary keys
    "ALTER TABLE {g}.dim_customer ADD CONSTRAINT pk_dim_customer "
    "PRIMARY KEY (customer_sk) RELY",

    "ALTER TABLE {g}.dim_product ADD CONSTRAINT pk_dim_product "
    "PRIMARY KEY (product_sk) RELY",

    "ALTER TABLE {g}.dim_date ADD CONSTRAINT pk_dim_date "
    "PRIMARY KEY (date_sk) RELY",

    # foreign keys
    "ALTER TABLE {g}.fact_sales ADD CONSTRAINT fk_sales_customer "
    "FOREIGN KEY (customer_sk) REFERENCES {g}.dim_customer RELY",

    "ALTER TABLE {g}.fact_sales ADD CONSTRAINT fk_sales_product "
    "FOREIGN KEY (product_sk) REFERENCES {g}.dim_product RELY",

    "ALTER TABLE {g}.fact_sales ADD CONSTRAINT fk_sales_date "
    "FOREIGN KEY (date_sk) REFERENCES {g}.dim_date RELY",

    "ALTER TABLE {g}.fact_payments ADD CONSTRAINT fk_payments_customer "
    "FOREIGN KEY (customer_sk) REFERENCES {g}.dim_customer RELY",

    "ALTER TABLE {g}.fact_returns ADD CONSTRAINT fk_returns_product "
    "FOREIGN KEY (product_sk) REFERENCES {g}.dim_product RELY",
]

for stmt in constraints:
    try:
        spark.sql(stmt.format(g=GOLD))
    except Exception as e:
        print(f"skipped: {e}")

print("Constraints applied.")

print("Gold layer complete.")
