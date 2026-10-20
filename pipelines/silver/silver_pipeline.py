# Silver Layer — Lakeflow Declarative Pipeline (DLT)
#
# Pattern per dataset:
#    stage (stream from bronze) -> trans (dedup + casts) ->
#    silver (expectations enforced) + quarantine (rejected rows)
#
# Orders arrive from two sources: the historical batch (flat CSV) and
# the real-time stream (nested JSON). The stream is flattened and its
# items exploded, then both are unioned BEFORE the rules run, so every
# order goes through exactly the same checks.
#
# Every rule below maps 1:1 to an issue injected by
# `data_generator/generators/data_quality.py` — quarantine counts should match
# the `[dirty]` report from the generation run.


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

BRONZE_ROOT = "/Volumes/ecommerce/bronze/bronzevolume"

DATASETS = [
    "customers", "products", "orders", "order_items",
    "payments", "shipments", "returns", "web_events", "reviews",
    "orders_stream",   # real-time JSON events (bronze/03_bronze_orders_stream)
]

# Stage tables (one per dataset, streaming from bronze)

def make_stage(name):
    @dlt.table(name=f"stage_{name}")
    def stage():
        return (
            spark.readStream.format("delta")
            .load(f"{BRONZE_ROOT}/{name}/data/")
        )
    return stage

for _name in DATASETS:
    make_stage(_name)

# ====================================================================
# CUSTOMERS
# ====================================================================

@dlt.view(name="trans_customers")
def trans_customers():
    return (
        spark.readStream.table("stage_customers")
        .withColumn("date_of_birth",
                    try_to_date(col("date_of_birth"), "yyyy-MM-dd"))
        .withColumn("signup_date",
                    try_to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("_ingested_at", current_timestamp())
        .dropDuplicates(["customer_id"])
        .drop("_rescued_data")
    )

customers_rules = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_email": "email IS NOT NULL",
    "valid_dob":
        "date_of_birth BETWEEN '1920-01-01' AND current_date()",
    "valid_segment": "customer_segment IN ('Genius', 'Regular')",
}

dlt.create_streaming_table(
    name="silver_customers",
    expect_all_or_drop = customers_rules
)
dlt.create_auto_cdc_flow(
  target = "silver_customers",
  source = "trans_customers",
  keys = ["customer_id"],
  sequence_by = col("_ingested_at"),
  stored_as_scd_type = "1"
)

@dlt.table(name="quarantine_customers")
def quarantine_customers():
    return spark.readStream.table("trans_customers").filter(
        "customer_id IS NULL OR email IS NULL "
        "OR date_of_birth NOT BETWEEN '1920-01-01' AND current_date() "
        "OR customer_segment NOT IN ('Genius', 'Regular')"
    )

# ====================================================================
# PRODUCTS
# ====================================================================

@dlt.view(name="trans_products")
def trans_products():
    return (
        spark.readStream.table("stage_products")
        .withColumn("cost", col("cost").cast("double"))
        .withColumn("list_price", col("list_price").cast("double"))
        .withColumn("launch_date",
                    try_to_date(col("launch_date"), "yyyy-MM-dd"))
        .withColumn("_ingested_at", current_timestamp())
        .dropDuplicates(["product_id"])
        .drop("_rescued_data")
    )


products_rules = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_cost": "cost > 0",
    "valid_category": "category IS NOT NULL",
    "cost_below_price": "cost <= list_price",
}

dlt.create_streaming_table(
    name="silver_products",
    expect_all_or_drop = products_rules
)
dlt.create_auto_cdc_flow(
  target = "silver_products",
  source = "trans_products",
  keys = ["product_id"],
  sequence_by = col("_ingested_at"),
  stored_as_scd_type = "1"
)


@dlt.table(name="quarantine_products")
def quarantine_products():
    return spark.readStream.table("trans_products").filter(
        "product_id IS NULL OR cost <= 0 "
        "OR category IS NULL OR cost > list_price"
    )

# ====================================================================
# ORDERS
# ====================================================================


VALID_ORDER_STATUSES = (
    "'completed','cancelled','refunded',"
    "'partially_refunded','pending'"
)


ORDER_COLS = [
    "order_id", "customer_id", "order_timestamp", "order_status",
    "currency", "payment_method", "sales_channel", "coupon_code",
]

ORDER_ITEM_COLS = [
    "order_item_id", "order_id", "product_id", "quantity",
    "unit_price", "discount_amount", "tax_amount", "total_amount",
]


@dlt.view(name="stream_orders_flat")
def stream_orders_flat():
    """Order header from the JSON stream, aligned to the batch schema."""
    items_total = aggregate(
        col("items"), lit(0.0),
        lambda acc, x: acc + x["total_amount"].cast("double"),
    )
    return (
        spark.readStream.table("stage_orders_stream")
        .select(
            *[col(c).cast("string").alias(c) for c in ORDER_COLS
              if c != "coupon_code"],
            lit(None).cast("string").alias("coupon_code"),
            lit("stream").alias("source_system"),
            col("order_total").cast("double").alias("_stream_order_total"),
            round(items_total, 2).alias("_stream_items_total"),
        )
    )


@dlt.view(name="stream_order_items_flat")
def stream_order_items_flat():
    """One row per item of each streamed order (explode the array)."""
    return (
        spark.readStream.table("stage_orders_stream")
        .select("order_id", explode(col("items")).alias("item"))
        .select(
            # OS<ts><seq> -> OIS<ts><seq><line>, never collides with OI...
            concat(
                lit("OIS"), expr("substring(order_id, 3)"),
                lpad(col("item.line_number").cast("string"), 2, "0"),
            ).alias("order_item_id"),
            col("order_id").cast("string").alias("order_id"),
            *[col(f"item.{c}").cast("string").alias(c)
              for c in ORDER_ITEM_COLS[2:]],
            lit("stream").alias("source_system"),
        )
    )


@dlt.view(name="trans_orders")
def trans_orders():
    batch = (
        spark.readStream.table("stage_orders")
        .select(*ORDER_COLS)
        .withColumn("source_system", lit("batch"))
        .withColumn("_stream_order_total", lit(None).cast("double"))
        .withColumn("_stream_items_total", lit(None).cast("double"))
    )
    df = (
        batch.unionByName(spark.readStream.table("stream_orders_flat"))
        .withColumn("order_timestamp",
                    to_timestamp(col("order_timestamp")))
        .withColumn("order_status", lower(col("order_status")))
        .dropDuplicates(["order_id"])
    )
    # FK flag against silver customers (materialized read)
    valid_customers = dlt.read("silver_customers") \
        .select("customer_id").distinct()
    return df.join(
        valid_customers.withColumn("_customer_ok", lit(True)),
        on="customer_id", how="left",
    )


orders_rules = {
    "valid_order_id": "order_id IS NOT NULL",
    "known_customer": "_customer_ok = true",
    "not_in_future": "order_timestamp <= current_timestamp()",
    "valid_status": f"order_status IN ({VALID_ORDER_STATUSES})",
    # stream only: header total must equal the sum of its items
    "stream_total_matches_items":
        "_stream_items_total IS NULL "
        "OR abs(_stream_order_total - _stream_items_total) <= 0.05",
}


@dlt.table(name="silver_orders")
@dlt.expect_all_or_drop(orders_rules)
def silver_orders():
    return spark.readStream.table("trans_orders")

@dlt.table(name="quarantine_orders")
def quarantine_orders():
    return spark.readStream.table("trans_orders").filter(
        f"order_id IS NULL OR _customer_ok IS NULL "
        f"OR order_timestamp > current_timestamp() "
        f"OR order_status NOT IN ({VALID_ORDER_STATUSES}) "
        f"OR (_stream_items_total IS NOT NULL "
        f"    AND abs(_stream_order_total - _stream_items_total) > 0.05)"
    )

# ====================================================================
# ORDER ITEMS
# ====================================================================

@dlt.view(name="trans_order_items")
def trans_order_items():
    batch = (
        spark.readStream.table("stage_order_items")
        .select(*ORDER_ITEM_COLS)
        .withColumn("source_system", lit("batch"))
    )
    df = (
        batch.unionByName(spark.readStream.table("stream_order_items_flat"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("discount_amount",
                    col("discount_amount").cast("double"))
        .withColumn("tax_amount", col("tax_amount").cast("double"))
        .withColumn("total_amount",
                    col("total_amount").cast("double"))
        .dropDuplicates(["order_item_id"])
    )
    valid_products = dlt.read("silver_products") \
        .select("product_id").distinct()
    df = df.join(
        valid_products.withColumn("_product_ok", lit(True)),
        on="product_id", how="left",
    )
    # recomputed total for the amount check
    return df.withColumn(
        "_expected_total",
        round(col("quantity") * col("unit_price")
              - col("discount_amount") + col("tax_amount"), 2),
    )


order_items_rules = {
    "valid_item_id": "order_item_id IS NOT NULL",
    "known_product": "_product_ok = true",
    "positive_quantity": "quantity > 0",
    "correct_total": "abs(total_amount - _expected_total) <= 0.05",
}


@dlt.table(name="silver_order_items")
@dlt.expect_all_or_drop(order_items_rules)
def silver_order_items():
    return spark.readStream.table("trans_order_items")


@dlt.table(name="quarantine_order_items")
def quarantine_order_items():
    return spark.readStream.table("trans_order_items").filter(
        "order_item_id IS NULL OR _product_ok IS NULL "
        "OR quantity <= 0 "
        "OR abs(total_amount - _expected_total) > 0.05"
    )

# ====================================================================
# PAYMENTS
# ====================================================================

@dlt.view(name="trans_payments")
def trans_payments():
    df = (
        spark.readStream.table("stage_payments")
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("payment_timestamp",
                    to_timestamp(col("payment_timestamp")))
        .dropDuplicates(["payment_id"])
        .drop("_rescued_data")
    )
    orders_ref = dlt.read("silver_orders") \
        .select("order_id",
                col("order_status").alias("_order_status"))
    items_sum = dlt.read("silver_order_items") \
        .groupBy("order_id") \
        .agg(round(sum("total_amount"), 2).alias("_items_total"))
    return (
        df.join(orders_ref, on="order_id", how="left")
          .join(items_sum, on="order_id", how="left")
    )


payments_rules = {
    "valid_payment_id": "payment_id IS NOT NULL",
    "known_order": "_order_status IS NOT NULL",
    "amount_matches_items":
        "_items_total IS NULL "
        "OR abs(amount - _items_total) <= 0.05",
    "status_consistent":
        "NOT (payment_status = 'captured' "
        "AND _order_status = 'cancelled')",
}


@dlt.table(name="silver_payments")
@dlt.expect_all_or_drop(payments_rules)
def silver_payments():
    return spark.readStream.table("trans_payments")

@dlt.table(name="quarantine_payments")
def quarantine_payments():
    return spark.readStream.table("trans_payments").filter(
        "payment_id IS NULL OR _order_status IS NULL "
        "OR (_items_total IS NOT NULL "
        "    AND abs(amount - _items_total) > 0.05) "
        "OR (payment_status = 'captured' "
        "    AND _order_status = 'cancelled')"
    )

# ====================================================================
# SHIPMENTS
# ====================================================================

VALID_CARRIERS = "'DHL','DPD','GLS','Hermes','UPS'"


@dlt.view(name="trans_shipments")
def trans_shipments():
    return (
        spark.readStream.table("stage_shipments")
        .withColumn("shipped_at", to_timestamp(col("shipped_at")))
        .withColumn("delivered_at",
                    to_timestamp(col("delivered_at")))
        .withColumn("shipping_cost",
                    col("shipping_cost").cast("double"))
        .dropDuplicates(["shipment_id"])
        .drop("_rescued_data")
    )


shipments_rules = {
    "valid_shipment_id": "shipment_id IS NOT NULL",
    "delivery_after_shipping": "delivered_at >= shipped_at",
    "known_carrier": f"carrier IN ({VALID_CARRIERS})",
}


@dlt.table(name="silver_shipments")
@dlt.expect_all_or_drop(shipments_rules)
def silver_shipments():
    return spark.readStream.table("trans_shipments")


@dlt.table(name="quarantine_shipments")
def quarantine_shipments():
    return spark.readStream.table("trans_shipments").filter(
        f"shipment_id IS NULL OR delivered_at < shipped_at "
        f"OR carrier NOT IN ({VALID_CARRIERS})"
    )


# "Missing shipment" is an absence, not a bad row ->
# audited as completed orders without any shipment
@dlt.table(name="audit_orders_missing_shipment")
def audit_orders_missing_shipment():
    orders_c = dlt.read("silver_orders") \
        .filter("order_status = 'completed'")
    shipped = dlt.read("silver_shipments") \
        .select("order_id").distinct()
    return orders_c.join(shipped, on="order_id", how="left_anti")

# ====================================================================
# RETURNS
# ====================================================================

@dlt.view(name="trans_returns")
def trans_returns():
    df = (
        spark.readStream.table("stage_returns")
        .withColumn("return_date",
                    to_timestamp(col("return_date")))
        .withColumn("refund_amount",
                    col("refund_amount").cast("double"))
        .withColumn("quantity_returned", col("quantity_returned").cast("int"))
        .dropDuplicates(["return_id"])
        .drop("_rescued_data")
    )
    items_ref = dlt.read("silver_order_items").select(
        "order_item_id",
        col("total_amount").alias("_item_total"),
    )
    deliveries = dlt.read("silver_shipments").select(
        "order_id", col("delivered_at").alias("_delivered_at"),
    )
    orders_ref = dlt.read("silver_orders").select(
        "order_id",
        col("order_timestamp").alias("_order_timestamp"),
    )
    return (
        df.join(items_ref, on="order_item_id", how="left")
          .join(deliveries, on="order_id", how="left")
          .join(orders_ref, on="order_id", how="left")
    )


returns_rules = {
    "valid_return_id": "return_id IS NOT NULL",
    "return_after_delivery":
        "_delivered_at IS NULL OR return_date >= _delivered_at",
    "refund_not_excessive":
        "_item_total IS NULL "
        "OR refund_amount <= _item_total + 0.05",
    "order_timestamp":
        "_order_timestamp IS NULL OR return_date >= _order_timestamp",
}


@dlt.table(name="silver_returns")
@dlt.expect_all_or_drop(returns_rules)
def silver_returns():
    return spark.readStream.table("trans_returns")

@dlt.table(name="quarantine_returns")
def quarantine_returns():
    return spark.readStream.table("trans_returns").filter(
        "return_id IS NULL "
        "OR (_delivered_at IS NOT NULL "
        "    AND return_date < _delivered_at) "
        "OR (_item_total IS NOT NULL "
        "    AND refund_amount > _item_total + 0.05)"
        "OR (_order_timestamp IS NOT NULL "
        "    AND return_date < _order_timestamp)"
    )

# ====================================================================
# WEB EVENTS
# ====================================================================

PRODUCT_EVENTS = "'product_view','add_to_cart','remove_from_cart'"


@dlt.view(name="trans_web_events")
def trans_web_events():
    df = (
        spark.readStream.table("stage_web_events")
        .withColumn("event_timestamp",
                    to_timestamp(col("event_timestamp")))
        .dropDuplicates(["event_id"])
        .drop("_rescued_data")
    )
    valid_products = dlt.read("silver_products") \
        .select("product_id").distinct()
    return df.join(
        valid_products.withColumn("_product_ok", lit(True)),
        on="product_id", how="left",
    )


web_events_rules = {
    "valid_event_id": "event_id IS NOT NULL",
    "has_customer": "customer_id IS NOT NULL",
    "product_events_have_valid_product":
        f"event_type NOT IN ({PRODUCT_EVENTS}) "
        f"OR _product_ok = true",
}


@dlt.table(name="silver_web_events")
@dlt.expect_all_or_drop(web_events_rules)
def silver_web_events():
    return spark.readStream.table("trans_web_events")


@dlt.table(name="quarantine_web_events")
def quarantine_web_events():
    return spark.readStream.table("trans_web_events").filter(
        f"event_id IS NULL OR customer_id IS NULL "
        f"OR (event_type IN ({PRODUCT_EVENTS}) "
        f"    AND _product_ok IS NULL)"
    )

# ====================================================================
# REVIEWS
# ====================================================================

@dlt.view(name="trans_reviews")
def trans_reviews():
    df = (
        spark.readStream.table("stage_reviews")
        .withColumn("rating", col("rating").cast("int"))
        .withColumn("review_date",
                    to_timestamp(col("review_date")))
        .dropDuplicates(["review_id"])
        .drop("_rescued_data")
    )
    valid_products = dlt.read("silver_products") \
        .select("product_id").distinct()
    return df.join(
        valid_products.withColumn("_product_ok", lit(True)),
        on="product_id", how="left",
    )


reviews_rules = {
    "valid_review_id": "review_id IS NOT NULL",
    "rating_in_range": "rating BETWEEN 1 AND 5",
    "known_product": "_product_ok = true",
}


@dlt.table(name="silver_reviews")
@dlt.expect_all_or_drop(reviews_rules)
def silver_reviews():
    return spark.readStream.table("trans_reviews")

@dlt.table(name="quarantine_reviews")
def quarantine_reviews():
    return spark.readStream.table("trans_reviews").filter(
        "review_id IS NULL "
        "OR rating NOT BETWEEN 1 AND 5 "
        "OR _product_ok IS NULL"
    )
