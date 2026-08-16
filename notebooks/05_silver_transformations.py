# Databricks notebook source

# Joining Products, Categories & Suppliers tables into single Table(products)
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ BRONZE CHANGE TABLES
# ============================================================
categories = spark.read.table(
    "batch_process.bronze.categories_changes"
)

products = spark.read.table(
    "batch_process.bronze.products_changes"
)

suppliers = spark.read.table(
    "batch_process.bronze.suppliers_changes"
)

# ============================================================
# 3. JOIN PRODUCTS WITH CATEGORIES AND SUPPLIERS
# ============================================================
df = products.alias("p").join(
    categories.alias("c"),
    col("p.category_id") == col("c.category_id")
).join(
    suppliers.alias("s"),
    col("p.supplier_id") == col("s.supplier_id")
).select(
    col("p.product_id"),
    col("p.product_name"),
    col("c.category_name"),
    col("p.quantity_per_unit"),
    col("p.unit_price"),
    col("p.units_in_stock"),
    col("p.units_on_order"),
    col("p.reorder_level"),
    col("p.discontinued"),
    col("s.company_name").alias("supplier_name"),
    col("s.contact_name").alias("supplier_contact"),
    col("s.contact_title").alias("supplier_contact_title"),
    col("s.address").alias("supplier_address"),
    col("s.city").alias("supplier_city"),
    col("s.region").alias("supplier_region"),
    col("s.postal_code").alias("supplier_postal_code"),
    col("s.country").alias("supplier_country"),
    col("s.phone").alias("supplier_phone"),
    col("s.fax").alias("supplier_fax")
)


# ============================================================
# 4. ADD UPDATED TIMESTAMP
# ============================================================
df = (
    df
    .withColumn("updated_time", current_timestamp())
)

# ============================================================
# 6. WRITE TO SILVER TABLE
# ============================================================
df.write \
    .mode("overwrite") \
    .saveAsTable("batch_process.silver.products")

# COMMAND ----------

# Joining Orders & Order_Details tables into single Table(orders)
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ============================================================
# 2. READ BRONZE CHANGE TABLES
# ============================================================
orders = spark.read.table(
    "batch_process.bronze.orders_changes"
)

order_details = spark.read.table(
    "batch_process.bronze.order_details_changes"
)

# ============================================================
# 3. JOIN ORDERS WITH ORDER DETAILS
# ============================================================
df = (
    orders.alias("o")
    .join(
        order_details.alias("od"),
        col("o.order_id") == col("od.order_id")
    )
)

# ============================================================
# 4. CALCULATE LINE AMOUNT AND DERIVE FREIGHT CHARGES
# ============================================================
df = df.withColumn(
    "line_amount",
    col("unit_price") *
    col("quantity") *
    (1 - col("discount"))
)

# DEFINE WINDOW FOR EACH ORDER
w = Window.partitionBy("o.order_id")  

# CALCULATE TOTAL ORDER AMOUNT
df = df.withColumn(
    "total_order_amount",
    sum("line_amount").over(w)
)                                        

# ALLOCATE FREIGHT ACROSS ORDER LINES
df = df.withColumn(
    "allocate_freight",
    round(
        col("o.freight") *
        col("line_amount") /
        col("total_order_amount"),
        2
    )
)

# ============================================================
# 5. SELECT REQUIRED SILVER COLUMNS
# ============================================================

df = df.select(
    col("od.order_id"),
    col("o.customer_id"),
    col("od.product_id"),
    col("od.unit_price"),
    col("od.quantity"),
    col("od.discount"),
    col("allocate_freight").alias("freight_charges"),
    col("o.employee_id"),
    col("o.ship_via").alias("shipper_id"),
    col("o.order_date"),
    col("o.shipped_date")
)

# ============================================================
# 6. ADD UPDATED TIMESTAMP
# ============================================================

df = (
    df
    .withColumn("updated_time", current_timestamp())
)

# ============================================================
# 7. WRITE TO SILVER TABLE
# ============================================================

df.write \
    .mode("overwrite") \
    .saveAsTable("batch_process.silver.orders")

# COMMAND ----------

# CUSTOMERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ BRONZE CUSTOMERS TABLE
# ============================================================
df = spark.read.table(
    "batch_process.bronze.customers_changes"
)

# ============================================================
# 3. ADD UPDATED TIMESTAMP AND DROP UNNECESSARY COLUMN
# ============================================================
df = (
    df
    .withColumn("updated_time", current_timestamp())
    .drop("ingestion_time")
)

# ============================================================
# 4. WRITE TO SILVER CUSTOMERS TABLE
# ============================================================
df.write \
    .mode("overwrite") \
    .saveAsTable("batch_process.silver.customers")

display(df)

# COMMAND ----------

# EMPOYEES
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ BRONZE EMPLOYEES TABLE
# ============================================================
df = spark.read.table(
    "batch_process.bronze.employees_changes"
)

# ============================================================
# 3. ADD UPDATED TIMESTAMP AND DROP UNNECESSARY COLUMN
# ============================================================
df = (
    df
    .withColumn("updated_time", current_timestamp())
    .drop("ingestion_time")
)

# ============================================================
# 4. WRITE TO SILVER EMPLOYEES TABLE
# ============================================================
df.write \
    .mode("overwrite") \
    .saveAsTable("batch_process.silver.employees")

# COMMAND ----------

# # Joining Shipments & Shippers tables into single Table(shipments)
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ BRONZE TABLES
# ============================================================
shippers = spark.read.table(
    "batch_process.bronze.shippers_changes"
)

shipments = spark.read.table(
    "batch_process.bronze.shipments_changes"
)

# ============================================================
# 3. JOIN SHIPMENTS WITH SHIPPERS
# ============================================================
df = (
    shipments.alias("sm")
    .join(
        shippers.alias("sp"),
        col("sm.ship_via") == col("sp.shipper_id")
    )
    .select(
        col("sm.order_id"),
        col("sm.shipped_date"),
        col("sm.ship_name"),
        col("sp.company_name").alias("shipper_name"),
        col("sm.ship_address"),
        col("sm.ship_city"),
        col("sm.ship_region"),
        col("sm.ship_postal_code"),
        col("sm.ship_country"),
        col("sp.phone")
    )
)

# ============================================================
# 4. ADD UPDATED TIMESTAMP
# ============================================================
df = (
    df
    .withColumn("updated_time", current_timestamp())
)

# ============================================================
# 5. WRITE TO SILVER TABLE
# ============================================================
df.write \
    .mode("overwrite") \
    .saveAsTable("batch_process.silver.shipments")
