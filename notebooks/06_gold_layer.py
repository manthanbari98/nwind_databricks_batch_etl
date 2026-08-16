# Databricks notebook source

# SHIPMENTS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ============================================================
# 2. READ SILVER SHIPMENTS TABLE
# ============================================================
df = spark.table(
    "batch_process.silver.shipments"
)

# ============================================================
# 3. DEFINE WINDOW FOR SURROGATE KEY
# ============================================================
window = Window.orderBy(lit(1))

# ============================================================
# 4. GENERATE SHIPMENT SURROGATE KEY
# ============================================================
df = df.withColumn(
    "shipment_sk",
    row_number().over(window)
)

# SELECT SURROGATE KEY FIRST
df = df.select(
    "shipment_sk",
    *[c for c in df.columns if c != "shipment_sk"]
)

# ============================================================
# 6. WRITE TO GOLD DIMENSION TABLE
# ============================================================
df.write \
    .mode("append") \
    .saveAsTable("batch_process.gold.dim_shipments")

# COMMAND ----------

# ============================================================
# DDL for Customers
# ============================================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.gold.dim_customers (
                customer_sk				BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                customer_id				STRING,
                company_name			      STRING,
                contact_name			      STRING,
                contact_title			      STRING,
                cust_address			      STRING,
                city					STRING,
                region					STRING,
                postal_code				STRING,
                country					STRING,
                phone					STRING,
                fax					STRING,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP,
                updated_time                    TIMESTAMP,
                effective_from                  TIMESTAMP,
                effective_to                    TIMESTAMP,
                is_current                      BOOLEAN)
                """)

# COMMAND ----------

# CUSTOMERS TABLE ALONG WITH SCD2
# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable

#=======================================================
# Source & Target
#=======================================================
src = spark.table("batch_process.silver.customers")

gold_table = "batch_process.gold.dim_customers"

trg = DeltaTable.forName(spark, gold_table)

current_df = (
    spark.table(gold_table)
    .filter(col("is_current") == True)
)
#=======================================================
# Step 1 : Identify Changed Records
#=======================================================
changed_df = (
    src.alias("s")
    .join(
        current_df.alias("t"),
        col("s.customer_id") == col("t.customer_id"),
        "inner"
    )
    .filter(col("s.updated_time") > col("t.updated_time"))
    .select("s.*")              # IMPORTANT: removes duplicate columns
)

#=======================================================
# Step 2 : Expire Old Version
#=======================================================
trg.alias("t").merge(
    changed_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    set={
        "is_current": "false",
        "effective_to": "s.updated_time"
    }
).execute()
#=======================================================
# Step 3 : Insert New Version of Changed Records
#=======================================================
changed_insert = (
    changed_df
    .withColumn("effective_from", col("updated_time"))
    .withColumn("effective_to", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True))
)

changed_insert.write \
    .mode("append") \
    .saveAsTable(gold_table)
#=======================================================
# Step 4 : Insert Brand New Customers
#=======================================================
new_customers = (
    src.alias("s")
    .join(
        current_df.alias("t"),
        col("s.customer_id") == col("t.customer_id"),
        "left_anti"
    )
    .withColumn("effective_from", col("updated_time"))
    .withColumn("effective_to", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True))
)

new_customers.write \
    .mode("append") \
    .saveAsTable(gold_table)

# COMMAND ----------

# ============================================================
# DDL FOR PRODUCTS
# ============================================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.gold.dim_products (
                product_sk			      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                product_id			      INT,
                product_name		            STRING,
                category_name			      STRING,
                quantity_per_unit		      STRING,
                unit_price			      DOUBLE,
                units_in_stock		      INT,
                units_on_order		      INT,
                reorder_level		            INT,
                discontinued		            BOOLEAN,
                supplier_name			      STRING,
                supplier_contact		      STRING,
                supplier_contact_title          STRING,
                supplier_address		      STRING,
                supplier_city		            STRING,
                supplier_region		      STRING,
                supplier_postal_code            STRING,
                supplier_country		      STRING,
                supplier_phone		      STRING,
                supplier_fax		            STRING,
                updated_time                    TIMESTAMP,
                effective_from                  TIMESTAMP,
                effective_to                    TIMESTAMP,
                is_current                      BOOLEAN)
                """)

# COMMAND ----------

# PRODUCTS TABLE WITH SCD2
# ============================================================
# IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable

#=======================================================
# Source & Target
#=======================================================
src = spark.table("batch_process.silver.products")

gold_table = "batch_process.gold.dim_products"

trg = DeltaTable.forName(spark, gold_table)

current_df = (
    spark.table(gold_table)
    .filter(col("is_current") == True)
)

#=======================================================
# Step 1 : Identify Changed Records
#=======================================================
changed_df = (
    src.alias("s")
    .join(current_df.alias("t"), "product_id")
    .filter(
        (col("s.updated_time") > col("t.updated_time")) |
        (col("s.discontinued") != col("t.discontinued"))
    )
        .select("s.*")
)

#=======================================================
# Step 2 : Expire Old Version
#=======================================================
trg.alias("t").merge(
    changed_df.alias("s"),
    "t.product_id = s.product_id AND t.is_current = true"
).whenMatchedUpdate(
    set={
        "is_current": "false",
        "effective_to": "s.updated_time"
    }
).execute()

#=======================================================
# Step 3 : Insert New Version of Changed Records
#=======================================================
changed_insert = (
    changed_df
    .withColumn("effective_from", col("updated_time"))
    .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
    .withColumn("is_current", lit(True))
)

changed_insert.write \
    .mode("append") \
    .saveAsTable(gold_table)

#=======================================================
# Step 4 : Insert Brand New Products
#=======================================================
new_products = (
    src.alias("s")
    .join(
        current_df.alias("t"),
        col("s.product_id") == col("t.product_id"),
        "left_anti"
    )
    .withColumn("effective_from", col("updated_time"))
    .withColumn("effective_to", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True))
)

new_products.write \
    .mode("append") \
    .saveAsTable(gold_table)

# COMMAND ----------

# ============================================================
# DLL FOR EMPLOYEES
# ============================================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.gold.dim_employees (
                employee_sk			      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                employee_id			            INT,
                employee_name			        STRING,
                title			                STRING,
                city			                STRING,
                country			                STRING,
                reportsTo		                DOUBLE,
                updated_time                    TIMESTAMP,
                effective_from                  TIMESTAMP,
                effective_to                    TIMESTAMP,
                record_hash				STRING,
                is_current                      BOOLEAN)
                """)


# COMMAND ----------

# EMPLOYEES TABLE WITH SCD2
# ============================================================
# IMPORT
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable

#=======================================================
# Source & Target
#=======================================================
src = spark.table("batch_process.silver.employees")

gold_table = "batch_process.gold.dim_employees"

trg = DeltaTable.forName(spark, gold_table)

current_df = (
    spark.table(gold_table)
    .filter(col("is_current") == True)
)

#=======================================================
# Step 1 : Identify Changed Records
#=======================================================
changed_df = (
    src.alias("s")
    .join(
        current_df.alias("t"),
        col("s.employee_id") == col("t.employee_id"),
        "inner"
    )
    .filter(col("s.updated_time") > col("t.updated_time"))
    .select("s.*")              # IMPORTANT: removes duplicate columns
)

#=======================================================
# Step 2 : Expire Old Version
#=======================================================
trg.alias("t").merge(
    changed_df.alias("s"),
    "t.employee_id = s.employee_id AND t.is_current = true"
).whenMatchedUpdate(
    set={
        "is_current": "false",
        "effective_to": "s.updated_time"
    }
).execute()

#=======================================================
# Step 3 : Insert New Version of Changed Records
#=======================================================
changed_insert = (
    changed_df
    .withColumn("effective_from", col("updated_time"))
    .withColumn("effective_to", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True))
)

changed_insert.write \
    .mode("append") \
    .saveAsTable(gold_table)

#=======================================================
# Step 4 : Insert Brand New Employees
#=======================================================
new_employees = (
    src.alias("s")
    .join(
        current_df.alias("t"),
        col("s.employee_id") == col("t.employee_id"),
        "left_anti"
    )
    .withColumn("effective_from", col("updated_time"))
    .withColumn("effective_to", lit("9999-12-31").cast("date"))
    .withColumn("is_current", lit(True))
)


new_employees.write \
    .mode("append") \
    .saveAsTable(gold_table)

# COMMAND ----------

# ORDERS FACT TABLE WITH SURROGATE KEYS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ SILVER ORDERS TABLE
# ============================================================
orders = spark.table(
    "batch_process.silver.orders"
)

# ============================================================
# 3. READ GOLD DIMENSION TABLES
# ============================================================
dim_customers = spark.table(
    "batch_process.gold.dim_customers"
)

dim_employees = spark.table(
    "batch_process.gold.dim_employees"
)

dim_products = spark.table(
    "batch_process.gold.dim_products"
)

dim_shipments = spark.table(
    "batch_process.gold.dim_shipments"
)

# ============================================================
# 4. JOIN ORDERS WITH GOLD DIMENSIONS
# ============================================================
df = (
    orders.alias("o")
    .join(
        dim_customers
        .filter(col("is_current") == True)
        .alias("c"),
        col("o.customer_id") == col("c.customer_id")
    )
    .join(
        dim_employees
        .filter(col("is_current") == True)
        .alias("e"),
        col("o.employee_id") == col("e.employee_id")
    )
    .join(
        dim_products
        .filter(col("is_current") == True)
        .alias("p"),
        col("o.product_id") == col("p.product_id")
    )
    .join(
        dim_shipments.alias("s"),
        col("o.order_id") == col("s.order_id")
    )
)

# ============================================================
# 5. SELECT REQUIRED FACT COLUMNS
# ============================================================
df = df.select(
    col("o.order_id"),
    col("c.customer_sk"),
    col("e.employee_sk"),
    col("p.product_sk"),
    col("s.shipment_sk"),
    col("o.quantity"),
    col("o.unit_price"),
    col("o.discount"),
    col("o.freight_charges"),
    col("o.order_date")
)

# ============================================================
# 6. WRITE TO GOLD FACT TABLE
# ============================================================
df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("batch_process.gold.fact_orders")
