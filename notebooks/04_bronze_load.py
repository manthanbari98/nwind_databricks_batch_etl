# Databricks notebook source

# AUDIT SCRIPTS
# ============================================================
# 1. IMPORTS
# ============================================================
import uuid
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import Row

# ============================================================
# 2. ETL AUDIT LOG FUNCTION
# ============================================================
def etl_log(
    pipeline_name,
    table_name,
    layer,
    start_time,
    rows_read,
    status,
    error_message=""
):
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    inserted = 0
    updated = 0
    deleted = 0

    if status == "SUCCESS":

        metrics = (
            DeltaTable
            .forName(spark, table_name)
            .history(1)
            .select("operationMetrics")
            .collect()[0][0]
        )

        inserted = int(
            metrics.get("numTargetRowsInserted", 0)
        )

        updated = int(
            metrics.get("numTargetRowsUpdated", 0)
        )

        deleted = int(
            metrics.get("numTargetRowsDeleted", 0)
        )


# ========================================================
# 6. CREATE AUDIT LOG RECORD
# ========================================================

    log = spark.createDataFrame([
        Row(
            run_id=str(uuid.uuid4()),
            pipeline_name=pipeline_name,
            table_name=table_name,
            layer=layer,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            rows_read=rows_read,
            rows_inserted=inserted,
            rows_updated=updated,
            rows_deleted=deleted,
            status=status,
            error_message=error_message
        )
    ])

# ========================================================
# 8. WRITE AUDIT LOG TO DELTA TABLE
# ========================================================
    (
        log.write
        .mode("append")
        .saveAsTable("batch_process.audit.etl_log")
    )


# COMMAND ----------

# CUSTOMERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .load("/Volumes/batch_process/default/source/customers")
    )

# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
    df = (
        df
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("CompanyName", "company_name")
        .withColumnRenamed("ContactName", "contact_name")
        .withColumnRenamed("ContactTitle", "contact_title")
        .withColumnRenamed("Address", "cust_address")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("PostalCode", "postal_code")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("Phone", "phone")
        .withColumnRenamed("Fax", "fax")
    )

# ============================================================
# 3. CREATE RECORD HASH
# ============================================================
    df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("company_name"), lit("")),
                    coalesce(col("contact_name"), lit("")),
                    coalesce(col("contact_title"), lit("")),
                    coalesce(col("cust_address"), lit("")),
                    coalesce(col("city"), lit("")),
                    coalesce(col("region"), lit("")),
                    coalesce(col("postal_code"), lit("")),
                    coalesce(col("country"), lit("")),
                    coalesce(col("phone"), lit("")),
                    coalesce(col("fax"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )

# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
    bronze_customers = DeltaTable.forName(
        spark,
        "batch_process.bronze.customers"
    )

    existing = (
        bronze_customers
        .toDF()
        .select(
            "customer_id",
            "record_hash"
        )
    )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
    changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.customer_id") == col("b.customer_id"),
            "left"
        )
        .filter(
            col("b.customer_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )

# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
    changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.customers_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
    (
        bronze_customers.alias("b")
        .merge(
            changes.alias("s"),
            "b.customer_id = s.customer_id"
        )
        .whenMatchedUpdate(
            condition="b.record_hash <> s.record_hash",
            set={
                "company_name": "s.company_name",
                "contact_name": "s.contact_name",
                "contact_title": "s.contact_title",
                "cust_address": "s.cust_address",
                "city": "s.city",
                "region": "s.region",
                "postal_code": "s.postal_code",
                "country": "s.country",
                "phone": "s.phone",
                "fax": "s.fax",
                "record_hash": "s.record_hash",
                "ingestion_time": "s.ingestion_time"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# ============================================================
# 8. LOG
# ============================================================
    rows_read = df.count()
    rows_changed = changes.count()

    etl_log(
        pipeline_name="BATCH_PROCESS",
        table_name="batch_process.bronze.customers",
        layer="BRONZE",
        start_time=start_time,
        rows_read=rows_read,
        status="SUCCESS"
    )

except Exception as error:

    etl_log(
        pipeline_name="BATCH_PROCESS",
        table_name="batch_process.bronze.customers",
        layer="BRONZE",
        start_time=start_time,
        rows_read=0,
        status="FAILED",
        error_message=str(error)
    )

    raise

# COMMAND ----------

# CATEGORIES
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/categories"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("CategoryID", "category_id")
            .withColumnRenamed("CategoryName", "category_name")
            .withColumnRenamed("Description", "description")
            .withColumnRenamed("Picture", "picture"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================   
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("category_id"), lit("")),
                    coalesce(col("category_name"), lit("")),
                    coalesce(col("description"), lit("")),
                    coalesce(col("picture"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )

# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
      bronze_categories = DeltaTable.forName(
        spark,
        "batch_process.bronze.categories"
      )

      existing = (
        bronze_categories
        .toDF()
        .select(
            "category_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.category_id") == col("b.category_id"),
            "left"
        )
        .filter(
            col("b.category_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )
  
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.categories_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_categories.alias("b")
      .merge(
            changes.alias("s"),
            "b.category_id = s.category_id"
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "category_name": "s.category_name",
            "description": "s.description",
            "ingestion_time": "s.ingestion_time"
        }
      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()

      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.categories",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.categories",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise

# COMMAND ----------

# EMPLOYEES
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/employees"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("employeeID", "employee_id")
            .withColumnRenamed("employeeName", "employee_name")
            .withColumnRenamed("title", "title")
            .withColumnRenamed("city", "city")
            .withColumnRenamed("country", "country")
            .withColumnRenamed("reportsTo", "reportsTo"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================ 
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("employee_id"), lit("")),
                    coalesce(col("employee_name"), lit("")),
                    coalesce(col("title"), lit("")),
                    coalesce(col("city"), lit("")),
                    coalesce(col("country"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )
      
# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
      bronze_employees = DeltaTable.forName(
        spark,
        "batch_process.bronze.employees"
      )

      existing = (
        bronze_employees
        .toDF()
        .select(
            "employee_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.employee_id") == col("b.employee_id"),
            "left"
        )
        .filter(
            col("b.employee_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )
      
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.employees_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_employees.alias("b")
      .merge(
            changes.alias("s"),
            "b.employee_id = s.employee_id"
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "employee_name": "s.employee_name",
            "title": "s.title",
            "city": "s.city",
            "country": "s.country",
            "reportsTo": "s.reportsTo",
            "ingestion_time": "s.ingestion_time"
            }

      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()

      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.employees",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.employees",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise

# COMMAND ----------

# PRODUCTS
# ============================================================
# 1. PRODUCTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/products"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("ProductID", "product_id")
            .withColumnRenamed("ProductName", "product_name")
            .withColumnRenamed("SupplierID","supplier_id")
            .withColumnRenamed("CategoryID", "category_id")
            .withColumnRenamed("QuantityPerUnit", "quantity_per_unit")
            .withColumnRenamed("UnitPrice", "unit_price")
            .withColumnRenamed("UnitsInStock", "units_in_stock")
            .withColumnRenamed("UnitsOnOrder", "units_on_order")
            .withColumnRenamed("ReorderLevel", "reorder_level")
            .withColumnRenamed("Discontinued", "discontinued"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================  
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("product_id"), lit("")),
                    coalesce(col("product_name"), lit("")),
                    coalesce(col("supplier_id"), lit("")),
                    coalesce(col("category_id"), lit("")),
                    coalesce(col("quantity_per_unit"), lit("")),
                    coalesce(col("unit_price"), lit("")),
                    coalesce(col("units_in_stock"), lit("")),
                    coalesce(col("units_on_order"), lit("")),
                    coalesce(col("reorder_level"), lit("")),
                    coalesce(col("discontinued"), lit(""))
                ),  
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )
      
# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
      bronze_products = DeltaTable.forName(
        spark,
        "batch_process.bronze.products"
      )

      existing = (
        bronze_products
        .toDF()
        .select(
            "product_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.product_id") == col("b.product_id"),
            "left"
        )
        .filter(
            col("b.product_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )
      
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.products_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_products.alias("b")
      .merge(
            changes.alias("s"),
            "b.product_id = s.product_id"
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "product_name": "s.product_name",
            "supplier_id": "s.supplier_id",
            "category_id": "s.category_id",
            "quantity_per_unit": "s.quantity_per_unit",
            "unit_price": "s.unit_price",
            "units_in_stock": "s.units_in_stock",
            "units_on_order": "s.units_on_order",
            "reorder_level": "s.reorder_level",
            "discontinued": "s.discontinued",
            "ingestion_time": "s.ingestion_time"
        }
      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()

      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.products",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.products",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise

# COMMAND ----------

# SHIPPERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/shippers"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("ShipperID", "shipper_id")
            .withColumnRenamed("CompanyName", "company_name")
            .withColumnRenamed("Phone", "phone"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================    
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("shipper_id"), lit("")),
                    coalesce(col("company_name"), lit("")),
                    coalesce(col("phone"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
      )

# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================                
      bronze_shippers = DeltaTable.forName(
        spark,
        "batch_process.bronze.shippers"
      )

      existing = (
        bronze_shippers
        .toDF()
        .select(
            "shipper_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.shipper_id") == col("b.shipper_id"),
            "left"
        )
        .filter(
            col("b.shipper_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
      )
      
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.shippers_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_shippers.alias("b")
      .merge(
            changes.alias("s"),
            "b.shipper_id = s.shipper_id"
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "company_name": "s.company_name",
            "phone": "s.phone",
            "ingestion_time": "s.ingestion_time"
        }
      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()
       
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.shippers",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.shippers",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise

# COMMAND ----------

# SUPPLIERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/suppliers"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("SupplierID", "supplier_id")
            .withColumnRenamed("CompanyName", "company_name")
            .withColumnRenamed("ContactName", "contact_name")
            .withColumnRenamed("ContactTitle", "contact_title")
            .withColumnRenamed("Address", "address")
            .withColumnRenamed("City", "city")
            .withColumnRenamed("Region", "region")
            .withColumnRenamed("PostalCode", "postal_code")
            .withColumnRenamed("Country", "country")
            .withColumnRenamed("Phone", "phone")
            .withColumnRenamed("Fax", "fax")
            .withColumnRenamed("HomePage", "home_page"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================  
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("supplier_id"), lit("")),
                    coalesce(col("company_name"), lit("")),
                    coalesce(col("contact_name"), lit("")),
                    coalesce(col("contact_title"), lit("")),
                    coalesce(col("address"), lit("")),
                    coalesce(col("city"), lit("")),
                    coalesce(col("region"), lit("")),
                    coalesce(col("postal_code"), lit("")),
                    coalesce(col("country"), lit("")),
                    coalesce(col("phone"), lit("")),
                    coalesce(col("fax"), lit("")),
                    coalesce(col("home_page"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )
      
# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
      bronze_suppliers = DeltaTable.forName(
        spark,
        "batch_process.bronze.suppliers"
      )

      existing = (
        bronze_suppliers
        .toDF()
        .select(
            "supplier_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.supplier_id") == col("b.supplier_id"),
            "left"
        )
        .filter(
            col("b.supplier_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )
    
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.suppliers_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_suppliers.alias("b")
      .merge(
            changes.alias("s"),
            "b.supplier_id = s.supplier_id"
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "company_name": "s.company_name",
            "contact_name": "s.contact_name",
            "contact_title": "s.contact_title",
            "address": "s.address",
            "city": "s.city",
            "region": "s.region",
            "postal_code": "s.postal_code",
            "country": "s.country",
            "phone": "s.phone",
            "fax": "s.fax",
            "ingestion_time": "s.ingestion_time"
        }
      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()

      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.suppliers",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.suppliers",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise

# COMMAND ----------

# SHIPMENTS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/shipments"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("CustomerID", "customer_id")
            .withColumnRenamed("EmployeeID", "employee_id")
            .withColumnRenamed("OrderDate", "order_date")
            .withColumnRenamed("RequiredDate", "required_date")
            .withColumnRenamed("ShippedDate", "shipped_date")    
            .withColumnRenamed("ShipVia", "ship_via")
            .withColumnRenamed("Freight", "freight")
            .withColumnRenamed("ShipName", "ship_name")
            .withColumnRenamed("ShipAddress", "ship_address")
            .withColumnRenamed("ShipCity", "ship_city")
            .withColumnRenamed("ShipRegion", "ship_region")
            .withColumnRenamed("ShipPostalCode", "ship_postal_code")
            .withColumnRenamed("ShipCountry", "ship_country"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("order_id"), lit("")),
                    coalesce(col("customer_id"), lit("")),
                    coalesce(col("employee_id"), lit("")),
                    coalesce(col("order_date"), lit("")),
                    coalesce(col("required_date"), lit("")),
                    coalesce(col("shipped_date"), lit("")),
                    coalesce(col("ship_via"), lit("")),
                    coalesce(col("freight"), lit("")),
                    coalesce(col("ship_name"), lit("")),
                    coalesce(col("ship_address"), lit("")),
                    coalesce(col("ship_city"), lit("")),
                    coalesce(col("ship_region"), lit("")),
                    coalesce(col("ship_postal_code"), lit("")),
                    coalesce(col("ship_country"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )
      
# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
      bronze_shipments = DeltaTable.forName(
        spark,
        "batch_process.bronze.shipments"
      )

      existing = (
        bronze_shipments
        .toDF()
        .select(
            "order_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.order_id") == col("b.order_id"),
            "left"
        )
        .filter(
            col("b.order_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )
      
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.shipments_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_shipments.alias("b")
      .merge(
            changes.alias("s"),
            "b.order_id = s.order_id"
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "customer_id": "s.customer_id",
            "employee_id": "s.employee_id",
            "order_date": "s.order_date",
            "required_date": "s.required_date",
            "shipped_date": "s.shipped_date",
            "ship_via": "s.ship_via",
            "freight": "s.freight",
            "ship_name": "s.ship_name",
            "ship_address": "s.ship_address",
            "ship_city": "s.ship_city",
            "ship_region": "s.ship_region",
            "ship_postal_code": "s.ship_postal_code",
            "ship_country": "s.ship_country",
            "ingestion_time": "s.ingestion_time"
        }
      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()

      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.shipments",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.shipments",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise

# COMMAND ----------

# ORDERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/orders"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("CustomerID", "customer_id")
            .withColumnRenamed("EmployeeID", "employee_id")
            .withColumnRenamed("OrderDate", "order_date")
            .withColumnRenamed("RequiredDate", "required_date")
            .withColumnRenamed("ShippedDate", "shipped_date")    
            .withColumnRenamed("ShipVia", "ship_via")
            .withColumnRenamed("Freight", "freight")
            .withColumnRenamed("ShipName", "ship_name")
            .withColumnRenamed("ShipAddress", "ship_address")
            .withColumnRenamed("ShipCity", "ship_city")
            .withColumnRenamed("ShipRegion", "ship_region")
            .withColumnRenamed("ShipPostalCode", "ship_postal_code")
            .withColumnRenamed("ShipCountry", "ship_country"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("order_id"), lit("")),
                    coalesce(col("customer_id"), lit("")),
                    coalesce(col("employee_id"), lit("")),
                    coalesce(col("order_date"), lit("")),
                    coalesce(col("required_date"), lit("")),
                    coalesce(col("shipped_date"), lit("")),
                    coalesce(col("ship_via"), lit("")),
                    coalesce(col("freight"), lit("")),
                    coalesce(col("ship_name"), lit("")),
                    coalesce(col("ship_address"), lit("")),
                    coalesce(col("ship_city"), lit("")),
                    coalesce(col("ship_region"), lit("")),
                    coalesce(col("ship_postal_code"), lit("")),
                    coalesce(col("ship_country"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )
      
# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
      bronze_orders = DeltaTable.forName(
        spark,
        "batch_process.bronze.orders"
      )

      existing = (
        bronze_orders
        .toDF()
        .select(
            "order_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.order_id") == col("b.order_id"),
            "left"
        )
        .filter(
            col("b.order_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )
      
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.orders_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_orders.alias("b")
      .merge(
            changes.alias("s"),
            "b.order_id = s.order_id"
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "customer_id": "s.customer_id",
            "employee_id": "s.employee_id",
            "order_date": "s.order_date",
            "required_date": "s.required_date",
            "shipped_date": "s.shipped_date",
            "ship_via": "s.ship_via",
            "freight": "s.freight",
            "ship_name": "s.ship_name",
            "ship_address": "s.ship_address",
            "ship_city": "s.ship_city",
            "ship_region": "s.ship_region",
            "ship_postal_code": "s.ship_postal_code",
            "ship_country": "s.ship_country",
            "ingestion_time": "s.ingestion_time"
        }
      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()

      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.orders",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.orders",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise

# COMMAND ----------

# ORDER_DETAILS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

start_time = datetime.now()

try:
# ============================================================
# 1. READ SOURCE
# ============================================================
      df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .load("/Volumes/batch_process/default/source/order_details"))
      
# ============================================================
# 2. STANDARDIZE COLUMNS
# ============================================================
      df = (df
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("ProductID", "product_id")
            .withColumnRenamed("UnitPrice", "unit_price")
            .withColumnRenamed("Quantity", "quantity")
            .withColumnRenamed("Discount", "discount")
            .withColumnRenamed("Product Name","product_name"))
      
# ============================================================
# 3. CREATE RECORD HASH
# ============================================================
      df = (
        df
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("unit_price"), lit("")),
                    coalesce(col("quantity"), lit("")),
                    coalesce(col("discount"), lit("")),
                    coalesce(col("product_name"), lit(""))
                ),
                256
            )
        )
        .withColumn("ingestion_time", current_timestamp())
    )
      
# ============================================================
# 4. READ PERMANENT BRONZE TABLE
# ============================================================
      bronze_order_details = DeltaTable.forName(
        spark,
        "batch_process.bronze.order_details"
      )

      existing = (
        bronze_order_details
        .toDF()
        .select(
            "order_id",
            "product_id",
            "record_hash"
        )
      )

# ============================================================
# 5. FIND NEW + CHANGED RECORDS
# ============================================================
      changes = (
        df.alias("s")
        .join(
            existing.alias("b"),
            col("s.order_id") == col("b.order_id"),
            "left"
        )
        .filter(
            col("b.order_id").isNull() |
            col("b.product_id").isNull() |
            (col("s.record_hash") != col("b.record_hash"))
        )
        .select("s.*")
    )
      
# ============================================================
# 6. SAVE ONLY NEW/CHANGED RECORDS
# ============================================================
      changes.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("batch_process.bronze.order_details_changes")

# ============================================================
# 7. MERGE CHANGES INTO PERMANENT BRONZE
# ============================================================
      (
      bronze_order_details.alias("b")
      .merge(
            df.alias("s"),
            """b.order_id = s.order_id AND
            b.product_id = s.product_id"""
      )
      .whenMatchedUpdate(
            condition="""
            b.record_hash <> s.record_hash
        """,
            set={
            "unit_price": "s.unit_price",
            "quantity": "s.quantity",
            "discount": "s.discount",
            "product_name": "s.product_name",
            "ingestion_time": "s.ingestion_time"
        }
      )
      .whenNotMatchedInsertAll()
      .execute()
      )

# ============================================================
# 8. LOG
# ============================================================
      rows_read = df.count()
      rows_changed = changes.count()

      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.order_details",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = rows_read,
            status = "SUCCESS"
      )

except Exception as error:
      etl_log(
            pipeline_name = "BATCH_PROCESS",
            table_name = "batch_process.bronze.order_details",
            layer = "BRONZE",
            start_time = start_time,
            rows_read = 0,
            status = "FAILED",
            error_message = str(error)
      )
      raise
