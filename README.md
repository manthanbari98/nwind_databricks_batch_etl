# NWind Databricks Batch ETL

An end-to-end batch data engineering project built using Databricks, PySpark and Delta Lake.

This project processes NWind source data through a Medallion Architecture consisting of Bronze, Silver and Gold layers, with incremental change detection, Delta MERGE operations, business transformations, SCD Type 2 dimensions, dimensional modeling and ETL audit logging.

---

## 📌 Project Overview

The objective of this project is to build a scalable batch ETL pipeline that transforms raw NWind operational data into analytics-ready datasets.

The pipeline follows:

Source Data → Bronze → Silver → Gold → Analytics

The Gold layer contains dimension and fact tables designed for analytical workloads.

---

## 🏗️ Architecture

```text
                NWind Source Data
                       │
                       ▼
              ┌─────────────────┐
              │    Databricks   │
              │  Batch Pipeline  │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │     BRONZE      │
              │                 │
              │ Source Data     │
              │ Record Hashing  │
              │ Change Detection│
              │ Delta MERGE     │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │     SILVER      │
              │                 │
              │ Joins           │
              │ Transformations │
              │ Business Logic  │
              │ Derived Columns  │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │      GOLD       │
              │                 │
              │ SCD Type 2      │
              │ Dimensions      │
              │ Fact Table      │
              │ Star Schema     │
              └────────┬────────┘
                       │
                       ▼
                 Analytics / BI

## 🛠️ Technologies
Databricks
Apache Spark
PySpark
Delta Lake
Python
SQL
Medallion Architecture
Dimensional Modeling
SCD Type 2

## 📂 Project Structure
nwind_databricks_batch_etl/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── notebooks/
│   ├── 01_init_database.py
│   ├── 02_audit_table.py
│   ├── 03_bronze_ddl.py
│   ├── 04_bronze_load.py
│   ├── 05_silver_transformations.py
│   └── 06_gold_layer.py
│
└── docs/
    ├── architecture.md
    └── data_model.md
## 🥉 Bronze Layer

The Bronze layer stores source-level data in Delta tables.

Processing
Read source CSV files from Databricks Volumes.
Standardize source column names.
Generate a record hash using SHA-256.
Read the existing Bronze table.
Compare incoming records with existing records.
Identify new and changed records.
Store detected changes in _changes tables.
MERGE changes into the permanent Bronze Delta table.
Record ETL execution information in the audit table.
Bronze Tables
customers
categories
employees
order_details
orders
products
suppliers
shippers
shipments
## 🥈 Silver Layer

The Silver layer transforms and enriches the Bronze data.

Transformations
Customer transformations
Employee transformations
Product enrichment
Supplier and category joins
Order and order-detail joins
Shipment and shipper joins
Derived columns
Updated timestamps
Business calculations
Order Transformation

Order details are joined with orders and line-level amounts are calculated.

Freight charges are allocated across order lines based on each line's contribution to the total order amount.

Order
  +
Order Details
  │
  ▼
Line Amount
  │
  ▼
Total Order Amount
  │
  ▼
Allocated Freight
## 🥇 Gold Layer

The Gold layer provides analytics-ready dimensional models.

Dimension Tables
dim_customers
dim_products
dim_employees
dim_shipments
Fact Table
fact_orders

The fact table combines Silver order data with surrogate keys from the Gold dimensions.

## 🔄 SCD Type 2

SCD Type 2 is implemented to preserve historical changes in dimension data.

The process:

Identify changed records.
Find the currently active dimension record.
Expire the existing version.
Set is_current = false.
Set effective_to.
Insert the new version.
Set is_current = true.

Example:

customer_id | company_name | effective_from | effective_to | is_current
-----------------------------------------------------------------------
ALFKI       | Old Company  | 2026-01-01     | 2026-06-01   | false
ALFKI       | New Company  | 2026-06-01     | 9999-12-31   | true

This allows historical analysis of dimension changes.

## ⭐ Dimensional Model
                    dim_customers
                         │
                         │
                         ▼
dim_employees ───── fact_orders ───── dim_products
                         │
                         │
                         ▼
                   dim_shipments
## 📊 Fact Table

The fact_orders table contains order-level and order-line analytical information.

It uses surrogate keys from the Gold dimensions, including:

customer_sk
employee_sk
product_sk
shipment_sk

along with order and transaction attributes.

## 📝 ETL Audit Logging

The project includes an ETL audit logging mechanism.

The audit process records:

Run ID
Pipeline name
Table name
Processing layer
Start time
End time
Duration
Rows read
Rows inserted
Rows updated
Rows deleted
Status
Error message

Audit records are stored in:

batch_process.audit.etl_log
## ▶️ Execution Order

Run the notebooks in the following order:

01_init_database.py
        │
        ▼
02_audit_table.py
        │
        ▼
03_bronze_ddl.py
        │
        ▼
04_bronze_load.py
        │
        ▼
05_silver_transformations.py
        │
        ▼
06_gold_layer.py
## 🔑 Key Features
Batch ETL processing
Databricks
PySpark
Delta Lake
Medallion Architecture
Incremental change detection
SHA-256 record hashing
Delta MERGE
Silver-layer transformations
Business calculations
Freight allocation
SCD Type 2
Surrogate keys
Star schema
Fact and dimension modeling
ETL audit logging
Error handling
## 📁 Source Data

The project uses NWind sample data.

Source CSV files are not included in this repository.

The notebooks expect the source data to be available in the configured Databricks Volume paths.

Update the source paths according to your Databricks environment before running the pipeline.

## 🚀 How to Run
1. Open Databricks

Import the notebooks from the notebooks/ directory into your Databricks workspace.

2. Configure Source Data

Place the NWind source CSV files in the appropriate Databricks Volume location.

3. Run the Notebooks

Execute the notebooks in the documented order:

Database Initialization
        ↓
Audit Table Creation
        ↓
Bronze Table Creation
        ↓
Bronze Batch Load
        ↓
Silver Transformations
        ↓
Gold Layer
4. Validate Gold Tables

After successful execution, validate the Gold dimensions and fact table.

## 🎯 Project Outcome

The pipeline transforms raw NWind operational data into structured analytical datasets using a layered Lakehouse architecture.

The final Gold layer provides analytics-ready dimension and fact tables that can be consumed by reporting and business intelligence tools.

## 👨‍💻 Author

Manthan Bari

Data Engineering Portfolio Project



---


## 6. One thing I would NOT put in the README


Don't write things like:


> "I learned PySpark from this project."


or


> "This was my practice project."


Even if that's true.


For GitHub/interview purposes, present it professionally:


> **"An end-to-end batch data engineering pipeline built using Databricks, PySpark and Delta Lake."**


That's a much stronger presentation.


---


## 7. After README, we'll add the two docs


Then your final repository will look like:


```text
nwind_databricks_batch_etl/
│
├── README.md                  ⭐ Project overview
├── LICENSE
├── .gitignore
│
├── notebooks/
│   ├── 01_init_database.py
│   ├── 02_audit_table.py
│   ├── 03_bronze_ddl.py
│   ├── 04_bronze_load.py
│   ├── 05_silver_transformations.py
│   └── 06_gold_layer.py
│
└── docs/
    ├── architecture.md       📐 Pipeline explanation
    └── data_model.md         ⭐ Tables + relationships
