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
```
## 🛠️ Technologies
```
> Databricks
> Apache Spark
> PySpark
> Delta Lake
> Python
> SQL
> Medallion Architecture
> Dimensional Modeling
> SCD Type 2
```

## ⭐ Dimensional Model
```
                    dim_customers
                         │
                         │
                         ▼
dim_employees ───── fact_orders ───── dim_products
                         │
                         │
                         ▼
                   dim_shipments
```

## ▶️ Execution Order
```
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
```

## 🔑 Key Features

Batch ETL processing |
Databricks |
PySpark |
Delta Lake |
Medallion Architecture |
Incremental change detection |
SHA-256 record hashing |
Delta MERGE |
Silver-layer transformations |
Business calculations |
Freight allocation |
SCD Type 2 |
Surrogate keys |
Star schema |
Fact and dimension modeling |
ETL audit logging |
Error handling |

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


