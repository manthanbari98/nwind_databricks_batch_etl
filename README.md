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
