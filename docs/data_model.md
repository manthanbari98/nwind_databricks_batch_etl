# Data Model

## Overview

The Gold layer follows a dimensional modeling approach with a central fact table surrounded by dimension tables.

The model contains:

- `fact_orders`
- `dim_customers`
- `dim_products`
- `dim_employees`
- `dim_shipments`

The fact table uses surrogate keys from the dimension tables to support analytical queries.

---


## Gold Data Model


```text
                         dim_customers
                              │
                              │ customer_sk
                              │
          employees_sk        ▼    product_sk
dim_employees ─────────> fact_orders <───────── dim_products
                              │
                              │ shipment_sk
                              ▼
                        dim_shipments
```
## Dimension Tables
### 1. dim_customers

Customer dimension containing customer attributes and historical versions.

### Key
customer_sk

Surrogate key generated using an identity column.

### Business Key
customer_id

### Important Columns

| Column         | Description                         |
| -------------- | ----------------------------------- |
| customer_sk    | Surrogate key                       |
| customer_id    | Business/customer identifier        |
| company_name   | Customer company                    |
| contact_name   | Customer contact                    |
| contact_title  | Contact title                       |
| cust_address   | Customer address                    |
| city           | Customer city                       |
| region         | Customer region                     |
| postal_code    | Postal code                         |
| country        | Customer country                    |
| phone          | Customer phone                      |
| fax            | Customer fax                        |
| record_hash    | Record change detection hash        |
| ingestion_time | Source ingestion timestamp          |
| updated_time   | Silver update timestamp             |
| effective_from | Start of dimension version          |
| effective_to   | End of dimension version            |
| is_current     | Indicates current dimension version |


### SCD Type 2

dim_customers maintains historical versions of customer records.
```
customer_id
     │
     ├── Version 1 → is_current = false
     │
     └── Version 2 → is_current = true
```

### 2. dim_products

Product dimension containing product attributes.

### Key
product_sk
### Business Key
product_id

The product dimension is used by fact_orders through product_sk.

### 3. dim_employees

Employee dimension containing employee information and historical versions.

### Key
employee_sk
### Business Key
### Important Columns
| Column         | Description                  |
| -------------- | ---------------------------- |
| employee_sk    | Surrogate key                |
| employee_id    | Employee identifier          |
| employee_name  | Employee name                |
| title          | Employee title               |
| city           | Employee city                |
| country        | Employee country             |
| reportsTo      | Reporting employee           |
| updated_time   | Silver update timestamp      |
| effective_from | Start of dimension version   |
| effective_to   | End of dimension version     |
| record_hash    | Record change detection hash |
| is_current     | Current version indicator    |

dim_employees uses SCD Type 2 processing to preserve historical employee changes.

### 4. dim_shipments

Shipment dimension containing shipment and shipper information.

### Key
shipment_sk
### Business Relationship

Shipments are associated with orders using:

order_id

The shipment surrogate key is then used by the fact table.

## Fact Table
### fact_orders

The fact_orders table is the central analytical table in the Gold layer.

It combines Silver order data with the current Gold dimension records.

### Columns
| Column          | Description                      |
| --------------- | -------------------------------- |
| order_id        | Order identifier                 |
| customer_sk     | Customer dimension surrogate key |
| employee_sk     | Employee dimension surrogate key |
| product_sk      | Product dimension surrogate key  |
| shipment_sk     | Shipment dimension surrogate key |
| quantity        | Ordered quantity                 |
| unit_price      | Unit price                       |
| discount        | Order line discount              |
| freight_charges | Allocated freight charges        |
| order_date      | Order date                       |

## Relationships
### Customer Relationship
```
fact_orders.customer_sk
        │
        ▼
dim_customers.customer_sk
```

The fact table uses the customer surrogate key to connect orders with customer attributes.

### Employee Relationship
```
fact_orders.employee_sk
        │
        ▼
dim_employees.employee_sk
```

The fact table uses the employee surrogate key to connect orders with employee information.

### Product Relationship
fact_orders.product_sk
        │
        ▼
dim_products.product_sk
```

The fact table uses the product surrogate key to connect order lines with product information.

### Shipment Relationship
```
fact_orders.shipment_sk
        │
        ▼
dim_shipments.shipment_sk
```

The shipment dimension is associated with the order using order_id, and the resulting shipment surrogate key is stored in the fact table.
```
## Source-to-Gold Flow
```
Bronze
  │
  ├── customers
  ├── categories
  ├── employees
  ├── products
  ├── suppliers
  ├── orders
  ├── order_details
  ├── shipments
  └── shippers
          │
          ▼
Silver
  │
  ├── customers
  ├── employees
  ├── products
  ├── orders
  └── shipments
          │
          ▼
Gold
  │
  ├── dim_customers
  ├── dim_products
  ├── dim_employees
  ├── dim_shipments
  │
  └── fact_orders
```
## Fact Table Grain

The fact_orders table is built from the Silver order dataset.

## SCD Type 2 Design

The SCD Type 2 dimensions maintain historical versions using:
```
effective_from
effective_to
is_current
```
### When an existing dimension record changes:
```
Current Record
     │
     ▼
Detect Change
     │
     ▼
Expire Old Version
     │
     ▼
Insert New Version
     │
     ▼
New Record = Current
```
This allows historical analysis without losing previous dimension values.

## Analytical Model

The resulting Gold layer provides an analytics-ready star-schema structure.
```
                    ┌──────────────────┐
                    │  dim_customers   │
                    └────────┬─────────┘
                             │
                             │
┌──────────────────┐         ▼         ┌──────────────────┐
│  dim_employees   │──── fact_orders ──│   dim_products   │
└──────────────────┘         │         └──────────────────┘
                             │
                             │
                             ▼
                    ┌──────────────────┐
                    │  dim_shipments   │
                    └──────────────────┘
```
This model separates transactional measures from descriptive dimension attributes and is designed for downstream analytical and BI workloads.
