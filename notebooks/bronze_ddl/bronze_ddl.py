# Databricks notebook source
# SCRIPT PURPOSE : Create bronze DDL

#========================================
#categories
#========================================
from pyspark.sql.functions import *

spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.categories (
                category_id                     INT,
                category_name                   STRING,
                description                     STRING,
                picture				      STRING,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
                        """)

#========================================
#customers
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.customers (
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
                ingestion_time                  TIMESTAMP)
                """)

#========================================
#employees
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.employees (
                employee_id				INT,
                employee_name				STRING,
                title				      STRING,
                city				      STRING,
                country					STRING,
                reportsTo				INT,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
                """)

#========================================
#order_details
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.order_details (
                order_id				INT,
                product_id				INT,
                unit_price			      DOUBLE,
                quantity				INT,
                discount				DOUBLE,
                product_name			      STRING,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
                """)

#========================================
#orders
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.orders (
                order_id			      INT,
                customer_id			      STRING,
                employee_id			      INT,
                order_date			      STRING,
                required_date			      STRING,
                shipped_date		            STRING,
                ship_via			      INT,
                freight				      DOUBLE,
                ship_name			      STRING,
                ship_address		            STRING,
                ship_city			      STRING,
                ship_region			      STRING,
                ship_postal_code	            STRING,
                ship_country		            STRING,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
                """)

#========================================
#products
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.products (
                product_id			      INT,
                product_name		            STRING,
                supplier_id			      INT,
                category_id			      INT,
                quantity_per_unit		      STRING,
                unit_price			      DOUBLE,
                units_in_stock		      INT,
                units_on_order		      INT,
                reorder_level		            INT,
                discontinued		            BOOLEAN,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
                """)

#========================================
#suppliers
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.suppliers (
                supplier_id			      INT,
                company_name		            STRING,
                contact_name		            STRING,
                contact_title		            STRING,
                address	                        STRING,
                city				      STRING,
                region				      STRING,
                postal_code			      STRING,
                country				      STRING,
                phone				      STRING,
                fax					STRING,
                home_page			      STRING,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
                """)

#========================================
#shippers
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.shippers (
                shipper_id			      INT,
                company_name		            STRING,
                phone				      STRING,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
                """)

#========================================
#shipments
#========================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS batch_process.bronze.shipments (
                order_id			      INT,
                customer_id			      STRING,
                employee_id			      INT,
                order_date			      STRING,
                required_date		            STRING,
                shipped_date		            STRING,
                ship_via			      INT,
                freight				      DOUBLE,
                ship_name			      STRING,
                ship_address		            STRING,
                ship_city			      STRING,
                ship_region			      STRING,
                ship_postal_code	            STRING,
                ship_country		            STRING,
                record_hash				STRING,
                ingestion_time                  TIMESTAMP)
          """)
