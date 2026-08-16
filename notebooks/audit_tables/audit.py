#CREATE AUDIT SCHEMA AND TABLE

#Audit Schema
spark.sql("""
          CREATE SCHEMA IF NOT EXISTS batch_process.audit;
          """)

# COMMAND ----------

#Audit Table
spark.sql("""
CREATE TABLE IF NOT EXISTS batch_process.audit.etl_log
(
    log_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    run_id STRING,
    pipeline_name STRING,
    table_name STRING,
    layer STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DOUBLE,
    rows_read INT,
    rows_inserted INT,
    rows_updated INT,
    rows_deleted INT,
    status STRING,
    error_message STRING
)
""")
