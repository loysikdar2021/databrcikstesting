-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####1. Create your bronze layer tables ingesting from the landing zone

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dlt_customers_raw
AS SELECT *, current_timestamp() as load_time
FROM cloud_files('/Volumes/ls_dev/ls_demo_db/landing_zone/customers', 
                 "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dlt_invoices_raw
AS SELECT *, current_timestamp() as load_time
FROM cloud_files("/Volumes/ls_dev/ls_demo_db/landing_zone/invoices", 
                 "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Create your silver layer tables reading incremental data from bronze layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dlt_customers_cleaned (
    CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW)
AS 
SELECT CustomerID as customer_id, CustomerName as customer_name, load_time   
FROM STREAM(LIVE.dlt_customers_raw)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dlt_invoices_cleaned (
    CONSTRAINT valid_invoice_and_qty EXPECT (invoice_no IS NOT NULL AND quantity > 0) ON VIOLATION DROP ROW)
    PARTITIONED BY (invoice_year, country)
AS
SELECT InvoiceNo as invoice_no, StockCode as stock_code, Description as description,
        Quantity as quantity, to_date(InvoiceDate, "d-M-y H.m") as invoice_date, 
        UnitPrice as unit_price, CustomerID as customer_id, Country as country,
        year(to_date(InvoiceDate, "d-M-y H.m")) as invoice_year, 
        month(to_date(InvoiceDate, "d-M-y H.m")) as invoice_month,
        load_time
FROM STREAM(LIVE.dlt_invoices_raw)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Build your SCD Type 2 dimensions using CDC from silver layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dlt_customers;

APPLY CHANGES INTO LIVE.dlt_customers
FROM STREAM(LIVE.dlt_customers_cleaned)
KEYS (customer_id)
SEQUENCE BY load_time
STORED AS SCD TYPE 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Merge into your fact table using CDC from the silver layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dlt_invoices PARTITIONED BY (invoice_year, country);

APPLY CHANGES INTO LIVE.dlt_invoices
FROM STREAM(LIVE.dlt_invoices_cleaned)
KEYS (invoice_no, stock_code, invoice_date)
SEQUENCE BY load_time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Materialize your gold layer summary using silver layer fact

-- COMMAND ----------

CREATE LIVE TABLE daily_sales_uk_2022
AS SELECT country, invoice_year, invoice_month, invoice_date,
          round(sum(quantity*unit_price),2) as total_sales
FROM LIVE.dlt_invoices
WHERE invoice_year = 2022 AND country="United Kingdom"
GROUP BY country, invoice_year, invoice_month, invoice_date
