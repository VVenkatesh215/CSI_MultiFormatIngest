
# Customer Data Ingestion and Delta Lake Management on Azure & Databricks

## Project Overview

This project demonstrates the process of ingesting customer data in various file formats into Azure Data Lake Storage Gen2 (ADLS Gen2), connecting it with Databricks, converting each format into a managed Delta table using CTAS (Create Table As Select), and validating the data using a generic Python function.

The goal was to standardize data from multiple file formats (CSV, TSV, JSON, XML, XLSX, TXT, AVRO, PARQUET, ORC, DELTA) and store it as optimized Delta tables for further analytics and consistency validation.

---

## Steps to Perform

### 1. Load sample customers data into ADLS Gen2

Use a sample customer dataset and store it in 10 different file formats (CSV, TSV, JSON, XML, XLSX, TXT, AVRO, PARQUET, ORC, DELTA).  
Used a sample customer dataset and converted it into 10 formats. Uploaded all files to a container `inputcontainer` in ADLS Gen2.

**Example CSV Schema:**

- CustomerID  
- FirstName  
- LastName  
- Email  
- PhoneNumber  
- Country  

![](<Load Sample Data into ADLS Gen2.png>)
---

### 2. Make a connection between ADLS Gen2 and Databricks

Mount the storage account container to Databricks using access keys.

```python
configs = {
  "fs.azure.account.key.csicustomers.dfs.core.windows.net": "<your-storage-account-key>"
}

dbutils.fs.mount(
  source = "wasbs://inputcontainer@csicustomers.blob.core.windows.net/",
  mount_point = "/mnt/input",
  extra_configs = configs)
```

```python
display(dbutils.fs.ls("/mnt/input"))
```
![](<Mount ADLS Gen2 into Databricks.png>)
---

### 3. Create a managed Delta table (not an external table) and load data into it

For each format, convert the source data to a Delta table using CTAS (Create Table As Select).

```python
df_csv = spark.read.option("header", True).csv("/mnt/input/customers.csv")
df_csv.createOrReplaceTempView("temp_csv_customers")
```


```sql
%sql
CREATE OR REPLACE TABLE delta_csv_customers
USING DELTA
AS SELECT * FROM temp_csv_customers;
```
![](<Create Managed Delta Table.png>)
---

### 4. Use CTAS for file formats like AVRO, PARQUET, ORC & DELTA

These can be read directly without creating a temporary view.

```python
df = spark.read.option("header", True).csv("/mnt/input/customers.csv")

df.write.format("avro").save("/mnt/input/customers_avro")
df.write.parquet("/mnt/input/customers_parquet")
df.write.format("orc").save("/mnt/input/customers_orc")
df.write.format("delta").save("/mnt/input/customers_delta")
```

---

### 5. For CSV, TSV, JSON, XML, XLSX, TXT:  
First, create a temporary view using the customer schema. Then use CTAS to convert into a Delta table.

#### AVRO

```python
df_avro = spark.read.format("avro").load("/mnt/input/customers_avro")
df_avro.createOrReplaceTempView("temp_avro_customers")
```

```sql
%sql
CREATE OR REPLACE TABLE delta_avro
USING DELTA
AS SELECT * FROM temp_avro_customers;
```

#### PARQUET

```python
df_parquet = spark.read.parquet("/mnt/input/customers_parquet")
df_parquet.createOrReplaceTempView("temp_parquet_customers")
```

```sql
%sql
CREATE OR REPLACE TABLE delta_parquet
USING DELTA
AS SELECT * FROM temp_parquet_customers;
```

#### ORC

```python
df_orc = spark.read.format("orc").load("/mnt/input/customers_orc")
df_orc.createOrReplaceTempView("temp_orc_customers")
```

```sql
%sql
CREATE OR REPLACE TABLE delta_orc
USING DELTA
AS SELECT * FROM temp_orc_customers;
```

#### DELTA

```python
df_delta = spark.read.format("delta").load("/mnt/input/customers_delta")
df_delta.createOrReplaceTempView("temp_delta_customers")
```

```sql
%sql
CREATE OR REPLACE TABLE delta_delta
USING DELTA
AS SELECT * FROM temp_delta_customers;
```

---

### 6. Data Validation

Write a generic Python function that verifies every Delta table has exactly 500 rows to ensure consistency and completeness across all formats.

```python
def validate_row_count(table_name: str, expected_count: int = 500):
    try:
        actual_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        if actual_count == expected_count:
            print(f"{table_name}: {actual_count} rows")
        else:
            print(f"{table_name}: {actual_count} rows (Expected: {expected_count})")
    except Exception as e:
        print(f"Failed to validate {table_name}: {e}")
```

```python
tables = [
    "delta_csv_customers", "delta_tsv", "delta_json", "delta_xml",
    "delta_xlsx", "delta_txt", "delta_avro", "delta_parquet",
    "delta_orc", "delta_delta"
]

for table in tables:
    validate_row_count(table)
```

![](<Data Validation.png>)
---

## Technologies Used

- Azure Storage Account (ADLS Gen2)
- Azure Databricks
- Apache Spark (via Databricks Notebooks)
- PySpark and Spark SQL
- Delta Lake
- pandas and openpyxl for XLSX processing
- spark-xml for XML file support

---

## Conclusion

This project demonstrates a complete end-to-end data engineering workflow using Azure and Databricks. Starting from ingesting customer data in 10 different formats into Azure Data Lake Storage Gen2, each file was processed using PySpark and Spark SQL in Databricks and converted into optimized managed Delta tables using the CTAS (Create Table As Select) approach.

A reusable validation function was created to ensure data consistency across all Delta tables, verifying that each table contained exactly 500 records. The process covered various data formats including structured (CSV, TSV, XLSX), semi-structured (JSON, XML), and binary columnar formats (AVRO, PARQUET, ORC, DELTA).

This project highlights practical use cases in data ingestion, transformation, storage optimization, and validation essential tasks in real world data engineering pipelines. The modular structure, reusable code, and compatibility with cloud native services like Azure make this approach scalable and production ready.
