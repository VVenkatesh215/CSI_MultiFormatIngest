# Databricks notebook source
configs = {
  "fs.azure.account.key.csicustomers.blob.core.windows.net": "<Replace with your access key>"
}

dbutils.fs.mount(
  source = "wasbs://inputcontainer@csicustomers.blob.core.windows.net/",
  mount_point = "/mnt/input",
  extra_configs = configs
)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/input"))


# COMMAND ----------

df = spark.read.option("header", True).csv("/mnt/input/customers.csv")
df.show()


# COMMAND ----------

# Example for CSV
df_csv = spark.read.option("header", True).csv("/mnt/input/customers.csv")
df_csv.createOrReplaceTempView("temp_csv_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE delta_csv_customers
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_csv_customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_csv_customers LIMIT 5;
# MAGIC

# COMMAND ----------

CREATE TABLE delta_csv_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM delta_csv_customers;
# MAGIC

# COMMAND ----------

df = spark.read.option("header", True).csv("/mnt/input/customers.csv")


# COMMAND ----------

# Convert to AVRO
df.write.format("avro").mode("overwrite").save("/mnt/input/customers_avro")

# COMMAND ----------

# Convert to PARQUET
df.write.parquet("/mnt/input/customers_parquet")

# COMMAND ----------

# Convert to ORC
df.write.format("orc").mode("overwrite").save("/mnt/input/customers_orc")

# COMMAND ----------

# Convert to DELTA
df.write.format("delta").mode("overwrite").save("/mnt/input/customers_delta")


# COMMAND ----------

df_avro = spark.read.format("avro").load("/mnt/input/customers_avro")
df_avro.createOrReplaceTempView("temp_avro_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_avro
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_avro_customers;
# MAGIC

# COMMAND ----------

# Read Parquet file
df_parquet = spark.read.parquet("/mnt/input/customers_parquet")

# Create Temp View
df_parquet.createOrReplaceTempView("temp_parquet_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_parquet
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_parquet_customers;
# MAGIC

# COMMAND ----------

# Read ORC file
df_orc = spark.read.format("orc").load("/mnt/input/customers_orc")

# Create Temp View
df_orc.createOrReplaceTempView("temp_orc_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_orc
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_orc_customers;
# MAGIC

# COMMAND ----------

# Read Delta folder
df_delta = spark.read.format("delta").load("/mnt/input/customers_delta")

# Create a Temp View
df_delta.createOrReplaceTempView("temp_delta_customers")


# COMMAND ----------

# MAGIC %sql CREATE OR REPLACE TABLE delta_delta
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_delta_customers;
# MAGIC

# COMMAND ----------

def validate_row_count(table_name):
    count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    if count == 500:
        print(f"{table_name}: 500 rows")
    else:
        print(f"{table_name}: {count} rows")

tables = ["delta_avro", "delta_parquet", "delta_orc", "delta_delta"]
for tbl in tables:
    validate_row_count(tbl)


# COMMAND ----------

df_tsv = spark.read.option("header", True).option("sep", "\t").csv("/mnt/input/customers.tsv")
df_tsv.createOrReplaceTempView("temp_tsv_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_tsv
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_tsv_customers;
# MAGIC

# COMMAND ----------

df_json = spark.read.option("multiline", True).json("/mnt/input/customers.json")
df_json.createOrReplaceTempView("temp_json_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_json
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_json_customers;
# MAGIC

# COMMAND ----------

df_txt = spark.read.option("header", True).option("delimiter", ",").csv("/mnt/input/customers.txt")
df_txt.createOrReplaceTempView("temp_txt_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_txt
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_txt_customers;
# MAGIC

# COMMAND ----------

# MAGIC %pip install pandas openpyxl
# MAGIC

# COMMAND ----------

import pandas as pd

pdf = pd.read_excel("/dbfs/mnt/input/customers.xlsx")
df_xlsx = spark.createDataFrame(pdf)
df_xlsx.createOrReplaceTempView("temp_xlsx_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_xlsx
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_xlsx_customers;
# MAGIC

# COMMAND ----------

df_xml = spark.read.format("xml") \
    .option("rowTag", "Customer") \
    .load("/mnt/input/customers.xml")

df_xml.createOrReplaceTempView("temp_xml_customers")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_xml
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM temp_xml_customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;
# MAGIC

# COMMAND ----------

# MAGIC %sql SELECT * FROM delta_json LIMIT 5;
# MAGIC

# COMMAND ----------

def validate_row_count(table_name: str, expected_count: int = 500):
    """
    Validates whether a given Delta table has the expected number of rows.
    
    Args:
        table_name (str): The name of the Delta table to validate.
        expected_count (int): The expected number of rows (default is 500).
    
    Prints:
        table_name: expected_count rows
        table_name: actual_count rows (Expected: expected_count)
    """
    try:
        actual_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        if actual_count == expected_count:
            print(f"{table_name}: {actual_count} rows")
        else:
            print(f"{table_name}: {actual_count} rows (Expected: {expected_count})")
    except Exception as e:
        print(f"Failed to validate {table_name}: {e}")


# COMMAND ----------

tables = [
    "delta_csv_customers", "delta_tsv", "delta_json", "delta_xml",
    "delta_xlsx", "delta_txt", "delta_avro", "delta_parquet",
    "delta_orc", "delta_delta"
]

for table in tables:
    validate_row_count(table)


# COMMAND ----------

# Example: AVRO (rename directory to distinguish)
dbutils.fs.cp(
    "/mnt/input/customers_avro",
    "/mnt/input/final/customers_avro",
    recurse=True
)

dbutils.fs.cp(
    "/mnt/input/customers_parquet",
    "/mnt/input/final/customers_parquet",
    recurse=True
)

dbutils.fs.cp(
    "/mnt/input/customers_orc",
    "/mnt/input/final/customers_orc",
    recurse=True
)

dbutils.fs.cp(
    "/mnt/input/customers_delta",
    "/mnt/input/final/customers_delta",
    recurse=True
)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/input"))
