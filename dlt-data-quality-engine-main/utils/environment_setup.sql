-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "rimprota_demo")
-- MAGIC dbutils.widgets.text("schema", "dlt_data_quality_lib")
-- MAGIC dbutils.widgets.text("expectations_table", "data_quality_expectations")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema = dbutils.widgets.get("schema")
-- MAGIC expectations_table = dbutils.widgets.get("expectations_table")

-- COMMAND ----------

-- DBTITLE 1,Criando database para testes
CREATE DATABASE IF NOT EXISTS ${catalog}.${schema};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplos de regras
-- MAGIC Abaixo apenas alguns exemplos de regras, mas para gerar essa tabela de forma mais performática podemos carregar estes dados a partir de um arquivo ao invés de chamar essa função executando vários inserts pontuais

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${schema}.${expectations_table} (
  catalog STRING COMMENT "Name of the catalog where the table/column is present",
  schema STRING COMMENT "Name of the schema where the table/column is present",
  table STRING COMMENT "Name of the table where the column is present",
  expectation_name STRING COMMENT "Expectation name, which will appear on DLT UI and Event Log",
  expectation_definition STRING COMMENT "The SQL expression that will be executed to validate the column",
  expectation_action STRING COMMENT "The action that will be taken if the expectation fails (e.g., 'expect', 'expect_or_fail', 'expect_or_drop')",
  creation_timestamp TIMESTAMP COMMENT "Timestamp when the expectation rule was created"
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def registerExpectation(source_catalog, source_schema, source_table, expectation, rule, expectation_type):
-- MAGIC     """
-- MAGIC     Registers a data quality expectation in the specified expectations table.
-- MAGIC
-- MAGIC     Parameters:
-- MAGIC     - source_catalog: The catalog where the source table is located.
-- MAGIC     - source_schema: The schema where the source table is located.
-- MAGIC     - source_table: The name of the source table.
-- MAGIC     - expectation: A description of the data quality expectation.
-- MAGIC     - rule: The specific rule or condition that defines the expectation.
-- MAGIC     - expectation_type: The type of expectation (e.g., 'expect', 'expect_or_fail', 'expect_or_drop').
-- MAGIC     """
-- MAGIC     spark.sql(f"""
-- MAGIC         INSERT INTO {catalog}.{schema}.{expectations_table} 
-- MAGIC         VALUES ('{source_catalog}', '{source_schema}', '{source_table}', '{expectation}', "{rule}", '{expectation_type}', CURRENT_TIMESTAMP())
-- MAGIC     """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Ensure pickup and dropoff timestamps are not null
-- MAGIC registerExpectation(
-- MAGIC     source_catalog='rimprota_demo',
-- MAGIC     source_schema='dlt_data_quality_lib',
-- MAGIC     source_table='silver_trips',
-- MAGIC     expectation='tpep_pickup_datetime IS NOT NULL',
-- MAGIC     rule="tpep_pickup_datetime IS NOT NULL",
-- MAGIC     expectation_type='expect'
-- MAGIC )
-- MAGIC
-- MAGIC registerExpectation(
-- MAGIC     source_catalog='rimprota_demo',
-- MAGIC     source_schema='dlt_data_quality_lib',
-- MAGIC     source_table='silver_trips',
-- MAGIC     expectation='tpep_dropoff_datetime IS NOT NULL',
-- MAGIC     rule="tpep_dropoff_datetime IS NOT NULL",
-- MAGIC     expectation_type='expect'
-- MAGIC )
-- MAGIC
-- MAGIC # Suspect long trips with a low fare amount
-- MAGIC registerExpectation(
-- MAGIC     source_catalog='rimprota_demo',
-- MAGIC     source_schema='dlt_data_quality_lib',
-- MAGIC     source_table='silver_trips',
-- MAGIC     expectation='Suspect Long Trip',
-- MAGIC     rule="NOT (trip_distance > 20 AND fare_amount < 5)",
-- MAGIC     expectation_type='expect'
-- MAGIC )
-- MAGIC
-- MAGIC # Ensure trip distance and fare amount are greater than 0
-- MAGIC registerExpectation(
-- MAGIC     source_catalog='rimprota_demo',
-- MAGIC     source_schema='dlt_data_quality_lib',
-- MAGIC     source_table='silver_trips',
-- MAGIC     expectation='trip_distance > 0',
-- MAGIC     rule="trip_distance > 0",
-- MAGIC     expectation_type='expect_or_drop'
-- MAGIC )
-- MAGIC
-- MAGIC registerExpectation(
-- MAGIC     source_catalog='rimprota_demo',
-- MAGIC     source_schema='dlt_data_quality_lib',
-- MAGIC     source_table='silver_trips',
-- MAGIC     expectation='fare_amount > 0',
-- MAGIC     rule="fare_amount > 0",
-- MAGIC     expectation_type='expect_or_drop'
-- MAGIC )
-- MAGIC
-- MAGIC # Ensure pickup and dropoff zip codes are not null
-- MAGIC registerExpectation(
-- MAGIC     source_catalog='rimprota_demo',
-- MAGIC     source_schema='dlt_data_quality_lib',
-- MAGIC     source_table='silver_trips',
-- MAGIC     expectation='pickup_zip IS NOT NULL',
-- MAGIC     rule="pickup_zip IS NOT NULL",
-- MAGIC     expectation_type='expect_or_fail'
-- MAGIC )
-- MAGIC
-- MAGIC registerExpectation(
-- MAGIC     source_catalog='rimprota_demo',
-- MAGIC     source_schema='dlt_data_quality_lib',
-- MAGIC     source_table='silver_trips',
-- MAGIC     expectation='dropoff_zip IS NOT NULL',
-- MAGIC     rule="dropoff_zip IS NOT NULL",
-- MAGIC     expectation_type='expect_or_fail'
-- MAGIC )

-- COMMAND ----------

SELECT * FROM ${catalog}.${schema}.${expectations_table}
