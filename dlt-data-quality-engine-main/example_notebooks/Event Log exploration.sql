-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##What is the Delta Live Tables event log?
-- MAGIC The Delta Live Tables event log contains all information related to a pipeline, including audit logs, data quality checks, pipeline progress, and data lineage. You can use the event log to track, understand, and monitor the state of your data pipelines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##How can I view the event log?
-- MAGIC You can view event log entries in the Delta Live Tables user interface, the Delta Live Tables API, or by directly querying the event log

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Event Log Schema
-- MAGIC
-- MAGIC * id - A unique identifier for the event log record. 
-- MAGIC * sequence - A JSON document containing metadata to identify and order events
-- MAGIC * origin - A JSON document containing metadata for the origin of the event
-- MAGIC * timestamp - The time the event was recorded
-- MAGIC * message - A human-readable message describing the event
-- MAGIC * level - The event type, for example, INFO, WARN, ERROR, or METRICS
-- MAGIC * error - If an error occurred, details describing the error
-- MAGIC * details - A JSON document containing structured details of the event
-- MAGIC * event_type - The event type
-- MAGIC * maturity_level - The stability of the event schema
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If your pipeline publishes tables to Unity Catalog, you must use the event_log table valued function (TVF) to fetch the event log for the pipeline. You retrieve the event log for a pipeline by passing the pipeline ID or a table name to the TVF.
-- MAGIC
-- MAGIC ```Select * from event_log( { TABLE() | pipeline_id } )```

-- COMMAND ----------

SELECT * FROM event_log("3110e1c6-28fd-4d07-9e67-7960f7352926") -- id do pipeline DLT
LIMIT 20

-- COMMAND ----------

SELECT * FROM event_log(table(my_catalog.my_schema.my_dlt_table))
LIMIT 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To simplify querying events for a pipeline, the owner of the pipeline can create a view over the event_log TVF

-- COMMAND ----------

CREATE OR REPLACE VIEW my_catalog.my_schema.dlt_event_log_raw AS SELECT * FROM event_log("3110e1c6-28fd-4d07-9e67-7960f7352926");

-- COMMAND ----------

-- Temp View com id apenas da ultima execução
CREATE OR REPLACE TEMP VIEW latest_update AS
SELECT
  origin.update_id AS id
FROM my_catalog.my_schema.dlt_event_log_raw
WHERE event_type = 'create_update'
ORDER BY timestamp DESC LIMIT 1;

-- COMMAND ----------

SELECT * FROM latest_update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Query lineage information from the event log
-- MAGIC Events containing information about lineage have the event type flow_definition. The details:flow_definition object contains the output_dataset and input_datasets defining each relationship in the graph.

-- COMMAND ----------

SELECT
  details:flow_definition.output_dataset as output_dataset,
  details:flow_definition.input_datasets as input_dataset
FROM
  my_catalog.my_schema.dlt_event_log_raw,
  latest_update
WHERE event_type = 'flow_definition'
  AND origin.update_id = latest_update.id


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Query data quality from the event log
-- MAGIC If you define expectations on datasets in your pipeline, the data quality metrics are stored in the details:flow_progress.data_quality.expectations object. Events containing information about data quality have the event type flow_progress

-- COMMAND ----------

{
  "dropped_records":791,
  "expectations":[
      {"name":"Tamanho da tag deve ser maior que 3 caracteres","dataset":"cajamarquilla_streaming_bronze_dlt","passed_records":17328804,"failed_records":0},
      {"name":"Tag deve estar dentro do domínio esperado","dataset":"cajamarquilla_streaming_bronze_dlt","passed_records":17328013,"failed_records":791},
      {"name":"Tag nao pode ser nula nem vazia","dataset":"cajamarquilla_streaming_bronze_dlt","passed_records":17328804,"failed_records":0},
      {"name":"Valores devem estar dentro do range esperado","dataset":"cajamarquilla_streaming_bronze_dlt","passed_records":2346185,"failed_records":14982619}
    ]
}

-- COMMAND ----------

CREATE OR REPLACE VIEW my_catalog.my_schema.dlt_event_log_data_quality_agg AS
SELECT
   timestamp
  ,pipeline_name
  ,expectations.dataset as dataset
  ,expectations.name as expectation
  ,SUM(expectations.passed_records) as passing_records
  ,SUM(expectations.failed_records) as failing_records
FROM (
  SELECT
     from_utc_timestamp(timestamp, 'America/Sao_Paulo') as timestamp
    ,origin.pipeline_name AS pipeline_name
    ,explode(from_json(
        details:flow_progress:data_quality:expectations,
        "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
    )) AS expectations
  FROM
    my_catalog.my_schema.dlt_event_log_raw
  WHERE event_type = 'flow_progress'
) sq1
WHERE expectations IS NOT NULL
GROUP BY ALL

-- COMMAND ----------

SELECT *
FROM my_catalog.my_schema.dlt_event_log_data_quality_agg
ORDER BY timestamp, dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Audit Delta Log Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query user actions in the event log
-- MAGIC You can use the event log to audit events, for example, user actions. Events containing information about user actions have the event type user_action.
-- MAGIC
-- MAGIC Information about the action is stored in the user_action object in the details field.

-- COMMAND ----------

SELECT
  from_utc_timestamp(timestamp, 'America/Sao_Paulo') as timestamp,
  details:user_action:action,
  details:user_action:user_name
FROM my_catalog.my_schema.dlt_event_log_raw
WHERE event_type = 'user_action'
