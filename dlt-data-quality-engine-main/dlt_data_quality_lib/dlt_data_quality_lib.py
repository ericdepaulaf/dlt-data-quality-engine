import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from functools import wraps
from collections import namedtuple

# Define a named tuple for data quality rules
DqRule = namedtuple("dq_rule", ["func", "name", "filter"])
dq_rules = {}

# Function to get the appropriate DLT expectation function
def get_func(func: str):
  if func.strip().lower() == 'expect':
    return dlt.expect
  if func.strip().lower() == 'expect_or_drop':
    return dlt.expect_or_drop
  if func.strip().lower() == 'expect_or_fail':
    return dlt.expect_or_fail
  else:
    return None

# Load data quality rules from a specified table
def load_data_quality_rules(spark: SparkSession, catalog: str, schema: str):
    expectations_table: str = "rimprota_demo.dlt_data_quality_lib.data_quality_expectations"
    
    # Query the expectations table and transform the data
    df = (spark.table(expectations_table)
          .filter(col('catalog') == catalog)
          .filter(col('schema') == schema)
          .select(col('table').alias('table'),
                  col('expectation_action').alias('func'),
                  col('expectation_name').alias('name'),
                  col('expectation_definition').alias('filter'))
          .withColumn('dqRule', struct('func', 'name', 'filter'))
          .groupBy('table')
          .agg(collect_set('dqRule').alias('dqRules'))
    )
    # Populate the dq_rules dictionary
    for row in df.collect():
      dq_table = row[0]
      rules_array = []
      for rule in row[1]:
        dq_rule = DqRule(func=get_func(rule["func"]), name=rule["name"], filter=rule["filter"])
        rules_array.append(dq_rule)
      dq_rules[dq_table] = rules_array

# Filter data that doesn't meet quality expectations
def quarantine_filter(df: DataFrame, union_df: DataFrame, rule_name: str, rule_filter: str) -> DataFrame:
  negated_filter = "NOT({0})".format(rule_filter)
  filtered_df = (df.filter(expr(negated_filter))
                   .withColumn('_expectation_failed', lit(rule_name)))
  if union_df is not None:
    return filtered_df.unionAll(union_df)
  else:
    return filtered_df

# Create a quarantine table for data that doesn't meet quality expectations
def create_quarantine(func, table_name, expect_or_drop_rules):
    @dlt.table(name=table_name + "_quarantine")
    def quarantine_tbl():
        df = func() # starting dataframe that is expressed in the DLT function of the original code that will call the library
        output_df: DataFrame = None
        for dq_rule in expect_or_drop_rules:
          output_df = quarantine_filter(df, output_df, dq_rule.name, dq_rule.filter)
        # adding metadata columns with execution information
        return (output_df
                .withColumn("_execution_timestamp", current_timestamp())
        )

# Apply expectation rules to a function
def apply_expectation_rules(func, table_name):
    for dq_rule in dq_rules[table_name]:
        func = dq_rule.func(dq_rule.name, dq_rule.filter)(func) 
    return func

# Get rules that use expect_or_drop
def get_expect_or_drop_rules(dq_rules):
  rules = []
  for dq_rule in dq_rules:
    if (dq_rule.func == dlt.expect_or_drop):
        rules.append(dq_rule)
  return rules

# Decorator for applying data quality rules
def data_quality(table=None):
    def wrapper(func):
        table_name = table if table else func.__name__

        @wraps(func)
        def inner_wrapper():                                   
            return func()

        if table_name in dq_rules:
            drop_rules = get_expect_or_drop_rules(dq_rules[table_name])
            if drop_rules:
              create_quarantine(func, table_name, drop_rules)
              inner_wrapper = apply_expectation_rules(inner_wrapper, table_name)
        return inner_wrapper
    
    return wrapper