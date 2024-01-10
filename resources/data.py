"""
pyspark

import df_transform

"""
import os

# from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()

test_df: DataFrame = spark.createDataFrame(
    [("A", 0, 1), ("B", 1, 2), ("C", 2, 3)], schema=("K E   Y", "cols with   sp  aces", " .value")
)

test_df1: DataFrame = spark.createDataFrame(
    [("A", 0, 1), ("B", 1, 2), ("C", 2, 3)], schema=("K E   Y", "cols with   sp  aces", "value")
)

test_df_float: DataFrame = spark.createDataFrame(
    [
        ("A", "0", 1, 1.0),
        ("B", "1", 2, 2.1),
        ("C", "a,b", 3, 3.0),
        ("D", "2.1.1", 2, 5.0),
        ("D", "1.", 2, 1.0),
        ("D", ".1", 2, 0.001),
    ],
    schema=("key", "string_value", "int_value", "float_value"),
)

test_df_ts: DataFrame = spark.createDataFrame(
    [
        ("A", "2018-01-08 00:00:00"),
        ("B", "2018-01-08"),
        ("C", "2018-31-12 00:00:00"),
        ("D", "2.1.1"),
        ("D", "2020-10-08 00:00:00"),
        ("D", ".1"),
    ],
    schema=("key", "string_value"),
)

simpleData = (
    ("James", "", "Smith", 36636, "NewYork", 3100),
    ("Michael", "", "Rose", 40288, "California", 4300),
    ("Robert", "", "Williams", 42114, "Florida", 1400),
    ("Maria", "Anne", "Jones", 39192, "Florida", 5500),
    ("Jen", "Mary", "Brown", 34561, "NewYork", 3000),
)
columns = ["firstname", "middlename", "lastname", "id", "location", "salary"]

simple_df = spark.createDataFrame(data=simpleData, schema=columns)

"""
# this is a sample from UK companies data persons-with-significant-control snapshot 2021-01-11
df_uk_companies_holdings: DataFrame = spark\
    .read\
    .format("json")\
    .load(f"resources/json_sample_data.json")
"""

# need to make a deterministic json with arrays maybe 2 arrays
# This is set through running pystitchr_env.sh
python_root_path = f"{os.environ.get('ROOT_DIR')}"
print(python_root_path)
df_json: DataFrame = (spark.read.format("json").option("multiline", "true")
                      .load(f"{python_root_path}/resources/json_test.json")
                      )


test_run_pipeline_spec = [
    {
        "function": "rename_columns",
        "params": {"rename_mapping_dict": {"key": "key_value", "string_value": "s_val"}, "strict": False},
    },
    {"function": "drop_columns", "params": {"drop_columns_list": ["int_value"]}},
    {"function": "select_list", "params": {"column_list": ["key_value", "float_value"], "strict": False}},
]

test_df_cc: DataFrame = spark.createDataFrame(
    [("source_system_1", 0, 1), ("source_system_2", 1, 2), ("source_system_2", 2, 3)],
    schema=("source_system", "external_id", "field_1"),
)

test_df_cc_reference: DataFrame = spark.createDataFrame(
    [
        ("source_system_1", 0, 1, "2a97d6f573957f9d84406ce329d589d9"),
        ("source_system_2", 1, 2, "b891b437393b4f250dc8327a48de50f6"),
        ("source_system_2", 2, 3, "c99044c18b0f245ab1d09c2728593f30"),
    ],
    schema=("source_system", "external_id", "field_1", "id"),
)

test_df_cc_reference_id_concat: DataFrame = spark.createDataFrame(
    [
        ("source_system_1", 0, 1, "source_system_1:0"),
        ("source_system_2", 1, 2, "source_system_2:1"),
        ("source_system_2", 2, 3, "source_system_2:2"),
    ],
    schema=("source_system", "external_id", "field_1", "id"),
)

test_s_df: DataFrame = spark.createDataFrame(
    [
        (0, 1, "hello"),
    ],
    schema=("i", "s", "field_1"),
)

test_l_df: DataFrame = spark.createDataFrame(
    [
        (1, 2, "hello"),
    ],
    schema=("l", "t"),
)
