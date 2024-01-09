"""
On single-node emr after setting wheel and installing pystitchr

pyspark
import pipelineRuns
or
python demoPipelineRun.py

demo pipeline runs for transformations

"""

# from pyspark.sql import SparkSession
from pyspark.sql import *
import sys
import time
import logging
from pystitchr.util.utils import *
# this import is needed as we need to initialize pystitchr to include the dataframe extensions
# import pystitchr

spark = SparkSession.builder.getOrCreate()
# use INFO to log correctly, DEBUG is cryptic...
logging_level = logging.INFO
# setting the spark level logging level
spark.sparkContext.setLogLevel("WARN") # we need to control what spark dumps...

print(sys.path)

# use a relative path to the code
data_dir = "../data/"

json_df = spark.read.format("json").option("multiline", "true").load(f"../resources/json_test.json")
print(json_df.count())
json_df.printSchema()

test_df = (spark.read.format("csv").option("header", True).option("inferSchema", True)
           .load(f"{data_dir}/test_input_data.csv.gz")
           .crossJoin(json_df)
           )

test_df.createOrReplaceTempView("_tmp")

test_df.printSchema()

# for unpivot we need to specify the left hand keys.
# The code can be extended to take the diff of attributes if none are provided but does not handle that case yet
# note that the functions that may be added as UDFs are in df_functions
# note: select exclude is like drop_columns but may run more efficiently
# rename_4_parquet is a wrapper with dummy params for rename_4_parquet... should be able to do better

random_pivot_table = get_random_string(10)
pipeline_spec = [
    {
        "function": 'add_columns',
        "params": {
            "new_columns_mapping_dict": {
                'BARCODE': 'get_random_alphanumeric(8, ceil(f3*1000))',
                'WELL_NUMBER': 'ceil(f2*100)',
                'PLATE_ID': 'get_random_alphanumeric(8, ceil(f4*1000))',
                'EXPERIMENT_ID': 'get_random_alphanumeric(15, ceil(f6*1000))'
            }
        }
    },
    {
        "function": "rename_columns",
        "params": {
            "rename_mapping_dict": {
                "f1": "foo",
                "f2": "bar",
                'f6': "not-a-parquet(),{name}"
            }
        }
    },
    {
        "function": 'rename_4_parquet',
        "params": [],
    },
    {
        "function": "drop_columns",
        # either work
        # "params": ["f3", "f4", "f7", "f8", "f9"],
        "params": {"drop_columns_list": ["f3", "f4", "f7", "f8", "f9"]},
    },
    {
        "function": "flatten",
        "params": [],
    },
    {
        "function": "unpivot",
        "params": {"params_dict": {"keys": ['BARCODE', 'WELL_NUMBER', 'PLATE_ID', 'EXPERIMENT_ID',
                                            'address__city', 'not__a__parquet________name__'],
                                    "unpivot_columns": ["foo", "bar", "f5"],
                                    "key_column": "key_column",
                                    "value_column": "value"
                    }
                   },
    },
    {
        "function": "pivot",
        "params": {"params_dict": {"hive_view": f"test_{random_pivot_table}"}}
     },
    {
        "function": 'select_list',
        "params": ['PLATE_ID',
                   'WELL_NUMBER',
                   'BARCODE',
                   'not__a__parquet________name__',
                   'f5',
                   'foo']
    },
    {"function": 'select_exclude',
     "params": ['f5']
     },
    {
        "function": 'filter_or',
        "params": ['WELL_NUMBER = 20',
                   'WELL_NUMBER = 30'],
    },
    {
        "function": 'select_list',
        "params": ['PLATE_ID',
                   'WELL_NUMBER',
                   'BARCODE',
                   'not__a__parquet________name__',
                   'foo']
    },
]

df1 = test_df
df_out = df1.run_pipeline(pipeline_spec, logging_level)
df_out.printSchema()
df_out.show(20, False)
t_start = time.perf_counter()
df_out.write.format('parquet').option('header', True).mode('overwrite').save(f"{data_dir}/testPipeline.parquet")
print(f"runtime is {round(time.perf_counter() - t_start, 2)}")

# checking the pivoted view
# spark.sql(f"select * from test_{random_pivot_table}").show()
