"""
On single-node emr after setting wheel and installing pystitchr

pyspark
import pipelineRuns
or
python demoPipelineRun_v0_3.py

demo pipeline runs for transformations

"""

# from pyspark.sql import SparkSession
from pyspark.sql import *
import sys
import time
import logging
from pystitchr.util.utils import *
# this import is needed as we need to initialize pystitchr to include the dataframe extensions
import pystitchr

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

test_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(f"{data_dir}/test_input_data.csv.gz").crossJoin(json_df)

test_df.createOrReplaceTempView("_tmp")

test_df.printSchema()

# for unpivot we need to specify the left hand keys.
# The code can be extended to take the diff of attributes if none are provided but does not handle that case yet
# note that the functions that may be added as UDFs are in df_functions
# note: select exclude is like drop_columns but may run more efficiently
# rename_4_parquet is a wrapper with dummy params for rename_4_parquet... should be able to do better

random_pivot_table = get_random_string(10)
pipeline_spec = {1: {'add_columns': {'BARCODE': 'get_random_alphanumeric(8, ceil(f3*1000))',
                                     'WELL_NUMBER': 'ceil(f2*100)',
                                     'PLATE_ID': 'get_random_alphanumeric(8, ceil(f4*1000))',
                                     'EXPERIMENT_ID': 'get_random_alphanumeric(15, ceil(f6*1000))'
                                     }
                     },
                 2: {"rename_columns": {"f1": "foo", "f2": "bar", 'f6': "not-a-parquet(),{name}"}},
                 3: {'rename_4_parquet': []},
                 4: {"drop_columns": ["f3", "f4", "f7", "f8", "f9"]},
                 5: {"flatten": []},
                 # output from unpivot generates a key_column and a value column.
                 # 6: {"unpivot": {"keys": ['BARCODE', 'WELL_NUMBER', 'PLATE_ID', 'EXPERIMENT_ID', 'address__city',
                 #                         'not__a__parquet________name__'],
                 #                "unpivot_columns": ["foo", "bar", "f5"]
                 #                }},
                 6: {"unpivot": {"keys": ['BARCODE', 'WELL_NUMBER', 'PLATE_ID', 'EXPERIMENT_ID',
                                          'address__city', 'not__a__parquet________name__'],
                                 "unpivot_columns": ["foo", "bar", "f5"],
                                 "key_column": "key_column", "value_column": "value"
                                 }},
                 # pivot defaults to processing a key_column, value column-pair.
                 # But we can specify alternates with the params dict
                 # 7: {"pivot": {}},
                 7: {"pivot": {"hive_view": f"test_{random_pivot_table}"}},
                 # 7: {"pivot": {"pivoted_values": ["foo", "f5"]}},
                 # 7: {"pivot": {"pivot_values": ["foo", "f5"],
                 #              "key_column": "key_column", "value_column": "value"}},
                 8: {'select_list': ['PLATE_ID', 'WELL_NUMBER', 'BARCODE', 'not__a__parquet________name__',
                                     'f5', 'foo']},
                 9: {'select_exclude': ['f5']},
                 10: {'filter_and': ['WELL_NUMBER >= 20', 'WELL_NUMBER <= 30']},
                 # 11: {'filter_or': ['WELL_NUMBER in ( 20, 30)']},
                 11: {'filter_or': ['WELL_NUMBER = 20', 'WELL_NUMBER = 30']},
                 12: {'select_list': ['PLATE_ID', 'WELL_NUMBER', 'BARCODE', 'not__a__parquet________name__', 'foo']},
                 }

df1 = test_df
df_out = df1.run_pipeline_v0_3(pipeline_spec, logging_level)
# this would default to ERROR which means no logging
# df_out = dft.run_pipeline(df1, pipeline_spec)
df_out.printSchema()
# df_out.show(20, False)
# checking the pivoted view
spark.sql(f"select * from test_{random_pivot_table}").show()
t_start = time.perf_counter()
df_out.write.format('csv').option('header', True).mode('overwrite').save(f"{data_dir}/testPipeline.csv")
print(f"runtime is {round(time.perf_counter() - t_start, 2)}")

