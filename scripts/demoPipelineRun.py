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
import json
import pystitchr.base.df_transforms as dft

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

print(sys.path)

# use a relative path to the code
data_dir = "../data/"

# Opening JSON file
# f = open('test.json',)

# returns JSON object as
# a dictionary
# data = json.load(f)
# Closing file
# f.close()


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
# rename_4_parquet_p is a wrapper with dummy params for rename_4_parquet... should be able to do better
# same for flatten_p
pipeline_spec = {1: {'add_columns': {'BARCODE': 'get_random_alphanumeric(8, ceil(f3*1000))',
                     'WELL_NUMBER': 'ceil(f2*100)',
                     'PLATE_ID': 'get_random_alphanumeric(8, ceil(f4*1000))',
                     'EXPERIMENT_ID': 'get_random_alphanumeric(15, ceil(f6*1000))'
                                     }
                     },
                 2: {"rename_columns": {"f1": "foo", "f2": "bar", 'f6': "not-a-parquet(),{name}"}},
                 3: {'rename_4_parquet_p': []},
                 4: {"drop_columns": ["f3", "f4", "f7", "f8", "f9"]},
                 5: {"flatten_p": []},
                 6: {"unpivot_p": [['BARCODE', 'WELL_NUMBER', 'PLATE_ID', 'EXPERIMENT_ID', 'address__city', 'not__a__parquet________name__'], ["foo", "bar", "f5"]]},
                 7: {"pivot_p": ["foo", "f5"]},
                 8: {'select_list': ['PLATE_ID', 'WELL_NUMBER', 'BARCODE', 'not__a__parquet________name__', 'f5', 'foo']},
                 9: {'select_exclude': ['f5']},
                 10: {'filter_and': ['WELL_NUMBER >= 20', 'WELL_NUMBER <= 30']},
                 # 11: {'filter_or': ['WELL_NUMBER in ( 20, 30)']},
                 11: {'filter_or': ['WELL_NUMBER = 20', 'WELL_NUMBER = 30']},
                 12: {'select_list': ['PLATE_ID', 'WELL_NUMBER', 'BARCODE', 'not__a__parquet________name__', 'foo']},
                 }

df1 = test_df

df_out = dft.run_pipeline(df1, pipeline_spec)
df_out.printSchema()
# df_out.show(20, False)

import time
t_start = time.perf_counter()
df_out.write.format('csv').option('header', True).mode('overwrite').save(f"{data_dir}/testPipeline.csv")
print(f"runtime is {round(time.perf_counter() - t_start, 2)}")
# print(json_df.count())
# json_df.printSchema()
