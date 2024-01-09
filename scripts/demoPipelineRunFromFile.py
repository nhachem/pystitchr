"""
On single-node emr after setting wheel and installing pystitchr

pyspark
import demoPipelineRunFromFile
or
python demoPipelineRunFromFile.py

: demo pipeline runs for transformations

"""

# from pyspark.sql import SparkSession
from pyspark.sql import *
import sys
import time
import logging

# this import is technically not needed as subsequent import initializes pystitchr to include the dataframe extensions
# import pystitchr
from pystitchr.util.utils import *
spark = SparkSession.builder.getOrCreate()
# use INFO to log correctly, DEBUG is cryptic...
logging_level = logging.INFO
# setting the spark level logging level
spark.sparkContext.setLogLevel('WARN')

print(sys.path)

# use a relative path to the code
data_dir = "../data/"

spec_dir = "../resources"

pipeline_spec = load_json_spec(f'{spec_dir}/demo_pipeline.json')

json_df = spark.read.format("json").option("multiline", "true").load(f"../resources/json_test.json")
print(json_df.count())
json_df.printSchema()

test_df = spark.read.format("csv").option("header", True).option("inferSchema", True) \
    .load(f"{data_dir}/test_input_data.csv.gz").crossJoin(json_df)

test_df.createOrReplaceTempView("_tmp")
test_df.printSchema()

df1 = test_df

# df_out = dft.run_pipeline(df1, pipeline_spec, logging_level)
df_out = df1.run_pipeline(pipeline_spec, logging_level)
df_out.printSchema()
df_out.show(20, False)

t_start = time.perf_counter()
df_out.write.format('csv').option('header', True).mode('overwrite').save(f"{data_dir}/testPipeline.csv")
print(f"runtime is {round(time.perf_counter() - t_start, 2)}")
