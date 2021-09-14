"""
pyspark

from stitchr.df_checks import *

"""

import json
import pyspark
from pyspark.sql.dataframe import DataFrame

# import typing_extensions
# import sys
# from typing import Tuple

spark = pyspark.sql.SparkSession.builder.getOrCreate()
# spark.sparkContext.setLogLevel('WARN')

# useful utility functions


def read_csv_file(file_path: str, inferSchema = False) -> DataFrame:
    return spark \
        .read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", f"{inferSchema}") \
        .load(file_path)


def read_csv_file_with_schema(file_path: str, schema) -> DataFrame:
    return spark \
        .read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", False) \
        .schema(schema) \
        .load(file_path)


def load_json_file(file_path: str) -> dict:
    import json
    # Opening JSON file
    f = open(file_path,)
    # returns JSON object as a dictionary
    data = json.load(f)
    # Closing file after loading
    f.close()
    return data


def load_json_spec(file_path: str) -> dict:
    # Opening JSON spec file
    f = open(file_path, )

    # returns JSON object as a dictionary
    spec = json.load(f)
    # Closing file
    f.close()
    return spec


def _test():
    """
    test code
    :return:
    """
    import os
    from pyspark.sql import SparkSession
    import pyspark.sql.catalog

    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.catalog.__dict__.copy()
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName("column checks  tests") \
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    globs['spark'] = spark
    # ...


if __name__ == "__main__":
    print('running tests \n')
    _test()
