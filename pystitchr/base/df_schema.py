"""
pyspark

from stitchr_extensions.df_schema import *

"""

# import os
# from pyspark.sql.functions import concat_ws, collect_list
# import typing

import pyspark
# StructField, StructType, ArrayType, MapType
from pyspark.sql.types import *
# col, concat, lit, when, explode
from pyspark.sql.functions import when, explode
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as f

# import typing_extensions

import sys
from random import choice
from string import ascii_letters
import re

spark = (pyspark.sql.SparkSession.builder.getOrCreate())
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

"""
dict structures that we can get from data catalogs
"""

to_spark_type_dict_by_string = {
    "string": "StringType",
    "Char": "StringType",
    "Datetime": "TimestampType",
    "Duration": "FloatType",
    "Double": "DoubleType",
    "number": "DoubleType",
    # "Num": "FloatType",
    "Num": "DoubleType",
    "float": "FloatType",
    "integer": "LongType",
    "Int": "LongType",
    "boolean": "BooleanType",
    "Boolean": "BooleanType",
    "None": "NullType",
    "Unknown": "StringType"
}

cast_to_spark_type_dict = {
    "string": "string",
    "Char": "string",
    "Datetime": "timestamp",
    "Duration": "float",
    "number": "double",
    # "Num": "FloatType",
    "Num": "double",
    "float": "float",
    "integer": "long",
    "Int": "long",
    "boolean": "boolean",
    "Boolean": "boolean",
    "None": "string",
    "Unknown": "string"
}

"""spark = SparkSession.builder.getOrCreate()
# import spark.implicits._
spark.sparkContext.setLogLevel('WARN')
"""
# print(sys.path)

""" setting up the path to include Stitchr and other project related imports"""


# sys.path.append(os.environ['STITCHR_ROOT'] + '/pyspark-app/app')

# print("Spark Version:", spark.sparkContext.version)

"""
to_spark_type_dict_by_string
cast_to_spark_type_dict
generate_schema_by_string
generate_missing_columns
left_diff_schemas
right_diff_schemas
schema_diff
fields_diff
"""

"""schema generation code, including missing columns 
"""
"""
assumes we have the table schema_metadata
we could pass the columns as a list and then convert tin the function
Need to add a catch all StringType() to the map
"""


def generate_schema_by_string(domain: str, columns: list, attributes_df: DataFrame):
    """
    using call by string name getattr()
    @param domain:
    @type domain:
    @param columns:
    @type columns:
    @param attributes_df:
    @type attributes_df:
    @return:
    @rtype:
    """
    col_df = spark.createDataFrame(columns, StringType())
    column_meta_df = attributes_df \
        .filter(f"domain_prefix = '{domain}'") \
        .select("sequence", "variable_name", "datatype", "core")
    # NH need to test that the order is conserved...
    meta_df = col_df.join(column_meta_df, col_df.value == column_meta_df.variable_name, "left") \
        .drop("variable_name") \
        .withColumnRenamed("value", "variable_name") \
        .withColumn("datatype", when(column_meta_df.datatype.isNull(), "Unknown")
                    .otherwise(column_meta_df.datatype))
    # meta_df.show(50, False)
    column_meta = meta_df.collect()
    # print(column_meta)
    m = list(map(lambda column: StructField(column.variable_name,
                                            getattr(t, to_spark_type_dict_by_string[column.datatype])(), True),
                 column_meta))
    return StructType(m)


def generate_missing_columns(domain: str, columns: list, attributes_df: DataFrame) -> list:
    """
    generate add on columns type casted
    @param domain:
    @type domain:
    @param columns:
    @type columns:
    @param attributes_df:
    @type attributes_df:
    @return:
    @rtype:
    """
    col_df = spark.createDataFrame(columns, StringType())
    # col_df.show(50)
    column_meta_df = attributes_df \
        .filter(f"domain_prefix = '{domain}'") \
        .select("sequence", "variable_name", "datatype", "core")
    # NH need to test that the order is conserved...
    meta_df = col_df.join(column_meta_df, col_df.value == column_meta_df.variable_name, "right") \
        .filter("value is null").orderBy("sequence")
    # meta_df.show(50)
    column_meta = meta_df.collect()
    # print(column_meta)
    m = list(map(lambda column: (column.sequence, column.variable_name, cast_to_spark_type_dict.get(column.datatype)),
                 column_meta))
    return m


def generate_ordered_column_list(domain: str, attributes_df: DataFrame) -> list:
    """
    This function gets a list of columns by position directly returned from a schema definition structure
    (here in attributes_df)
    @param domain:
    @type domain:
    @param attributes_df:
    @type attributes_df:
    @return:
    @rtype:
    """
    row_list = attributes_df \
        .filter(f"domain_prefix = '{domain}'") \
        .select("sequence", "variable_name").orderBy("sequence")\
        .collect()

    return [row['variable_name'] for row in row_list]


def get_schema(df: DataFrame) -> DataFrame:
    """
    returns the schema as a DataFrame
    @param df:
    @type df:
    @return:
    @rtype:
    """
    _df = spark.read.json(sc.parallelize([df.schema.json()]))
    return _df.withColumn("field", explode("fields")).drop("fields").select("field.*")


def left_diff_schemas(left_df: DataFrame, right_df: DataFrame) -> list:
    """
    takes 2 dataframes (left and right) and
    returns a list of columns in left DataFrame set but not in the right DataFrame
    @param left_df:
    @type left_df:
    @param right_df:
    @type right_df:
    @return:
    @rtype:
    """
    left_columns_set = set(left_df.schema.fieldNames())
    right_columns_set = set(right_df.schema.fieldNames())
    # print(list(df_columns_set))
    # warn that some columns are not in the list... Or maybe throw an error?
    return list(left_columns_set - right_columns_set)


def right_diff_schemas(left_df: DataFrame, right_df: DataFrame) -> list:
    """
    takes 2 dataframes (left and right) and
    returns a list of columns in right DataFrame  but not in the left DataFrame
    @param left_df:
    @type left_df:
    @param right_df:
    @type right_df:
    @return:
    @rtype:
    """
    left_columns_set = set(left_df.schema.names)
    right_columns_set = set(right_df.schema.names)
    # print(list(df_columns_set))
    # warn that some columns are not in the list... Or maybe throw an error?
    return list(right_columns_set - left_columns_set)


# modify to test nested and also use set operations left.diff(right)?
# look into panda equivalent or maybe qoalas
def schema_diff(left_df: DataFrame, right_df: DataFrame):
    """
    returns the left and right schema difference
    @param left_df:
    @type left_df:
    @param right_df:
    @type right_df:
    @return:
    @rtype:
    """
    right_columns_set = set(right_df.schema)
    left_columns_set = set(left_df.schema)
    return ([l for l in left_df.schema if l not in right_columns_set],
            [r for r in right_df.schema if r not in left_columns_set]
            )


# to add to pystitchr
def fields_diff(left_df: DataFrame, right_df: DataFrame):
    """
    difference of field names (similar to schema_diff but only on names)
    @param left_df:
    @type left_df:
    @param right_df:
    @type right_df:
    @return:
    @rtype:
    """

    l_set = set(left_df.schema.fieldNames())
    r_set = set(right_df.schema.fieldNames())
    return l_set.difference(r_set), r_set.difference(l_set)


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
        .appName("sql.catalog tests") \
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    globs['spark'] = spark
    # ...


if __name__ == "__main__":
    print('running tests \n')
    _test()
