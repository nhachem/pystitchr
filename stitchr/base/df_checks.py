"""
pyspark

from stitchr.base.df_checks import *

"""

# import os
# import typing

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import col, min, max
from pyspark.sql.dataframe import DataFrame
# import pyspark.sql.functions as f

# import typing_extensions

# import sys
# from random import choice
# from string import ascii_letters
# import re
# from typing import Tuple

spark = pyspark.sql.SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

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
    # "Num": "float",
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
column 
"""
"""
NH: we may need more work DecimalType() returns DecimalType(10,0)
This list could be stored and retrieved based on string maps?
"""
numeric_sql_types: tuple = (
    ByteType(),
    ShortType(),
    LongType(),
    IntegerType(),
    FloatType(),
    DoubleType(),
    DecimalType())


def is_numeric_data_type(df: DataFrame, column_name: str) -> bool:
    """
    checks if the column type is numeric data type
    :param df:
    :param column_name:
    :return: set of records that fail. So if empty it means true else false
    """
    d = next(field.dataType for field in df.schema.fields if field.name == column_name)
    # NH: assume it is there and we expect it to occur once... else we need to check for null

    return d in numeric_sql_types


def check_numeric_bound_pt(df: DataFrame,
                           column_name: str,
                           value: float = 0,
                           operation: str = '<') -> (DataFrame, DataFrame):
    """
    generic check bounds
    :param df:
    :param column_name:
    :param value:
    :param operation:
    :return: set of records that fail. So if empty it means true else false
    """
    return df, df.filter(f"{column_name} {operation} {value}")


def check_numeric_bound(df: DataFrame, column_name: str, value: float = 0, operation: str = '<') -> DataFrame:
    """
    check upper or lower bound bound

    :param df:
    :param column_name:
    :param value:
    :param operation:
    :return: set of records that fail. So if empty it means true else false
    """
    _, df_nb = check_numeric_bound_pt(df, column_name, value, operation)
    return df_nb


def check_upper_bound(df: DataFrame, column_name: str, value: float) -> DataFrame:
    """
    this check upper bound values same as check_numeric_bound with <
    :param df:
    :param column_name:
    :param value:
    :return: set of rows that break the constraints else null
    """
    # NH: may need to add `` to the strings for non conventional attributes
    # make sure the type is numeric
    return df.filter(col(column_name) < value)


def check_lower_bound(df: DataFrame, column_name: str, value: float) -> DataFrame:
    """
    this check lower  bound values same as check_numeric_bound with >
    :param df:
    :param column_name:
    :param value:
    :return:
    """
    # NH: may need to add `` to the strings for non conventional attributes
    return df.filter(col(column_name) > value)


def check_numeric_range(df: DataFrame, column_name: str, low_value: float, high_value: float) -> (DataFrame, float):
    """

    :param df:
    :param column_name:
    :param low_value:
    :param high_value:
    :return:
    """
    # NH: assumes we already checked is numeric
    # df_bound = check_numeric_bound(df, column_name, low_value, '>')\
    #    .exceptAll(check_numeric_bound(df, column_name, high_value, '>'))
    df_bound = check_numeric_bound(check_numeric_bound(df, column_name, low_value, '>'), column_name, high_value, '<')
    return df_bound, (df_bound.count()/df.count())*100


def check_numeric_bounds(df: DataFrame, column_name: str, low_value: float, high_value: float) -> (DataFrame, float):
    """
    check numric data boundaries
    :param df:
    :param column_name:
    :param low_value:
    :param high_value:
    :return:
    """
    # assumes we checked that the column is numeric allready
    df_bound = df.filter(col(column_name) < high_value).filter(col(column_name) > low_value)
    return df_bound, (df_bound.count()/df.count())*100


def check_less_than(df: DataFrame, column_name: str, value: float = 0) -> DataFrame:
    """

    :param df:
    :param column_name:
    :param value:
    :return:
    """
    return check_numeric_bound(df, column_name, value)


def check_greater_than(df: DataFrame, column_name: str, value: float = 0) -> DataFrame:
    """

    :param df:
    :param column_name:
    :param value:
    :return:
    """
    return check_numeric_bound(df, column_name, value, '>')


def check_negative_numeric(df: DataFrame, column_name: str) -> DataFrame:
    """

    :param df:
    :param column_name:
    :return:
    """
    return check_less_than(df, column_name, 0)


def check_positive_numeric(df: DataFrame, column_name: str) -> DataFrame:
    """

    :param df:
    :param column_name:
    :return:
    """
    return check_greater_than(df, column_name, 0)


def get_boundaries(df: DataFrame, column_name: str) -> (float, float):
    """
    get the min and max values in a column. If it is a tring then string sorting alphabetically is implied
    :param df:
    :param column_name:
    :return: (min_value, max_value)
    """
    return df.select(min(column_name), max(column_name)).first()


def get_numeric_boundaries(df: DataFrame, column_name: str) -> (float, float):
    """
    get the min and max values in a numric column. forces a cast to float.
    If the column can't be casted as such then this wil throw an error which is currently not trapped
    :param df:
    :param column_name:
    :return: (min_value, max_value)
    """
    return df.select(col(column_name).cast("float").alias(column_name)).select(min(column_name), max(column_name)).first()


def check_string_length(df: DataFrame, column_name: str, low_length: int, high_length: int) -> DataFrame:
    """
    this is a string range type check vs a numeric range check
    may be able to return a DF of all string that fail of an empty frame?
    :param df:
    :param column_name:
    :param low_length:
    :param high_length:
    :return:
    """
    # NH: may need to add `` to the strings for non conventional attributes
    # place holder
    return df


def check_column_nulls(df: DataFrame, column: str) -> (DataFrame, DataFrame):
    """

    :param df:
    :param column:
    :return:
    """
    # here checks individual column if it has nulls and returns all columns that are null
    return df, df.where(col(column).isNull())


def get_nulls(df: DataFrame, column: str) -> (DataFrame, float):
    """

    :param df:
    :param column:
    :return:
    """
    _, df_nulls = check_column_nulls(df, column)
    percent = (df_nulls.count()/df.count())*100
    return df_nulls, percent


def check_all_null(df: DataFrame, column_list: list) -> DataFrame:
    """

    :param df:
    :param column_list:
    :return:
    """
    # here checks are on each individually and returns the list of columns (as a dataframe ?) that are actually nulls
    # if input list is null then all columns are checks. If except is true (default is false) then all columns except in the list are checked.
    # difficult to do across all columns... one needs to apply .isNull to each column and then collapse to verify
    # NH: place holder
    # will use the check_column_null
    return df #.where(col("dt_mvmt").isNull())


def domain_check(df: DataFrame, src_column_names_list: list,
                 lookup_df: DataFrame, lookup_column_list: list) -> DataFrame:
    """
    like an FK check
    :param df:
    :param src_column_names_list:
    :param lookup_df:
    :param lookup_column_list:
    :return:
    """
    # NH: like an FK check
    #  if (lookupColumnList.isEmpty)
    if lookup_column_list:
        target_column_list = lookup_column_list
    else:
        target_column_list = src_column_names_list
    tcl = f"`{'`,`'.join(target_column_list)}`".split(',')
    r_df = lookup_df.select(*tcl)
    return df.select(*src_column_names_list).subtract(r_df)


def unique_check(df,
                 column_names_list: list,
                 pass_through: bool = False) -> DataFrame:
    """
    like a pk check
    :param df:
    :param column_names_list:
    :param pass_through:
    :return:
    """
    df_pk: DataFrame = df.select(*column_names_list)\
        .groupBy(*column_names_list)\
        .count().withColumnRenamed("count", "group_count")\
        .filter(col("group_count") > 1)
    if pass_through:
        return df_pk # need to use dynamic join to do it... we can write this in both python or scala
    else:
        return df_pk # return the problem records


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
