"""
pyspark

from stitchr_extensions.df_checks import *

"""

import pyspark
from pyspark.sql.functions import col, when, size, split, to_timestamp
from pyspark.sql import DataFrame
# import typing_extensions
import sys

spark = pyspark.sql.SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

"""
dict structures that we can get from data catalogs
"""

to_spark_type_dict_by_string: dict = {
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
column utility functions
"""

# testing function name .... we would use to have 1to1 between pandas and spark (to be done later)
"""
https://stackoverflow.com/questions/5067604/determine-function-name-from-within-that-function-without-using-traceback
# for current func name, specify 0 or no argument.
# for name of caller of current func, specify 1.
# for name of caller of caller of current func, specify 2. etc.
"""


def current_func_name(n: int = 0) -> str:
    sys._getframe(n + 1).f_code.co_name


# this is under test to have a function print its name
def print_function_info():
    print(f"You are in function: {current_func_name()}")
    print(f"This function's caller was: {current_func_name( 1 )}")


def select_list(df: DataFrame, column_list: list) -> DataFrame:
    """
    return select the specified column list
    :param df:
    :param column_list:
    :return:
    """
    # NH: 2/12/21 following should be made as a function that takes a List makes a string adding ``
    # and re-splitting to support funky column names
    cl: list = f"`{'`,`'.join( column_list )}`".split(',')
    return df.select(*cl)


def select_column_list(column_list: list) -> DataFrame:
    """
    generates a dataframe in the order of the column list.
    :param column_list:
    :return: DataFrame
    """
    def _(df_implicit: DataFrame) -> DataFrame:
        cl: list = f"`{'`,`'.join( column_list )}`".split(',')
        return df_implicit.select(*cl)
    return _


def select_column_list_args(*column_list) -> DataFrame:
    """
    same as select_column_list but we attempt to make all function based on *args or **kwargs
    :param column_list:
    :return: DataFrame
    """
    def _(df_implicit: DataFrame) -> DataFrame:
        return df_implicit.transform(select_column_list(column_list))
    return _


def g_n_s(df: DataFrame, column_name: str, column_name_ref: str) -> (int, float):
    """
    generates 2 values: count of null and relative percent of nulls (relative as we could have the column
    as a transform of another which already has nulls and we want to get the actual additional stats)

    :param df:
    :param column_name:
    :param column_name_ref:
    :return:
    """
    df_check = df.filter(col(f"{column_name}").isNotNull())
    if column_name_ref is None:
        count = df_check.count()
    else:
        count = df_check.count() - df.filter(col(f"{column_name_ref}").isNotNull())
    percent_null: float = (count / df.count())*100
    return count, percent_null


def get_col_null_stats(column_name: str, column_name_ref: str = None) -> DataFrame:
    """
    generates a dataframe with `column_name`, `null_cnt`, `percent_null`

    :param column_name:
    :param column_name_ref:
    :return:
    """
    def _(df_implicit) -> DataFrame:
        (c, p) = g_n_s(df_implicit, column_name, column_name_ref)
        return spark.createDataFrame([(column_name, c, p)], schema=("column_name", "null_cnt", "percent_null"))
    return _


def get_null_stats(df: DataFrame, column_name_list: list = None) -> DataFrame:
    """
    adds a column `column_name`_null with 1 if the `column_name` value is null for each in the `column_name_list`.

    If `column_name_list` is `None` then all columns are processed

    Warning: this can be expensive and time consuming

    :param df:

    :param column_name_list:

    :return:
    """
    # could use jinja but ok as is
    # template = f"(case when {{<col>>}} is null then null else 1 end) as {{<col>>}}_null_count"
    if column_name_list is None:
        c_n_l = df.schema.fieldNames()
    else:
        c_n_l = column_name_list
    select_expr_list = list(map(lambda column_name:
                                f"(case when {column_name} is null then 1 else 0 end) as {column_name}_null", c_n_l))
    # NH: ToDo change to logging.log
    print(select_expr_list)
    # df.selectExpr(*select_expr_list)
    # can change to get back a dict of stats (whic may be what we want?
    return df.selectExpr(*select_expr_list)


def get_table_columns_stats(df: DataFrame, column_name_list):
    # this does not work we need concrete hive tables
    df.createOrReplaceTempView("_tmp")
    # can't analyze a view if it is not cached
    analyze_table = f"ANALYZE TABLE _tmp COMPUTE STATISTICS FOR COLUMNS {','.join( column_name_list )}"
    df_stats = spark.sql(analyze_table)
    return df_stats


def get_unique_val(df: DataFrame, column_list: list) -> DataFrame:
    """
    get unique values... if we past a list of one column we can use to get a reference value
    for the whole dataframe (if it is a literal across all rows)
    :param df:
    :param column_list:
    :return:
    """
    # NH: ToDo change to log .print(f"This function is: {current_func_name()}")
    # print_function_info()
    return df.select(*column_list).distinct()


def get_unique_values(column_list: list) -> DataFrame:
    """
    get unique values... if we past a list of one column we can use it to get a reference value
    for the whole dataframe (if it is a literal across all rows)
    :implicit df_implicit: call with transform composition wrapper like df.transform(function_call)
    :param column_list:
    :return:
    """
    def _(df_implicit):
        return get_unique_val(df_implicit, column_list)
    return _


def get_valid_numeric(df: DataFrame, column_name: str) -> (DataFrame, int):
    """
    returns an extended DF with a column (boolean) checking if the value is numeric
    check the count vs the full cardinality of the set.
    :param df:
    :param column_name:
    :return:
    """
    # print(f"This function is: {current_func_name()}")
    # printFunctionInfo()
    pattern = "^[0-9, .]*$"
    """
    we need to check if the number of `.` is at most 2. the split returns a list of 1 when we have no decimal point 
    and thus we should check that it does not exceed 2 
    """
    df_check = df.withColumn("is_valid_numeric",
                             when(
                                  col(column_name).rlike(pattern) & (size(split(col(column_name), '\.')) < 3),
                                  True)
                             .otherwise(False))
    percent_failed = (df_check.filter("is_valid_numeric is false").count() / df_check.count()) * 100
    return df_check, df_check.filter("is_valid_numeric is false").count(), percent_failed


# 'yyyy-MM-dd HH:mm:ss'
def cast_to_timestamp(df: DataFrame, column_name: str, time_format: str = None) -> (DataFrame, int):
    """
    returns an extended DF with a column (boolean) checking if the value is a timestamp
    and adds a new column <column_name>_ts

    :param df:
    :param column_name:
    :param time_format: ex 'MM-dd-yyyy HH:mm:ss.SSSS' or 'MM-dd-yyyy HH:mm:ss'
    :return:
    """
    # print(f"This function is: {current_func_name()}")
    # printFunctionInfo()

    """
    need to add a boolean to see if the cast failed
    """
    df_check = df.withColumn(f"{column_name}_ts", to_timestamp(col(column_name), time_format))
    percent_failed = (df_check.filter(f"{column_name}_ts is Null").count() - df.filter(
        f"{column_name} is Null").count()) / df.count()
    return df_check, percent_failed


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
