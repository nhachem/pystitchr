"""
pyspark

from stitchr_extensions.df_transforms import *

"""

# import os
# from pyspark.sql.functions import concat_ws, collect_list
# import typing

import re
# import sys
from random import choice
from string import ascii_letters
from typing import List

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat, lit, when
from pyspark.sql.types import *

import pystitchr.base.df_transforms as dft
import pystitchr.base.df_functions as fn
import logging

# from pystitchr.util.log4j4y import log
from pystitchr.util.simple_logging import log

# import typing_extensions

spark = (pyspark.sql.SparkSession.builder.getOrCreate())
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
# maybe add to logging print(sys.path)

""" setting up the path to include Stitchr and other project related imports"""

# sys.path.append(os.environ['STITCHR_ROOT'] + '/pyspark-app/app')

# print("Spark Version:", spark.sparkContext.version)

"""schema generation code, including missing columns 
"""
"""
assumes we have the table schema_metadata
we could pass the columns as a list and then convert tin the function
Need to add a catch all StringType() to the map
"""


# using call by string name getattr()
def generate_schema_by_string(domain: str, columns: list, attributes_df: DataFrame):
    import pyspark.sql.types as t
    filter_string = "','".join(columns)
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
    # meta_df.printSchema()
    column_meta = meta_df.collect()
    print(column_meta)
    m = list(map(lambda column: StructField(column.variable_name,
                                            getattr(t, to_spark_type_dict_by_string[column.datatype])(), True),
                 column_meta))
    return StructType(m)


def generate_missing_columns(domain: str, columns: list, attributes_df: DataFrame) -> list:
    # using call by string name getattr()
    col_df = spark.createDataFrame(columns, StringType())
    column_meta_df = attributes_df \
        .filter(f"domain_prefix = '{domain}'") \
        .select("sequence", "variable_name", "datatype", "core")
    # NH need to test that the order is conserved...
    meta_df = col_df.join(column_meta_df, col_df.value == column_meta_df.variable_name, "right") \
        .filter("value is null").orderBy("sequence")

    # meta_df.show(50, False)
    # meta_df.printSchema()
    column_meta = meta_df.collect()
    print(column_meta)
    m = list(map(lambda column: (column.sequence, column.variable_name, cast_to_spark_type_dict.get(column.datatype)),
                 column_meta))
    return m


def left_diff_schemas(left_df: DataFrame, right_df: DataFrame) -> list:
    """

    :param left_df:
    :param right_df:
    :return:
    """
    left_columns_set = set(left_df.schema.fieldNames())
    right_columns_set = set(right_df.schema.fieldNames())
    # what is the toSet on python? .toSet
    # print(list(df_columns_set))
    # warn that some columns are not in the list... Or maybe throw an error?
    return list(left_columns_set - right_columns_set)


def right_diff_schemas(left_df: DataFrame, right_df: DataFrame) -> list:
    """

    :param left_df:
    :param right_df:
    :return:
    """
    left_columns_set = set(left_df.schema.names)
    right_columns_set = set(right_df.schema.names)
    # what is the toSet on python? .toSet
    # print(list(df_columns_set))
    # warn that some columns are not in the list... Or maybe throw an error?
    return list(right_columns_set - left_columns_set)


# modify to test nested and also use set operations left.diff(right)?
# look into panda equivalent or maybe quoalas
def schema_diff(left_df: DataFrame, right_df: DataFrame):
    right_columns_set = set(right_df.schema)
    left_columns_set = set(left_df.schema)
    return ([l for l in left_df.schema if l not in right_columns_set],
            [r for r in right_df.schema if r not in left_columns_set]
            )


# to add to pystitchr
def fields_diff(left_df: DataFrame, right_df: DataFrame):
    l_set = set(left_df.schema.fieldNames())
    r_set = set(right_df.schema.fieldNames())
    return l_set.difference(r_set), r_set.difference(l_set)


def select_list(df: DataFrame, column_list: list) -> DataFrame:
    """

    :param df:
    :param column_list:
    :return:
    """
    cl: list = f"`{'`,`'.join(column_list)}`".split(',')
    return df.select(*cl)


def select_exclude(df: DataFrame, columns_2_exclude: list) -> DataFrame:
    """
    :param df:
    :param columns_2_exclude:
    :return:
    """
    column_list: list = list(set(df.schema.fieldNames()) - set(columns_2_exclude))
    return df.select(*column_list)


def drop_columns(df: DataFrame, drop_columns_list: list) -> DataFrame:
    """

    :param drop_columns_list:
    :param df:
    :return:
    """
    df_columns_set = set(df.schema.fieldNames())
    # warn that some columns are not in the list... Or maybe throw an error?
    cols_that_do_not_exist = set(drop_columns_list) - df_columns_set
    # get the actual list of columns to drop
    columns_2_remove = list(set(drop_columns_list) - cols_that_do_not_exist)
    # drop and return
    return df.drop(*columns_2_remove)


# too long as a line need to figure out how to wrap the lambda function in multi-line code?
"""
def drop_columns(drop_columns_list: list, df: DataFrame) -> DataFrame:
    
    return df.drop(*list(set(drop_columns_list) - (set(drop_columns_list) - set(df.schema.fieldNames()))))

"""


def rename_columns(df: DataFrame, rename_mapping_dict: dict, strict: bool = True) -> DataFrame:
    # Takes a dictionary of columns to be renamed and returns a converted dataframe

    df_columns: list = df.schema.fieldNames()
    # check if any column to be renamed is non existent
    rename_columns_set = set(rename_mapping_dict.keys())
    schema_columns_set = set(df_columns)
    not_in_schema = list(rename_columns_set - schema_columns_set)
    # maybe better to change to a try except or better setup app error trapping
    if len(not_in_schema) > 0 and strict:
        log.error(f"columns to rename {not_in_schema} are not in the dataframe schema")
        exit(1)
    # we use sqlExpr to keep the schema during the rename process
    df_new_columns: list = [f"`{c}` as `{rename_mapping_dict[c]}`" if (c in rename_mapping_dict)
                            else f"`{c}`"
                            for c in df_columns]
    # NH: this does not guarantee that we keep the schema types.
    # return df.toDF(*df_new_columns) so we generated a select expression
    return df.selectExpr(*df_new_columns)


def rename_column(df: DataFrame, existing: str, new: str) -> DataFrame:
    """
    Returns a new :class:`DataFrame` by renaming an existing column.
    This is a no-op if schema doesn't contain the given column name.
    Effectively a wrapper over withColumnRenamed

    :param df:
    :rtype: object
    :param existing: string, name of the existing column to rename.
    :param new: string, new name of the column.

    >>> df.rename_column('age', 'age2').collect()
    [Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
    """
    return df.withColumnRenamed(existing, new)


def map_columns(df: DataFrame, rename_mapping_dict: dict) -> DataFrame:
    """
    This function renames all existing columns and skips the non-existing ones
    :param df:
    :param rename_mapping_dict:
    :return: DataFrame
    Takes a dictionary of columns to be renamed and returns a converted dataframe
    """
    return rename_columns(df, rename_mapping_dict, False)


def rename_4_parquet(df: DataFrame) -> DataFrame:
    """
    rename all columns of the dataFrame so that we can save as a Parquet file
    :param df:
    :return:
    """
    # need to add left/right trims and replace multiple __ with one?
    # r = "[ ,;{}()\n\t=]"
    # added "." and "-" and / so that we skip using ``
    # regex = r"[ ,;{}()\n\t=.-]"
    regex = r"[- ,;{}()\n\t=./]"
    delimiter = '__'
    schema_fields = df.schema.fields
    return spark.createDataFrame(
        df.rdd,
        StructType(
            # [StructField(re.sub(regex, delimiter, sf.name.replace(' ', '__')), sf.dataType, sf.nullable) for sf in
            [StructField(re.sub(regex, delimiter, sf.name.replace(' ', '')), sf.dataType, sf.nullable) for sf in
             schema_fields]
        )
    )


def rename_4_parquet_p(df: DataFrame, dummy_list: list = [None]) -> DataFrame:
    return rename_4_parquet(df)


def unpivot(df: DataFrame, unpivot_keys: list,
            unpivot_column_list: list,
            key_column: str = "key_column",
            value_column: str = "value") -> DataFrame:
    """

    :param df:
    :param unpivot_keys:
    :param unpivot_column_list:
    :param key_column:
    :param value_column:
    :return:
    """
    # we can improve by checking the parameter lists to be in the schema
    stack_fields_array = unpivot_column_list
    # we need to cast to STRING as we may have int, double , etc... we would couple this with extracting the types
    pivot_map_list = [f"'{s.replace('`', '')}', STRING(`{s}`) " for s in stack_fields_array]
    stack_fields: str = f"stack({len(stack_fields_array)},{','.join([str(x) for x in pivot_map_list])})"
    df.createOrReplaceTempView('_unpivot')
    q = f"select `{'`,`'.join([str(x) for x in unpivot_keys])}`, {stack_fields} as (`{key_column}`, `{value_column}`) " \
        f"from _unpivot"
    # replace with logging ... print(f'''query is: {q} \n''')
    return spark.sql(q)


def unpivot_p(df: DataFrame, params: list) -> DataFrame:
    _keys = params[0]
    _unpivot_list = params[1]
    if len(_keys) == 0:
        _keys = list(set(df.schema.fieldNames()).difference(set(_unpivot_list)))
    return unpivot(df, _keys, _unpivot_list)


def flatten0(data_frame: DataFrame) -> DataFrame:
    """
    NH: Experimental
    :param data_frame:
    :return:
    """
    fields: List[StructField] = data_frame.schema.fields
    field_names: list = data_frame.schema.fieldNames()
    # exploded_df = data_frame
    for index, value in enumerate(fields):
        field = value
        field_type = field.dataType
        field_name = field.name
        # print(f'{field}, {field_name}, {field_type}')
        # we have the case of MapTYpe to handle or isinstance(field_type, MapType)):
        # this means when we see a map we treat as array and add key/value columns?!
        if isinstance(field_type, ArrayType):
            field_names_excluding_array = [fn for fn in field_names if fn != field_name]
            field_names_to_select = field_names_excluding_array + [
                f"explode_outer({field_name}) as {field_name}"]
            # exploded_df = exploded_df.selectExpr(*field_names_to_select)
            # return flatten0(exploded_df)
            return flatten0(data_frame.selectExpr(*field_names_to_select))
        elif isinstance(field_type, MapType):
            """
            This is quite expensive if we do not have a known enumeration of key. 
            From https://stackoverflow.com/questions/52762487/flattening-maptype-column-in-pyspark
            df.withColumn("id", f.monotonically_increasing_id())\
            .select("id", f.explode("a"))\
            .groupby("id")\
            .pivot("key")\
            .agg(f.first("value"))\
            .drop("id")\
            In this case, we need to create an id column first so that there's something to group by.
            The pivot here can be expensive, depending on the size of your data.
            """
            """
            This solution here outputs a cartesian on each mapped field... 
            Using a pivot like above is better but very expensive
            posexplode does not work so we add an increasing id that we can control
            """
            df_mapped = data_frame \
                .withColumn("id", F.monotonically_increasing_id()) \
                .select('*', F.explode(field_name)) \
                .withColumnRenamed("key", f"{field_name}__key_column") \
                .withColumnRenamed("value", f"{field_name}__value") \
                .withColumnRenamed("id", f"{field_name}__id").drop(field_name)

            return flatten0(df_mapped)
            # return data_frame
        elif isinstance(field_type, StructType):
            child_fieldnames = [f"{field_name}.{child.name}" for child in field_type]
            new_fieldnames = [fn for fn in field_names if fn != field_name] + child_fieldnames
            renamed_cols = [col(x).alias(x.replace(".", "__")) for x in new_fieldnames]
            # exploded_df = exploded_df.select(*renamed_cols)
            # print(len(exploded_df.schema.fieldNames()))
            # return flatten0(exploded_df)
            return flatten0(data_frame.select(*renamed_cols))
    # print(f"schema size is {len(data_frame.schema.fieldNames())}")
    return data_frame


def flatten_no_explode(data_frame: DataFrame) -> DataFrame:
    """
    NH: experimental ...still under test.... may explode single element arrays (which may also be acceptable)
    :param data_frame:
    :return:
    """
    fields: List[StructField] = data_frame.schema.fields
    field_names: list = data_frame.schema.fieldNames()
    # print(len(field_names))
    # exploded_df = data_frame
    for index, value in enumerate(fields):
        field = value
        field_type = field.dataType
        field_name = field.name
        # print(f'{field}, {field_name}, {field_type}')
        if isinstance(field_type, StructType):
            child_fieldnames = [f"{field_name}.{child.name}" for child in field_type]
            print(f'{field_name}, {child_fieldnames}')
            new_fieldnames = [fn for fn in field_names if fn != field_name] + child_fieldnames
            renamed_cols = [col(x).alias(x.replace(".", "__")) for x in new_fieldnames]
            # exploded_df = exploded_df.select(*renamed_cols)
            # print(len(exploded_df.schema.fieldNames()))
            # exploded_df.printSchema()
            # return flatten_no_explode(exploded_df)
            return flatten_no_explode(data_frame.select(*renamed_cols))
    # print(f'schema size is {len(data_frame.schema.fieldNames())}')
    return data_frame


def flatten(data_frame: DataFrame, mode: str = 'full', delimiter: str = '__') -> DataFrame:
    # cases are full means full explode.
    #           struct only structs,
    #           map will unwind the maps as a pivot and a group by + structs
    #           array will effectively do struct and arrays only (with explode not positional)
    fields: List[StructField] = data_frame.schema.fields
    field_names: list = data_frame.schema.fieldNames()
    for index, value in enumerate(fields):
        field = value
        field_type = field.dataType
        field_name = field.name
        # print(f'{field}, {field_name}, {field_type}, {mode}')
        if isinstance(field_type, ArrayType) and mode in ['array', 'full']:
            field_names_excluding_array = [fn for fn in field_names if fn != field_name]
            field_names_to_select = field_names_excluding_array + [
                f"explode_outer({field_name}) as {field_name}"]
            # exploded_df = exploded_df.selectExpr(*field_names_to_select)
            # return flatten0(exploded_df)
            return flatten(data_frame.selectExpr(*field_names_to_select), mode, delimiter)
        elif isinstance(field_type, MapType) and mode in ['map', 'full']:
            """
            This is quite expensive if we do not have a known enumeration of key. 
            Adapted from https://stackoverflow.com/questions/52762487/flattening-maptype-column-in-pyspark
            In this case, we need to create an id column first so that there's something to group by.
            The pivot here can be expensive, depending on the size of your data.
            posexplode does not work as the pos is associated with a key and value independently.
            so we add an increasing id that we can control
            """
            df_left: DataFrame = data_frame \
                .withColumn("id", F.monotonically_increasing_id())
            # print(f"{df_left.count()}")
            # df_left.printSchema()
            df_mapped = df_left \
                .select("id", F.explode(field_name)) \
                .withColumn("key1", concat(lit(f"{field_name}{delimiter}"), col("key"))) \
                .drop("key") \
                .groupby("id") \
                .pivot("key1") \
                .agg(F.first('value')) \
                .withColumnRenamed("id", "id_right")
            # print(f"{df_mapped.count()}")
            # df_mapped.printSchema()
            df_flat: DataFrame = df_left.join(df_mapped, df_left.id == df_mapped.id_right, "inner") \
                .drop("id").drop("id_right").drop(field_name)
            return flatten(df_flat, mode, delimiter)
        elif isinstance(field_type, StructType):
            child_fieldnames = [f"{field_name}.{child.name}" for child in field_type]
            new_fieldnames = [fn for fn in field_names if fn != field_name] + child_fieldnames
            renamed_cols = [col(x).alias(x.replace(".", delimiter)) for x in new_fieldnames]
            # exploded_df = exploded_df.select(*renamed_cols)
            # print(len(exploded_df.schema.fieldNames()))
            # return flatten0(exploded_df)
            return flatten(data_frame.select(*renamed_cols), mode, delimiter)
    # print(f'schema size is {len(data_frame.schema.fieldNames())}')
    return data_frame


def flatten_p(df: DataFrame, dummy_param_list: list = [None]) -> DataFrame:
    return flatten(df)


# need to make a function @property
def get_random_string(length: int) -> str:
    """
    Random string with the combination of lower and upper case
    :param length:
    :return:
    """
    letters = ascii_letters
    result_str = ''.join(choice(letters) for _ in range(length))
    return result_str


def add_columns(df: DataFrame, new_columns_mapping_dict: dict) -> DataFrame:
    """
    This takes a dict of (new_column: str -> sql_expr: str)
    NOTICE: the code uses a | delimiter to slit a string.
    If a special function uses a delimiter then this function will fail
    ToDo: NH. If the | is an issue, we may use a default delimiter of | but allow to pass different ones as needed
    the approach would be the most efficient as the transform expressions may be quite complex.
    The implication is that UDFs are registered
    :param df:
    :param new_columns_mapping_dict: maps of {new_column: str -> sql_expr: str }
    :return:
    """
    step = new_columns_mapping_dict
    # we add `` around column names
    sql_expr = '|'.join([f"{step[c]} as `{c}`" for c in step]).split('|')
    # print(sql_expr)
    return df.selectExpr("*", *sql_expr)


def add_column(df: DataFrame, new_column: str, transform):
    """
    NH: added here for coverage. But will be rarely used
    :param df:
    :param new_column:
    :param transform:
    :return:
    """

    # Assuming all columns are correct... But we better add a check step similar to the drop columns function
    return df.withColumn(new_column, transform)


def gen_pivot_sql(df: DataFrame, pivoted_columns_list: list = [None]
                  , key_column: str = 'key_column'
                  , value_column: str = 'value'
                  , fn: str = "max") -> str:
    pivot_columns = []
    if len(pivoted_columns_list) != 0:
        pivot_columns = pivoted_columns_list
    else:
        pivot_columns = df.select(key_column).distinct().rdd.map(lambda r: r[0]).collect()
        # df.select("key").distinct().map(r => f"{r(0)}").collect().toList
        # or maybe list comprehension
        # pivot_columns = [i.k for i in df.select('k').distinct().collect()]

    # we may need to rewrite the columns to strip/replace characters that are not acceptable for column names
    #  l = s"'${pivotColumns.mkString("','")}'"
    # rewrite this as a list comprehension?
    # l = pivot_columns.foldLeft("")((head, next) => {s"$head'${next}' ${next.replace(".", "__")},"}).stripSuffix(",")
    column_list = "', '".join(map(str, pivot_columns))
    df.createOrReplaceTempView("_tmp")
    q = f"SELECT * FROM (SELECT * FROM _tmp) PIVOT ( {fn}({value_column}) FOR {key_column} in ( '{column_list}' ))"
    return q


def pivot(df: DataFrame, pivoted_columns_list: list = [None]
          , key_column: str = 'key_column'
          , value_column: str = 'value'
          , fn: str = "max") -> DataFrame:
    q = gen_pivot_sql(df, pivoted_columns_list, key_column, value_column, fn)
    return spark.sql(q)


def pivot_p(df: DataFrame, pivoted_columns_list: list = [None]) -> DataFrame:
    q = gen_pivot_sql(df, pivoted_columns_list)
    return spark.sql(q)


def filter_op(df: DataFrame, filter_expr_list: list = ["1=1"], operation: str = 'AND') -> DataFrame:
    """
    applies a composition of boolean expressions that are either ORed or ANDed together
    :param df:
    :param filter_expr_list: list of filter expressions that return boolean and stitched with the operation (AND or OR)
    :param operation: default to AND
    :return: dataframe filtered down based on the filters
    """
    operation_wrapper = f") {operation} ("
    query_filter = f"({operation_wrapper.join(map(str, filter_expr_list))})"
    return df.filter(query_filter)


def filter_and(df: DataFrame, filter_expr_list: list = ["1=1"]) -> DataFrame:
    """
    applies the AND of filter expressions
    :param df:
    :param filter_expr_list: list of filter expressions that return a boolean ANDed together
    :return: dataframe filtered down based on the filters
    """
    return filter_op(df, filter_expr_list)


def filter_or(df: DataFrame, filter_expr_list: list = ["1=1"]) -> DataFrame:
    """
    applies a union of boolean filter expressions
    :param df:
    :param filter_expr_list: list of filter expressions that return boolean Union-ed together
    :return: dataframe filtered down based on the filters
    """
    return filter_op(df, filter_expr_list, 'OR')


def gen_df_column_list(df):
    """
    not used for now
    :param df:
    :return:
    """
    return df.schema.fieldNames()


def look_up(df: DataFrame, lookup_df: DataFrame, ref_index: int,
            reference_column: str,
            new_column: str,
            lookup_type: str = "value") -> DataFrame:
    """
    # we should have a key for ref instead of index
    :param df:
    :param lookup_df:
    :param ref_index:
    :param reference_column:
    :param new_column:
    :param lookup_type:
    :return:
    """

    filtered_lookup_df = lookup_df.filter(f"step={ref_index}").select("x", "y")
    df_ref = df.join(filtered_lookup_df,
                     col(f"`{reference_column}`") == filtered_lookup_df.x, "left_outer")\
        .drop("x")
    if lookup_type == "value":
        return df_ref.withColumnRenamed("y", new_column)
    elif lookup_type == "cells":
        # here we generate the list of columns we would map to column values
        # using toPandas and convert to list... may be faster
        schema_columns = list(df_ref.filter("y is not null").select("y").distinct().toPandas()['y'])
        # schema_columns = df_ref.filter("y is not null").select("y").distinct().rdd.map(lambda r: r[0]).collect()

        # would be nice to really map the value to a col(value) and not use coalesce
        return df_ref.withColumn(new_column, F.coalesce(*[F.when(col("y") != f"{c}", lit(None))
                                                        .otherwise(df_ref[f"`{c}`"]) for c in schema_columns]))\
            .drop("y")
        # return df_ref.withColumn(new_column, df_ref[f"`{df_ref.y}`"]).drop("y")
        # return df_ref.select(F.coalesce(*[F.when(df_ref.y == c, df_ref[c]).otherwise(None) for c in col_seq]))
    else:
        # nothing to do
        return df


# needs work as this is really 2 dataframes
def add_columns_lookup(df: DataFrame, mapping_dict: dict):
    """
    Note todo the lookup file path should be configuration-based?
    otherwise we need it as an independent parameter as it does not make sense to read it for every new column
    as we do not see the case for independent lookup tables
    :param df:
    :param mapping_dict:
    :return:
    """
    res_df = df
    for c in mapping_dict:
        params = mapping_dict[c]
        l_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(params[0])
        ref_index = params[1]
        ref_column = params[2]
        # todo make it more robust
        if len(params) == 3:
            res_df = look_up(res_df, l_df, ref_index, ref_column, c)
        else:
            res_df = look_up(res_df, l_df, ref_index, ref_column, c, params[3])
    return res_df


# this class may be deprecated if we end up having 2 independent code lines (scala and python)
# class DfExtensions(DataFrame):
class DfExtensions:
    """
    set of transforms that invoke pystitchr extensions
    wrapper around the scala implementation and interfaces
    """

    # from pyspark DataFrame ...
    def __init__(self, jdf, sql_ctx):
        # DataFrame(jdf, sql_ctx)
        self._jdf = jdf
        self.sql_ctx = sql_ctx
        self._sc = sql_ctx and sql_ctx._sc
        self.is_cached = False
        self._schema = None  # initialized lazily
        self._lazy_rdd = None
        # Check whether _repr_html is supported or not, we use it to avoid calling _jdf twice
        # by __repr__ and _repr_html_ while eager evaluation opened.
        self._support_repr_html = False
        # assert isinstance(df, object)
        # DataFrame(df)

    def left_diff_schemas(self, right_df: DataFrame) -> list:
        """
        still is done inside pyspark... need to modify to invoke the gateway wrapper
        :param right_df:
        :return:
        """
        left_columns_set = set(self.df.schema.names)
        right_columns_set = set(right_df.schema.names)
        # warning: some columns are not in the list... maybe throw a warning error?
        return list(left_columns_set - right_columns_set)

    def right_diff_schemas(self, right_df: DataFrame) -> list:
        """
        still is done inside pyspark... need to modify to invoke the gateway wrapper
        :param right_df:
        :return:
        """
        left_columns_set = set(self.df.schema.names)
        right_columns_set = set(right_df.schema.names)
        # warning: some columns are not in the list... maybe throw a warning error?
        return list(right_columns_set - left_columns_set)

    def drop_columns(self, drop_columns_list: list):
        """

        :param drop_columns_list:
        :return:
        """
        df_columns_set = set(self._jdf.schema.names)
        # warning: some columns are not in the list... maybe throw a warning error?
        cols_that_donot_exist = set(drop_columns_list) - df_columns_set
        # get the actual list of columns to drop
        columns2remove = list(set(drop_columns_list) - cols_that_donot_exist)
        # drop and return
        # return self._jdf.drop(*columns2remove)
        # testing 4/9/21
        return DfExtensions(self._jdf.drop(*columns2remove))

    def rename_columns(self, rename_mapping_dict: dict):
        """
        :param rename_mapping_dict:
        :return: DataFrame
        Takes a dictionary of columns to be renamed and returns a converted dataframe
        Uses the thin wrapper around spark scala with Py4J
        """
        # return DfExtensions(self.sql_ctx._jvm.com.pystitchr.extensions.transform.Dataframe.renameColumns(
        #    _dict_to_scala_map(self._sc, rename_mapping_dict)), self._jdf, self.sql_ctx)
        # return self.sql_ctx._jvm.com.pystitchr.extensions.transform.Dataframe.renameColumns(_dict_to_scala_map(self._sc, rename_mapping_dict), self._jdf)
        # return DfExtensions(self.sql_ctx._jvm.com.pystitchr.extensions.transform.Dataframe.renameColumns(_dict_to_scala_map(self._sc, rename_mapping_dict)), self.sql_ctx)
        return DfExtensions(self._sc._jvm.com.stitchr.extensions.transform.Df.renameColumns(
            _dict_to_scala_map(self._sc, rename_mapping_dict))(self._jdf), self._sc)

    def rename_column(self, existing: str, new: str):
        """Returns a new `DataFrame` by renaming an `existing` column to `new` column.
        This is a no-op if the source schema does not contain the given `existing` column name.

        :param existing: string, name of the existing column to rename.
        :param new: string, new name of the column.

        >>> example
        df.withColumnRenamed('age', 'age2').collect()
        [Row(age2=2, name=u'Alice'), Row(age2=5, name=u'Bob')]
        """
        # either calls work...
        #
        # return DfExtensions(self._jdf.withColumnRenamed(existing, new), self.sql_ctx)
        return DataFrame(self._jdf.withColumnRenamed(existing, new), self.sql_ctx)


def _dict_to_scala_map(sc, jm):
    """
    Convert a dict into a JVM Map.
    """
    return sc._jvm.PythonUtils.toScalaMap(jm)


def transform0(self, f):
    """
    pyspark does not have a transform before version 3... we need to add one to DataFrame.
    This is based on https://mungingdata.com/pyspark/chaining-dataframe-transformations/
    """
    return f(self)


def run_pipeline(input_df: DataFrame, pipeline: dict, logging_level: str = 'ERROR') -> DataFrame:
    # ToDo: control logging level globally from outside
    logging.basicConfig(level=logging_level)
    # don't want to modify the source so we assign it
    df_p = input_df
    steps = pipeline
    log.info(f"number of steps is {len(steps)}")
    for step in steps:
        log.info(steps[step])
        key = list(steps[step].keys())[0]
        params = steps[step][key]
        log.info(f"step is {step}, transform is {key} with attributes {params}")
        method_to_call = getattr(dft, key)
        df_p = df_p.transform(lambda df: method_to_call(df, params))
        # change to log if we want info
        # print(f"root level logging is {log.root.level}")
        # TODO: NH need to debug as this goes through log4j and is kind of messy
        # if logging.DEBUG >= log.root.level:
        # logs even if we use warn or higher so commented out
        # log.info(df_p.printSchema())
    return df_p


def _test():
    """
    test code
    :return:
    """
    #import pystitchr.base.tests.test_df_transforms
    #import pystitchr.base.df_transforms
    import unittest
    # ...
    #t = pystitchr.base.tests.test_df_transforms.TestTranformMethods()
    # ToDo need to work on this
    # t.test_right_diff_schemas()


DataFrame.transform0 = transform0

if __name__ == "__main__":
    print('running tests \n')
    _test()
