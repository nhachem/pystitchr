"""
pyspark

from pystitchr.base.df_transforms import *

"""

# import os

import re
# import sys
from typing import List

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, concat, lit, when, explode, from_json, to_json, md5
# , posexplode, arrays_zip
from pyspark.sql.types import *
from pyspark.sql.window import Window

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

error_schema = StructType([StructField("function_called", StringType(), True),
                           StructField("error_message", StringType(), True)])


def generate_error_df(source: str, error_msg: str) -> DataFrame:
    """
    returns a dataframe with the error with schema (data, schema
    @param source:
    @type source:
    @param error_msg:
    @type error_msg:
    @return:
    @rtype:
    """
    error_msg = [(source, error_msg)]
    return spark.createDataFrame(data=error_msg, schema=error_schema)


def get_schema(df: DataFrame) -> DataFrame:
    """
    returns the schema of  dataframe as a dataframe
    @param df:
    @type df:
    @return:
    @rtype:
    """
    _df = spark.read.json(spark.sparkContext.parallelize([df.schema.json()]))
    return _df.withColumn("field", explode("fields")).drop("fields").select("field.*")


# using call by string name getattr()
def generate_schema_by_string(domain: str, columns: list, attributes_df: DataFrame):
    """
    generates the schema struct object using call by string name getattr()
    NH: to move to df_schema module?
    @param domain:
    @type domain:
    @param columns:
    @type columns:
    @param attributes_df:
    @type attributes_df:
    @return:
    @rtype:
    """
    import pyspark.sql.types as t
    filter_string = "','".join(columns)
    col_df: DataFrame = spark.createDataFrame(columns, StringType())

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
    """
    generates missing columns from a dataframe containing attributes and returns the list
    NH: to move to df_schema module?
    @param domain:
    @type domain:
    @param columns:
    @type columns:
    @param attributes_df:
    @type attributes_df:
    @return:
    @rtype:
    """
    # using call by string name getattr()
    col_df: DataFrame = spark.createDataFrame(columns, StringType())
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
    takes 2 dataframes (left and right) and
    returns a list of columns in left DataFrame set but not in the right DataFrame
    NH: to move to df_schema module?
    @param left_df:
    @type left_df:
    @param right_df:
    @type right_df:
    @return:
    @rtype:
    """
    left_columns_set = set(left_df.schema.fieldNames())
    right_columns_set = set(right_df.schema.fieldNames())
    # what is the toSet on python? .toSet
    # print(list(df_columns_set))
    # warn that some columns are not in the list... Or maybe throw an error?
    return list(left_columns_set - right_columns_set)


def right_diff_schemas(left_df: DataFrame, right_df: DataFrame) -> list:
    """
    takes 2 dataframes (left and right) and
    returns a list of columns in right DataFrame  but not in the left DataFrame
    NH: to move to df_schema module?
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
# look into panda equivalent or maybe koalas
def schema_diff(left_df: DataFrame, right_df: DataFrame):
    """
    returns the left and right schema difference
    NH: to move to df_schema module?
    @param left_df:
    @type left_df:
    @param right_df:
    @type right_df:
    @return:
    @rtype:
    """
    right_columns_set = set(right_df.schema)
    left_columns_set = set(left_df.schema)
    return ([left for left in left_df.schema if left not in right_columns_set],
            [right for right in right_df.schema if right not in left_columns_set]
            )


def fields_diff(left_df: DataFrame, right_df: DataFrame):
    """
    difference of field names (similar to schema_diff but only on names)
    NH: to move to df_schema module?
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


def noop(df: DataFrame, f: dict) -> DataFrame:
    # for some reason I needed a parameter... this needs debugging
    return df


def add_hash_value(
    df: DataFrame,
    value_column: str = 'message_value') -> DataFrame:
    '''
    This is generic enough but is used to adjust the extraction views to add  a row_hash value
    '''
    _df = df
    # NH: TODO we could use directly the value as we have a string value in place
    return _df.withColumn("_hashValue", md5(to_json(value_column)))


def _select_list(df: DataFrame, column_list: list) -> DataFrame:
    """
    strictly restrictive select list of columns. if any columns in column_list are not in schema it throws an error

    @param df:
    @type df:
    @param column_list:
    @type column_list:
    @return:
    @rtype:
    """
    not_in_schema = _not_in_schema(df, column_list)
    # maybe better to change to a try except or better setup app error trapping
    if len(not_in_schema) > 0:
        log.error(f"App Error: columns to select {not_in_schema} are not in the dataframe schema")
        return generate_error_df("_select_list",
                                 f"App Error: columns to select {not_in_schema} are not in the dataframe schema")
    cl: list = f"`{'`,`'.join(column_list)}`".split(',')
    return df.select(*cl)


def select_expr_columns(
    df: DataFrame,
    expression_list: list
    ) -> DataFrame:
    """
    expression_list is of the form [{"new_column": <col_name>, "expression": "<sql column expression>"}, ...]
        --> maps to expression as new_column
    is  useful to write inline udf transforms on columns and perform a one step select that are supported by UDFs
    """
    expr = [f"{exp['transform']} as {exp['new_column']}" for exp in expression_list]
    # expr.append("*")

    return df.selectExpr(*expr)


def select_list(df: DataFrame, column_list: list, strict: bool = True) -> DataFrame:
    """
    allows for permissive or restrictive select of columns from column_list.
    If restrictive it errors out of the select_list is not fully included in the schema of the dataframe df

    @param df:
    @param column_list:
    @param strict: default is True that is restrictive. False allows selecting the intersect of column_list
    and schema columns
    @return:
    """
    not_in_schema = _not_in_schema(df, column_list)
    # maybe better to change to a try except or better setup app error trapping
    if len(not_in_schema) > 0 and strict:
        log.error(f"App Error: columns to select {not_in_schema} are not in the dataframe schema")
        return generate_error_df("select_list",
                                 f"App Error: columns to select {not_in_schema} are not in the dataframe schema")
    in_schema = _in_schema(df, column_list)
    cl: list = f"`{'`,`'.join(in_schema)}`".split(',')
    return df.select(*cl)


def select_list_from(df: DataFrame, column_list: list) -> DataFrame:
    """
    permissive select_list. that is it returns the intersection of columns in
    the schema of df that are part of the column_list

    @param df:
    @param column_list:
    @return: a dataframe of the selected column_list
    """
    return select_list(df, column_list, False)


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
    removes any column in the drop_columns_list. skip any non-existing columns
    @param drop_columns_list:
    @param df:
    @return:
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


def _not_in_schema(df: DataFrame, column_list: list) -> list:
    """
    check to see if column_list is or is not in schema of df
    @param df: input DataFrame
    @param column_list: list of columns to check which ones are not in the schema of df
    @return: list of what is not in the schema of df. if none then all are in the schema
    """
    df_columns: list = df.schema.fieldNames()
    # check if any column to be renamed is non existent
    columns_set = set(column_list)
    schema_columns_set = set(df_columns)
    return list(columns_set - schema_columns_set)


def _in_schema(df: DataFrame, column_list: list) -> list:
    """
    check if columns in columns_list are in schema

    @param df: input DataFrame
    @param column_list:
    @return: returns the list in the same order of what is in the schema of df
    """
    return [c for c in column_list if c in df.schema.fieldNames()]


def rename_columns(df: DataFrame, rename_mapping_dict: dict, strict: bool = True) -> DataFrame:
    """
    Takes a dictionary of columns to be renamed and returns a converted dataframe.
    if strict then throws errors and exitess if any column is not in the schema
    else it renames all existing columns and skips the non existing ones

    @param df:
    @type df:
    @param rename_mapping_dict:
    @type rename_mapping_dict:
    @param strict:
    @type strict:
    @return:
    @rtype:
    """
    not_in_schema = _not_in_schema(df, rename_mapping_dict.keys())
    # maybe better to change to a try except or better setup app error trapping
    if len(not_in_schema) > 0 and strict:
        log.error(f"App Error: columns to rename {not_in_schema} are not in the dataframe schema")
        return generate_error_df("rename_columns",
                                 f"App Error: columns to rename {not_in_schema} are not in the dataframe schema")
    # we use sqlExpr to keep the schema during the rename process
    df_new_columns: list = [f"`{c}` as `{rename_mapping_dict[c]}`" if (c in rename_mapping_dict)
                            else f"`{c}`"
                            for c in df.schema.fieldNames()]
    # NH: this does not guarantee that we keep the schema types.
    # return df.toDF(*df_new_columns)
    # so we generated a select expression
    return df.selectExpr(*df_new_columns)


def rename_column(df: DataFrame, existing: str, new: str) -> DataFrame:
    """
    Returns a new :class:`DataFrame` by renaming an existing column.
    This is a no-op if schema doesn't contain the given column name.
    Effectively a wrapper over withColumnRenamed

    @param df:
    @type df: object
    @param existing: string, name of the existing column to rename.
    @type existing:
    @param new: string, new name of the column.
    @type new:
    @return:
    @rtype:
    """
    return df.withColumnRenamed(existing, new)


def map_columns(df: DataFrame, rename_mapping_dict: dict) -> DataFrame:
    """
    Takes a dictionary of columns to be renamed and returns a converted dataframe
    This function renames all existing columns and skips the non-existing ones
    @param df:
    @type df:
    @param rename_mapping_dict:
    @type rename_mapping_dict:
    @return:
    @rtype:
    """
    return rename_columns(df, rename_mapping_dict, False)


def _rename_4_parquet(df: DataFrame) -> DataFrame:
    """
    rename all columns of the dataFrame so that we can save as a Parquet file
    uses default delimiter of __
    NH: to extend by externalizing the regex and delimiter. Would make it more generic
    @param df:
    @type df:
    @return:
    @rtype:
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


def rename_4_parquet(df: DataFrame, dummy_list: list = [None]) -> DataFrame:
    """
    same as _rename_4_parquet. dummy_list is unsued but is needed to work with the framework
    rename all columns of the dataFrame so that we can save as a Parquet file
    uses default delimiter of __
    @param df:
    @type df:
    @param dummy_list:
    @type dummy_list:
    @return:
    @rtype:
    """
    return _rename_4_parquet(df)


def _unpivot(df: DataFrame, unpivot_keys: list,
             unpivot_column_list: list,
             key_column: str = "key_column",
             value_column: str = "value") -> DataFrame:
    """

    @param df:
    @type df:
    @param unpivot_keys:
    @type unpivot_keys:
    @param unpivot_column_list:
    @type unpivot_column_list:
    @param key_column:
    @type key_column:
    @param value_column:
    @type value_column:
    @return:
    @rtype:
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


def unpivot(df: DataFrame, params_dict: dict = {}) -> DataFrame:
    """

    @param df:
    @type df:
    @param params_dict:
    @type params_dict:
    @return:
    @rtype:
    """
    _key_column = params_dict.get("key_column", 'key_column')
    _value_column = params_dict.get("value_column", 'value')
    _keys: list = params_dict.get("keys", [])
    _unpivot_list = params_dict.get("unpivot_columns", []) # NH: this technically should not happen
    if len(_keys) == 0:
        _keys = list(set(df.schema.fieldNames()).difference(set(_unpivot_list)))
    return _unpivot(df, _keys, _unpivot_list, _key_column, _value_column)


def unpivot_all(df: str, pk_list: list) -> DataFrame:
    """

    @param df:
    @type df:
    @param pk_list:
    @type pk_list:
    @return:
    @rtype:
    """
    all_columns_set = set(df.columns)
    _keys = pk_list
    _unpivot_list = list(all_columns_set - set(_keys))
    # unpivot_spec = {"keys": _keys,
    #                 "unpivot_columns": _unpivot_List,
    #                 "key_column": "property_key", "value_column": "property_value"
    #                 }
    # NH can use this or the simpler below
    # return df.unpivot(unpivot_spec)
    return _unpivot(df, _keys, _unpivot_list, "property_key", "property_value")


def _flatten_experimental(data_frame: DataFrame) -> DataFrame:
    """
    NH: Experimental

    @param data_frame:
    @type data_frame:
    @return:
    @rtype:
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
            return _flatten_experimental(data_frame.selectExpr(*field_names_to_select))
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

            return _flatten_experimental(df_mapped)
            # return data_frame
        elif isinstance(field_type, StructType):
            child_fieldnames = [f"{field_name}.{child.name}" for child in field_type]
            new_fieldnames = [fn for fn in field_names if fn != field_name] + child_fieldnames
            renamed_cols = [col(x).alias(x.replace(".", "__")) for x in new_fieldnames]
            # exploded_df = exploded_df.select(*renamed_cols)
            # print(len(exploded_df.schema.fieldNames()))
            # return flatten0(exploded_df)
            return _flatten_experimental(data_frame.select(*renamed_cols))
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


def _flatten(data_frame: DataFrame, mode: str = 'full', delimiter: str = '__') -> DataFrame:
    """
    # NH: need to document
    # cases are full means full explode.
    #           struct only structs,
    #           map will unwind the maps as a pivot and a group by + structs
    #           array will effectively do struct and arrays only (with explode not positional)

    @param data_frame:
    @type data_frame:
    @param mode:
    @type mode:
    @param delimiter:
    @type delimiter:
    @return:
    @rtype:
    """
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
            return _flatten(data_frame.selectExpr(*field_names_to_select), mode, delimiter)
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
            return _flatten(df_flat, mode, delimiter)
        elif isinstance(field_type, StructType):
            child_fieldnames = [f"{field_name}.{child.name}" for child in field_type]
            new_fieldnames = [fn for fn in field_names if fn != field_name] + child_fieldnames
            renamed_cols = [col(x).alias(x.replace(".", delimiter)) for x in new_fieldnames]
            # exploded_df = exploded_df.select(*renamed_cols)
            # print(len(exploded_df.schema.fieldNames()))
            # return flatten0(exploded_df)
            return _flatten(data_frame.select(*renamed_cols), mode, delimiter)
    # print(f'schema size is {len(data_frame.schema.fieldNames())}')
    return data_frame


def flatten(df: DataFrame, dummy_param_list: list = [None]) -> DataFrame:
    """

    @param df:
    @type df:
    @param dummy_param_list:
    @type dummy_param_list:
    @return:
    @rtype:
    """
    return _flatten(df)


# NH flatten_array, flatten_struct, flatten_map are experimental


def flatten_struct(df: DataFrame, dummy_param_list: list = [None]) -> DataFrame:
    """

    @param df:
    @type df:
    @param dummy_param_list:
    @type dummy_param_list:
    @return:
    @rtype:
    """
    return _flatten(df, mode='struct')


def flatten_array(df: DataFrame, dummy_param_list: list = [None]) -> DataFrame:
    """

    @param df:
    @type df:
    @param dummy_param_list:
    @type dummy_param_list:
    @return:
    @rtype:
    """
    return flatten(df, mode='array')


def flatten_map(df: DataFrame, dummy_param_list: list = [None]) -> DataFrame:
    """

    @param df:
    @type df:
    @param dummy_param_list:
    @type dummy_param_list:
    @return:
    @rtype:
    """
    return _flatten(df, mode='map')


def add_windowed_column(df: DataFrame, column_name: str, source_column: str,
                        window_function: str, partition_by_list: list, order_by_list) -> DataFrame:
    """
    adds a column derived by a window function
    @param df:
    @type df:
    @param column_name:
    @type column_name:
    @param source_column:
    @type source_column:
    @param window_function:
    @type window_function:
    @param partition_by_list:
    @type partition_by_list:
    @param order_by_list:
    @type order_by_list:
    @return:
    @rtype:
    """
    window_spec = Window.partitionBy(*partition_by_list).orderBy(*order_by_list)
    fn = getattr(pyspark.sql.functions, window_function)
    return df.withColumn(column_name, fn(col(source_column)).over(window_spec))


def _add_columns(df: DataFrame, new_columns_mapping_dict: dict, strict: bool = True) -> DataFrame:
    """
    This takes a dict of (new_column: str -> sql_expr: str)
    NOTICE: the code uses a | delimiter to split a string.
    If a special function uses a delimiter then this function will fail
    ToDo: NH. If the | is an issue, we may use a default delimiter of | but allow to pass different ones as needed
    the approach would be the most efficient as the transform expressions may be quite complex.
    The implication is that UDFs are registered

    @param df:
    @type df:
    @param new_columns_mapping_dict:
    @type new_columns_mapping_dict:
    @param strict:
    @type strict:
    @return:
    @rtype:
    """
    # ToDo: check also for all columns used in the functions?! This would not be trivial
    df_out = df
    step = new_columns_mapping_dict
    in_schema = _in_schema(df, new_columns_mapping_dict.keys())
    not_in_schema = _not_in_schema(df, new_columns_mapping_dict.keys())
    # maybe better to change to a try except or better setup app error trapping
    if len(in_schema) > 0 and strict:
        log.error(f"App Error: columns to add {in_schema} are already in the dataframe schema")
        return generate_error_df("add_columns",
                                 f"App Error: columns to add {not_in_schema} are already in the dataframe schema")
    else:
        if len(in_schema) > 0:
            log.warn(f"App warning: columns to add {in_schema} will be skipped")

        # if we get here we need to add only non-existing columns
        # we also add `` around column names
        _sql_expr = '|'.join([f"{step[c]} as `{c}`" if (c in not_in_schema) else 'NA' for c in step]).split('|')
        sql_expr = list(filter(lambda l: l != 'NA', _sql_expr))
        # if we have nothing to add then return the input dataframe
        if len(sql_expr) > 0:
            df_out = df.selectExpr("*", *sql_expr)
        else:
            log.warn(f"App warning: no new columns added")
    return df_out


def add_columns(df: DataFrame, new_columns_mapping_dict: dict) -> DataFrame:
    """
     We are assuming that adding columns filters out any columns that already exists

    @param df:
    @type df:
    @param new_columns_mapping_dict:
    @type new_columns_mapping_dict:
    @return:
    @rtype:
    """
    return _add_columns(df, new_columns_mapping_dict, False)


def add_columns_strict(df: DataFrame, new_columns_mapping_dict: dict) -> DataFrame:
    """
     We are assuming that adding columns filters out any columns that already exists
    @param df:
    @type df:
    @param new_columns_mapping_dict:
    @type new_columns_mapping_dict:
    @return:
    @rtype:
    """
    return _add_columns(df, new_columns_mapping_dict, True)


def add_column(df: DataFrame, new_column: str, transform):
    """
    NH: added here for coverage. But it will rarely be  used
    @param df:
    @type df:
    @param new_column:
    @type new_column:
    @param transform:
    @type transform:
    @return:
    @rtype:
    """
    # Assuming all columns are correct... But we better add a check step similar to the drop columns function
    return df.withColumn(new_column, transform)


def gen_pivot_sql(df: DataFrame, pivoted_columns_list: list = [None]
                  , key_column: str = 'key_column'
                  , value_column: str = 'value'
                  , fn: str = "max"
                  , hive_view: str = None) -> str:
    """
    generates the sql string corresponding to the sql pivot functions

    @param df:
    @type df:
    @param pivoted_columns_list:
    @type pivoted_columns_list:
    @param key_column:
    @type key_column:
    @param value_column:
    @type value_column:
    @param fn:
    @type fn:
    @param hive_view:
    @type hive_view:
    @return:
    @rtype:
    """
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
    if hive_view is None:
        df.createOrReplaceTempView("_tmp")
        return f"SELECT * FROM (SELECT * FROM _tmp) PIVOT ( {fn}({value_column}) FOR {key_column} in ( '{column_list}' ))"
    else:
        # NH: saveAsTable may not work in default spark pre 3.0?
        # NH: seems if we use spark on a laptop we can't reuse the storage
        spark.sql(f"drop table if exists {hive_view}_")
        df.write.mode("overwrite").saveAsTable(f"{hive_view}_")
        return f"SELECT * FROM (SELECT * FROM {hive_view}_) PIVOT ( {fn}({value_column}) FOR {key_column} in ( '{column_list}' ))"


def _pivot(df: DataFrame, pivoted_columns_list: list = [None]
           , key_column: str = 'key_column'
           , value_column: str = 'value'
           , fn: str = "max"
           , hive_view: str = None) -> DataFrame:
    """

    @param df:
    @type df:
    @param pivoted_columns_list:
    @type pivoted_columns_list:
    @param key_column:
    @type key_column:
    @param value_column:
    @type value_column:
    @param fn:
    @type fn:
    @param hive_view:
    @type hive_view:
    @return:
    @rtype:
    """
    q = gen_pivot_sql(df, pivoted_columns_list, key_column, value_column, fn, hive_view)
    if hive_view is None:
        return spark.sql(q)
    else:
        # NH: may add support for temp views later
        spark.sql(f""" create or replace view {hive_view} as {q}""")
        return spark.table(hive_view)
    # return spark.sql(q)


def pivot(df: DataFrame, params_dict: dict = {}) -> DataFrame:
    """

    @param df:
    @type df:
    @param params_dict:
    @type params_dict:
    @return:
    @rtype:
    """
    if params_dict is None:
        return _pivot(df, [])
    key_column = params_dict.get("key_column", 'key_column')
    value_column = params_dict.get("value_column", 'value')
    fn = params_dict.get("fn", 'max')
    hive_view = params_dict.get("hive_view", None)
    pivoted_columns_list: list = params_dict.get("pivot_values", [])
    return _pivot(df, pivoted_columns_list, key_column, value_column, fn, hive_view)


def filter_op(df: DataFrame, filter_expr_list: list = ["1=1"], operation: str = 'AND') -> DataFrame:
    """
    applies a composition of boolean expressions that are either ORed or ANDed together
    @param df:
    @type df:
    @param filter_expr_list: list of filter expressions that return boolean and stitched with the operation (AND or OR)
    @type filter_expr_list:
    @param operation: default to AND
    @type operation:
    @return: dataframe filtered by row based on the filters
    @rtype:
    """
    operation_wrapper = f") {operation} ("
    query_filter = f"({operation_wrapper.join(map(str, filter_expr_list))})"
    return df.filter(query_filter)


def filter_and(df: DataFrame, filter_expr_list: list = ["1=1"]) -> DataFrame:
    """
    applies the AND of filter expressions
    @param df:
    @type df:
    @param filter_expr_list: list of filter expressions that return a boolean ANDed together
    @type filter_expr_list:
    @return: dataframe filtered down based on the filters
    @rtype:
    """
    return filter_op(df, filter_expr_list)


def filter_or(df: DataFrame, filter_expr_list: list = ["1=1"]) -> DataFrame:
    """
    applies a union of boolean filter expressions
    @param df:
    @type df:
    @param filter_expr_list: list of filter expressions that return a boolean ANDed together
    @type filter_expr_list:
    @return: dataframe filtered down based on the filters
    @rtype:
    """
    return filter_op(df, filter_expr_list, 'OR')


def gen_df_column_list(df):
    """
    NH: not used for now
    @param df:
    @type df:
    @return:
    @rtype:
    """
    return df.schema.fieldNames()


def look_up(df: DataFrame, lookup_df: DataFrame, ref_index: int,
            reference_column: str,
            new_column: str,
            lookup_type: str = "value") -> DataFrame:
    """
    # NH: note we should have a key for ref instead of index
    @param df:
    @type df:
    @param lookup_df:
    @type lookup_df:
    @param ref_index:
    @type ref_index:
    @param reference_column:
    @type reference_column:
    @param new_column:
    @type new_column:
    @param lookup_type:
    @type lookup_type:
    @return:
    @rtype:
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


def translate_column_values(
    df: DataFrame,
    translation_df: DataFrame,
    source_fk_column: str,
    lookup_column: str,
    translation_column: str,
    join_type: str = "leftouter",
) -> DataFrame:
    """
    replaces the source_fk_column values with the ones from a translation_column based on a "join/lookup" column.
    It is expected that the translation_df has only 2 columns (lookup_column and translation_column).
    We assert that by selecting only the needed columns prior to the join-lookup
    """
    translation_df = translation_df.select(lookup_column, translation_column)
    return (
        df.join(translation_df, df[source_fk_column] == translation_df[lookup_column], join_type)
        .withColumn(source_fk_column, col(translation_column))
        .drop(lookup_column)
        .drop(translation_column)
    )


def union_by_name(empty_df: DataFrame, tables_for_union: list, verbose: bool = False) -> DataFrame:
    """
    Makes a union by name allowing missing columns one by one from a list of tables_for_union

    Args:
        empty_df: expect an empty df to start the union
        tables_for_union: full names of tables, e.g.: ["cc_raw_stage.sfdc_provider", "cc_raw_stage.hco_provider"]
        verbose: prints table name with the line counts

    Returns:
        dataframe that comprises all tables from tables_for_union
    """
    df = empty_df
    for table_name in tables_for_union:
        table = spark.table(table_name)
        if verbose:
            print(f"{table_name:<50}\t{table.count():>10_}")
        df = df.unionByName(table, allowMissingColumns=True)
    return df


def add_columns_lookup(df: DataFrame, mapping_dict: dict):
    """
    # needs work as this is really 2 dataframes
    Note todo the lookup file path should be configuration-based?
    otherwise we need it as an independent parameter as it does not make sense to read it for every new column
    as we do not see the case for independent lookup tables

    @param df:
    @type df:
    @param mapping_dict:
    @type mapping_dict:
    @return:
    @rtype:
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


def add_column_values_hash(df: DataFrame, exclude_columns: list = None) -> DataFrame:
    """add a hash column out of all column values to detect changes on merge by the hash value"""
    columns = sorted(df.columns.copy())  # preserve the order of columns to keep the hash value permutation independent
    if not exclude_columns:
        exclude_columns = list()
    compute_expressions = [
        f"if({field} is null, '''', cast({field} as string))" for field in columns if field not in exclude_columns
    ]

    return df.add_columns({"column_values_hash": f"md5(concat({','.join(compute_expressions)}))"})


def cast2json(
        df: DataFrame,
        column_name: str,
        new_column: str = None,
        db_schema: StructType = None) -> DataFrame:
    """
    takes a dataframe and parses the column_name into new_column or replaces it
    by its equivalent Struct if new_column is None.
    Note that providing the schema as input makes it much faster else the code parses every string
    in the column_name to assemble the covering schema
    Note: do not use in a streaming DF. Use it after the streaming step
    """
    out_column = column_name if new_column is None else new_column
    if db_schema is None:
        _schema = spark.read.json(df.select(column_name).rdd.map(lambda row: row[column_name])).schema
    else:
        _schema = db_schema

    return df.withColumn(out_column, from_json(col(column_name), _schema))


def _test():
    """
    test code
    @return:
    """
    #import pystitchr.base.tests.test_df_transforms
    #import pystitchr.base.df_transforms
    import unittest
    # ...
    #t = pystitchr.base.tests.test_df_transforms.TestTranformMethods()
    # ToDo need to work on this
    # t.test_right_diff_schemas()


if __name__ == "__main__":
    print('running tests \n')
    _test()
