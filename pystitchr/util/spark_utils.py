from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col
import functools
import operator

spark = SparkSession.builder.getOrCreate()


def transform0(self, f):
    """
    pyspark does not have a transform before version 3... we need to add one to DataFrame.
    This is based on https://mungingdata.com/pyspark/chaining-dataframe-transformations/
    """
    return f(self)


def build_disjunctive_join_expression(
        left_columns_string: str,
        right_columns_string: str) -> Column:
    """

        @param left_columns_string:
        @type left_columns_string:
        @param right_columns_string:
        @type right_columns_string:
        @return:
        @rtype:
    """
    left_array: list = left_columns_string.split(",")
    right_array: list = right_columns_string.split(",")
    return functools.reduce(
        operator.or_, map(lambda x: col("l." + x[0]) == col("r." + x[1]), zip(left_array, right_array)))


def build_conjunctive_join_expression(
        left_columns_list: list,
        right_columns_list: list) -> Column:
    """

        @param left_columns_list:
        @type left_columns_list:
        @param right_columns_list:
        @type right_columns_list:
        @return:
        @rtype:
        """
    return functools.reduce(
        operator.and_, map(lambda x: col("l." + x[0]) == col("r." + x[1]), zip(left_columns_list, right_columns_list)))


def _build_conjunctive_join_expression(
        left_columns_string: str,
        right_columns_string: str) -> Column:
    left_array: list = left_columns_string.split(",")
    right_array: list = right_columns_string.split(",")
    return functools.reduce(
        operator.and_, map(lambda x: col("l." + x[0]) == col("r." + x[1]), zip(left_array, right_array)))


def build_conjunctive_join_expression(
        left_columns_list: list,
        right_columns_list: list) -> Column:
    return functools.reduce(
        operator.and_, map(lambda x: col("l." + x[0]) == col("r." + x[1]), zip(left_columns_list, right_columns_list)))


def dynamic_join(
        self: DataFrame,
        right_df: DataFrame,
        left_columns_string: str,
        right_columns_string: str,
        join_type: str = "leftouter"
      ) -> DataFrame:
    """
    Todo NH Untested under dev
    @param self:
    @type self: DataFrame
    @param right_df:
    @type right_df: DataFrame
    @param left_columns_string:
    @type left_columns_string:
    @param right_columns_string:
    @type right_columns_string:
    @param join_type:
    @type join_type:
    @return:
    @rtype:
    """
    join_expression = build_conjunctive_join_expression(left_columns_string, right_columns_string)
    return self.alias("l").join(right_df.alias("r"), join_expression, join_type)

def dynamic_conjunctive_join(
        self: DataFrame,
        right_df: DataFrame,
        left_columns_string: str,
        right_columns_string: str,
        join_type: str = "leftouter"
      ) -> DataFrame:
    dynamic_join( self, right_df, left_columns_string, right_columns_string, join_type)


def dynamic_disjunctive_join(
        self: DataFrame,
        right_df: DataFrame,
        left_columns_string: str,
        right_columns_string: str,
        join_type: str = "leftouter"
      ) -> DataFrame:
    """
    Todo NH Untested under dev
    @param self:
    @type self: DataFrame
    @param right_df:
    @type right_df: DataFrame
    @param left_columns_string:
    @type left_columns_string:
    @param right_columns_string:
    @type right_columns_string:
    @param join_type:
    @type join_type:
    @return:
    @rtype:
    """
    join_expression = build_disjunctive_join_expression(left_columns_string, right_columns_string)
    return self.alias("l").join(right_df.alias("r"), join_expression, join_type)


# toDo: cast to json type
#  def cast2json
