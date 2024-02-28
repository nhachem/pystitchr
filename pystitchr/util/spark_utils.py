from pyspark.sql import Column, DataFrame, SparkSession, Row
from pyspark.sql.functions import col, trim, replace, lit

import functools
import operator
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()


def transform0(self, f):
    """
    pyspark does not have a transform before version 3... we need to add one to DataFrame.
    This is based on https://mungingdata.com/pyspark/chaining-dataframe-transformations/
    """
    return f(self)


def logical_parse(query: str) -> str:
    """
    returns the logical plan of the query.
    used mostly to get the dependt objcts ina query
    @param q:
    @type q:
    @return:
    @rtype:
    """
    return spark._jvm.org.apache.spark.sql.execution.SparkSqlParser().parsePlan(query).toString()


def get_empty_df(column_list: list = []) -> DataFrame:
    # columns = StructType([])
    columns = StructType([StructField(c, StringType()) for c in column_list])
    return spark.createDataFrame(data = [], schema = columns)


def display_df(df: DataFrame):
    df.show(truncate=False)
    return df


DataFrame.display=display_df


def display_sql(sql_str: str):
    spark.sql(sql_str).show(truncate=False)


def get_dependency_objects(query: str, object_ref: str, debug: bool = False) -> DataFrame:
    """
    based on the logical parsed string lines we extract unresolved relations and deduct the CTEs.
    Those would be the objects that a query depends_on
    we use DF manipulations but probably better ot use pyton list and sets and perform the diff
    This will do for now unless we find performance issues
    @param query:
    @type query:
    @return:
    @rtype:
    """

    lp0 = logical_parse(query).splitlines()

    lp = [Row(lp0[i]) for i in range(0, len(lp0), 1)]
    if debug:
        for i in range(0, len(lp), 1):
            print(lp[i])

    all_unresolved = [Row(lp0[i]) for i in range(0, len(lp0), 1) if 'UnresolvedRelation' in lp0[i]]
    cte_objects = [Row(lp0[i]) for i in range(0, len(lp0), 1) if 'CTE' in lp0[i]]

    # NH: note that stripping the [] may not be the best approach but will work with this for now
    # also replace of +- and the 'Unresolved should be independent
    all_unresolved_df = (spark.createDataFrame(all_unresolved, ["logical_plan_line"])
                         .selectExpr("""trim(replace(replace(replace(replace(
                         logical_plan_line, "+- 'UnresolvedRelation", ''), 
                         ":", ''), '[]', ''), ', , false', '') ) 
                         as depends_on""")
                         .distinct().selectExpr("""replace(replace(depends_on, '[', ''), ']', '')as depends_on""")
                         )
    # all_unresolved_df.display()

    cte_df = get_empty_df(["cte_objects"])
    if len(cte_objects) > 0:
        _cte_df = ( spark.createDataFrame(cte_objects, ["logical_plan_line"])
                    .selectExpr("""trim(replace(replace(logical_Plan_line, "+-", ''), "CTE", '')) 
                                as cte_objects""")
                    .distinct()
                )
        # _cte_df.display()
        cte_df = (_cte_df.selectExpr("""explode(split(replace(replace(cte_objects, '[', ''), ']', ''), ',')) 
                                    as cte_objects""")
                  .selectExpr("trim(cte_objects) as cte_object")
                  )
    # else:
        # cte_df is empty
    # cte_df.display()
    depends_on_df =all_unresolved_df.exceptAll(cte_df).withColumn("object_ref", lit(object_ref))

    return depends_on_df


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

