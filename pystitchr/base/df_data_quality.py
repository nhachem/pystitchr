"""
pyspark

from pystitchr.base.df_data_quality import *

"""


# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pystitchr.util.spark_utils import dynamic_join

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')


def add_dup_flag(df: DataFrame, unique_key: list) -> DataFrame:
    """
    NH: this can be done with sql by creating a temp view
    This approach works for any schema
    """
    r_unique_key = [f"r_{k}" for k in unique_key ]
    # print(r_unique_key)
    _dup_keys = df.get_dup(unique_key) \
                  .add_columns({'concat_pks': f"concat({','.join(unique_key)})",}) \
                  .drop('_cnt_unique')
    # can use the rename_columns instead
    for k in unique_key:
        _dup_keys = _dup_keys.withColumnRenamed(k, f"r_{k}")
    # _dup_keys.printSchema()
    _flagged_df = dynamic_join(df, _dup_keys, unique_key, r_unique_key)
    for k in unique_key:
        _flagged_df = _flagged_df.drop(f"r_{k}")

    return _flagged_df.add_columns({"dup_flag": "case when concat_pks is null then False else True end"}) \
                      .drop("concat_pks")

