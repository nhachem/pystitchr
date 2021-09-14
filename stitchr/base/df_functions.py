"""
pyspark

from stitchr_extensions.df_transforms import *

"""

# import os
# from pyspark.sql.functions import concat_ws, collect_list
# import typing

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import col, concat, lit, when
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f

# import typing_extensions

# import sys
from random import choice
from string import ascii_letters
import re
import random
from random import Random
from random import choice
from string import ascii_letters, digits
from pyspark.sql.functions import pow, lit, col, floor, ceil, udf, round
from pyspark.sql.types import StringType, LongType, FloatType
import os
import sys
import json

from typing import List

spark = (pyspark.sql.SparkSession.builder.getOrCreate())
spark.sparkContext.setLogLevel('WARN')


@udf(returnType=StringType())
def get_random_alphanumeric(length: int, rand: int) -> str :
    # Random string with the combination of lower and upper case
    rnd = Random(rand)
    alphanumeric = ascii_letters + digits
    result_str = ''.join(rnd.choice(alphanumeric)
                         for _ in range(length))
    return result_str


spark.udf.register("get_random_alphanumeric", get_random_alphanumeric)


@udf(returnType=StringType())
def match_string_length(s: str, length: int) -> str:
    """
    if length of col value matches then return value else null
    can be done with sql CASE WHEN...
    :param s:
    :param length:
    :return:
    """
    if len(s) == length:
        return s
    else:
        return None


spark.udf.register("match_string_length", match_string_length)


@udf(returnType=StringType())
def extract_between(s: str, regex_str: str) -> str:
    """
    # this UDF is used to extract the x/y arrays from the json strings
    :param s:
    :param regex_str:
    :return:
    """
    # maybe this will work
    regex_str.replace(s, "\n", "", 1)
    # or better with regex?
    import re
    return re.sub(regex_str, "", s)


spark.udf.register("extract_between", extract_between)


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


DataFrame.transform0 = transform0

if __name__ == "__main__":
    print('running tests \n')
    _test()
