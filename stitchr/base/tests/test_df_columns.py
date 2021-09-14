"""
stitchr_extensions tests

"""
import unittest
from resources import data
from pyspark.sql.functions import when, col
from stitchr.base.df_columns import *

class TestColumnMethods(unittest.TestCase):

    def test_get_valid_numeric(self):
        test_df: DataFrame = data.test_df_float
        df, cnt, _ = get_valid_numeric(test_df, "string_value")
        # this is used t really check if the cast would work
        df_transformed = df.withColumn("str_2_double", when(df.is_valid_numeric == True,  df.string_value)
                                       .otherwise(0).cast("float"))
        df_transformed.show()
        df_transformed.printSchema()
        self.assertEqual(cnt, 2)

    def test_cast_to_timestamp_default(self):
        test_df: DataFrame = data.test_df_ts
        df, p = cast_to_timestamp(test_df, "string_value")
        df.show()
        df.printSchema()
        self.assertEqual(p, 0.5)

    def test_cast_to_timestamp_exact(self):
        test_df: DataFrame = data.test_df_ts
        df, p = cast_to_timestamp(test_df, "string_value", "yyyy-MM-dd HH:mm:ss")
        df.show()
        df.printSchema()
        self.assertEqual(df.filter("string_value_ts is Null").count(), 4)

    def test_get_null_stats(self):
        test_df: DataFrame = data.test_df_ts
        df_ts, _ = cast_to_timestamp(test_df, "string_value")
        # null_stats_df = df_ts.transform(get_null_stats("string_value_ts"))
        null_stats_df = get_null_stats(df_ts, ["string_value_ts"])
        null_stats_df.show()
        c = null_stats_df.filter(col("string_value_ts_null") == 1).count()
        # print(c)
        self.assertEqual(c, 3)


if __name__ == '__main__':
    unittest.main()

