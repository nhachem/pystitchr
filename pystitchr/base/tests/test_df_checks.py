"""
df_checks tests

"""
import unittest

import xmlrunner


class TestCheckMethods(unittest.TestCase):

    """
    def test_get_valid_numeric(self):
        test_df: DataFrame = data.test_df_float
        df, cnt, _ = get_valid_numeric(test_df, "string_value")
        # this is used t really check if the cast would work
        df_transformed = df.withColumn("str_2_double", when(df.is_valid_numeric == True,  df.string_value)
                                       .otherwise(0).cast("float"))
        df_transformed.show()
        df_transformed.printSchema()
        self.assertEqual(cnt, 2)
    """


if __name__ == "__main__":
    unittest.main(testRunner=xmlrunner.XMLTestRunner(output="test-reports"))
