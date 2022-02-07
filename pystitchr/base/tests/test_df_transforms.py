"""
pystitchr tests

"""
import unittest
from resources import data
# NH: switched to monkey patched functions (through __init__ from pystitchr main module
from pystitchr import *
# from pystitchr.base.df_transforms import *
import os


class TestTranformMethods(unittest.TestCase):

    def test_left_diff_schemas(self):
        left_df: DataFrame = data.test_df
        right_df: DataFrame = data.test_df1
        left_diff: list = left_diff_schemas(left_df, right_df)
        # "K E   Y", "cols with   sp  aces", " .value"
        self.assertEqual(left_diff, [" .value"])

    def test_right_diff_schemas(self):
        left_df = data.test_df
        right_df = data.test_df1
        right_diff = right_diff_schemas(left_df, right_df)
        self.assertEqual(right_diff, ["value"])

    def test_select_list(self):
        # df = select_list(data.test_df, ["K E   Y", " .value"])
        df = data.test_df.select_list(["K E   Y", " .value"])
        self.assertEqual(df.schema.fieldNames(), ["K E   Y", " .value"])
        self.assertEqual(df.count(), 3)

    def test_select_exclude(self):
        # df = select_exclude(data.test_df1, ["cols with   sp  aces", "value"])
        df = data.test_df1.select_exclude(["cols with   sp  aces", "value"])
        self.assertEqual(df.schema.fieldNames(), ["K E   Y"])
        self.assertEqual(df.count(), 3)

    def test_drop_columns(self):
        df = data.test_df1.drop_columns(["cols with   sp  aces", "value"])
        self.assertEqual(df.schema.fieldNames(), ["K E   Y"])
        self.assertEqual(df.count(), 3)

    def test_rename_columns(self):
        rename_mapping_dict = {"K E   Y": "key",
                               "cols with   sp  aces": "col1",
                               " .value": "val"
                               }
        df = data.test_df.rename_columns(rename_mapping_dict)
        self.assertEqual(df.schema.fieldNames(), ['key', 'col1', 'val'])
        self.assertEqual(df.count(), 3)

    def test_rename_4_parquet(self):
        """
        input schema is "K E   Y", "cols with   sp  aces", " .value"
        :return:
        """
        # df = rename_4_parquet(data.test_df)
        df = data.test_df.rename_4_parquet()
        # replace with logging print(df.schema.fieldNames())
        self.assertEqual(df.schema.fieldNames(), ['KEY', 'colswithspaces', '__value'])
        self.assertEqual(df.count(), 3)

    def test_unpivot(self):
        data.simple_df.printSchema()
        # ["firstname", "middlename", "lastname", "id", "location", "salary"]
        df_unpivot = data.simple_df.unpivot({"keys": ["firstname", "lastname"], "unpivot_columns": ["id", "location", "salary"]})
        # the following would be logging
        df_unpivot.printSchema()
        df_unpivot.show()
        # asserting cardinality (maybe enough)
        self.assertEqual(df_unpivot.count(), 15)

    def test_unpivot_all(self):
        data.simple_df.printSchema()
        # ["firstname", "middlename", "lastname", "id", "location", "salary"]
        df_unpivot = data.simple_df.unpivot_all(["firstname", "middlename", "lastname"])
        # the following would be logging
        df_unpivot.printSchema()
        df_unpivot.show()
        # asserting cardinality (maybe enough)
        self.assertEqual(df_unpivot.count(), 15)

    def test_flatten(self):
        # df_out = flatten(data.df_json)
        df_out = data.df_json.flatten()
        self.assertEqual(len(df_out.schema.fieldNames()), 10)
        self.assertEqual(df_out.count(), 2)

    def test_flatten_no_explode(self):
        # df_out = flatten_no_explode(data.df_json)
        df_out = data.df_json.flatten_no_explode()
        self.assertEqual(len(df_out.schema.fieldNames()), 9)
        self.assertEqual(df_out.count(), 1)

    def test_pipeline(self):
        import json
        # print(os.getcwd())
        # path = os.getcwd()
        # os.chdir(f'../resources')
        python_root_path = f"{os.environ.get('ROOT_DIR')}"

        # Opening JSON file. need to make it relative...
        f = open(f'{python_root_path}/resources/test_pipeline.json',)
        # returns JSON object as
        # a dictionary
        pipeline_specs = json.load(f)
        # Closing file
        f.close()
        # df_out = run_pipeline(data.test_df_float, pipeline_specs)
        df_out = data.test_df_float.run_pipeline(pipeline_specs)
        # print(df_out.schema.fieldNames())
        self.assertEqual(len(df_out.schema.fieldNames()), 2)
        self.assertEqual(df_out.count(), 6)
        self.assertEqual(df_out.schema.fieldNames(), ['k', 'float_value'])


if __name__ == '__main__':
    unittest.main()

