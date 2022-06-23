from pyspark.sql.dataframe import DataFrame
# import pystitchr.base.df_transforms as dft
import logging
from pystitchr.util.simple_logging import log
# NH: needed to perform dynamic invocations
import pystitchr

def transform0(self, f):
    """
    pyspark does not have a transform before version 3... we need to add one to DataFrame.
    This is based on https://mungingdata.com/pyspark/chaining-dataframe-transformations/
    """
    return f(self)


def run_pipeline(input_df: DataFrame, pipeline: dict, logging_level: str = 'ERROR') -> DataFrame:
    """
    run_pipeline takes in a data frame and produces a result dataframe based on applying
    a sequence of df_2_df transforms.
    The sequence of functions would look like this. See demoPipelineRun_v0_3.py

    pipeline_spec = {1: {'add_columns': {"module": "pystitchr", "params": {'BARCODE':
                                                                            'get_random_alphanumeric(8, ceil(f3*1000))',
                                     'WELL_NUMBER': 'ceil(f2*100)',
                                     'PLATE_ID': 'get_random_alphanumeric(8, ceil(f4*1000))',
                                     'EXPERIMENT_ID': 'get_random_alphanumeric(15, ceil(f6*1000))'
                                     }}
                     },
                 2: {"rename_columns": {"module": "pystitchr", "params": {"f1": "foo", "f2": "bar",
                                                                          'f6': "not-a-parquet(),{name}"}}},
                 3: {'rename_4_parquet': {"module": "pystitchr", "params": []}},
                 4: {"drop_columns": {"module": "pystitchr", "params": ["f3", "f4", "f7", "f8", "f9"]}},
                 5: {"flatten": {"module": "pystitchr", "params":[]}},
                 6: {"unpivot": {"module": "pystitchr",
                                 "params": {"keys": ['BARCODE', 'WELL_NUMBER', 'PLATE_ID',
                                          'EXPERIMENT_ID', 'address__city', 'not__a__parquet________name__'],
                                            "unpivot_columns": ["foo", "bar", "f5"],
                                            "key_column": "key_column", "value_column": "value"
                                 }}},

    @param input_df:
    @type input_df: DataFrame (table)
    @param pipeline:
    @type pipeline: dictionary of transform steps. Functions in those steps are extensions to DataFrame
                    and stitch transforms
    @param logging_level: default to ERROR
    @type logging_level:
    @return: It is expected that the result out of the last step in the sequence is returned as a DataFrame.
    This means run_pipeline can be chained as well
    @rtype: DataFrame
    """
    import logging
    import importlib
    # ToDo: control logging level globally from outside
    logging.basicConfig(level=logging_level)
    log = logging.getLogger("logging")
    # don't want to modify the source so we assign it
    df_p = input_df
    steps = pipeline
    log.info(f"number of steps is {len(steps)}")
    for step in steps:
        log.info(steps[step])
        key = list(steps[step].keys())[0]
        dynamic_module_ref = steps[step][key]["module"]
        # NH: this is not needed unless the module has not been imported. One can invoke if the module is not found
        # locals()[dynamic_module_ref] = importlib.import_module(dynamic_module_ref)
        params = steps[step][key]["params"]
        # print(f"step is {step}, transform is {key} from module {dynamic_module_ref} with attributes {params}")
        log.info(f"step is {step}, transform is {key} from module {dynamic_module_ref} with attributes {params}")
        # check if method is in the object would look like this
        if hasattr(globals()[dynamic_module_ref], key):
            method_to_call = getattr(globals()[dynamic_module_ref], key)
        else:
            # hack but will do for now
            log.error(f"attribute {key} not in {dynamic_module_ref}")
            raise

        df_p = df_p.transform(lambda df: method_to_call(df, params))
        # change to log if we want info
        # print(f"root level logging is {log.root.level}")
        # TODO: NH need to debug as this goes through log4j and is kind of messy
        # if logging.DEBUG >= log.root.level:
        # logs even if we use warn or higher so commented out
        # log.info(df_p.printSchema())
    return df_p


def run_pipeline_v0_3(input_df: DataFrame, pipeline: dict, logging_level: str = 'ERROR') -> DataFrame:
    """
    only handles pystitchr transforms or original DataFrame. This is deprecated as of Version 0.4
    for doc see help(run_pipeline)
    @param input_df:
    @type input_df:
    @param pipeline:
    @type pipeline:
    @param logging_level:
    @type logging_level:
    @return:
    @rtype:
    """
    dynamic_module = __import__('pystitchr')
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
        # method_to_call = getattr(dft, key)
        method_to_call = getattr(dynamic_module, key)
        df_p = df_p.transform(lambda df: method_to_call(df, params))
        # change to log if we want info
        # print(f"root level logging is {log.root.level}")
        # TODO: NH need to debug as this goes through log4j and is kind of messy
        # if logging.DEBUG >= log.root.level:
        # logs even if we use warn or higher so commented out
        # log.info(df_p.printSchema())
    return df_p


