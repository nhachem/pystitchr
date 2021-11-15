from pyspark.sql.dataframe import DataFrame
# import pystitchr.base.df_transforms as dft
import logging
from pystitchr.util.simple_logging import log


def transform0(self, f):
    """
    pyspark does not have a transform before version 3... we need to add one to DataFrame.
    This is based on https://mungingdata.com/pyspark/chaining-dataframe-transformations/
    """
    return f(self)


def run_pipeline(input_df: DataFrame, pipeline: dict, logging_level: str = 'ERROR') -> DataFrame:
    """

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


