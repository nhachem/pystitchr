from pprint import pprint
from pyspark.sql import DataFrame

# from pystitchr.util.stitchr_logging import LoggerSingleton, put_log
# from pystitchr.util.simple_logging import log
# NH logging still under development
import logging

def run_pipeline(input_df: DataFrame, pipeline: list, logging_level: str = "ERROR") -> DataFrame:
    """
    run_pipeline takes in a data frame and produces a result dataframe based on applying a sequence
    of df_2_df transforms
    The sequence of functions would look like this for example to load sfdc provider data
    entity_table = "provider"
    source_system = 'sfdc'
    pipeline_spec = [
        {"function": "init_source_data", "params": [source_system, entity_table]},
        {"function": "add_fks", "params": [entity_table]},
        {"function": "add_target_columns", "params": [entity_table]},
        {"function": "add_dup_flag", "params": ["external_id", "source_system"]},
        # [6]: last but not least call the merge and return what was pushed (same df as what was pushed in)
    ]
    a pipeline spec can also include calls to functions by passing the parameters as a dict that is **kwargs
    Most usage relies on **kwargs
    for example
    pipeline = [
                {"function": "validate_foreign_key", "params": {"entity": entity, "trap_data": True}},
               ...]

    @param input_df:
    @type input_df: DataFrame (table)
    @param pipeline:
    @type pipeline: list of transform steps
    @param logging_level:
    @type logging_level:
    @return: It is expected that the result out of the last step in the sequence is returned as a DataFrame.
    This means run_pipeline can be chained as well
    @rtype: DataFrame
    """

    # logger = LoggerSingleton().get_logger("run_pipeline")
    import importlib
    # ToDo: control logging level globally from outside
    logging.basicConfig(level=logging_level)
    logger = logging.getLogger("logging")

    df_p = input_df
    steps = pipeline

    logger.debug(f"number of steps is {len(steps)}")
    for step in steps:
        # step.assertTrue(type is dict)
        logger.info(step)
        function = step["function"]
        dynamic_module_ref = "DataFrame"  # step.get("module", "DataFrame")
        # params = step["params"]
        params = step.get("params", [])
        logger.debug(
            f"step is {step}, transform is {function} from module {dynamic_module_ref} with attributes {params}"
        )
        # check if method is in the object would look like this
        if hasattr(globals()[dynamic_module_ref], function):
            method_to_call = getattr(globals()[dynamic_module_ref], function)
        else:
            # hack but will do for now
            logger.error(f"attribute {function} not in {dynamic_module_ref}")
            raise
        # assumes only 2 ways of providing list or dict
        # print(params)
        # df_p.transform(method_to_call, **params)
        df_p = (
            df_p.transform(lambda df: method_to_call(df, params))
            if isinstance(params, list)
            else df_p.transform(method_to_call, **params)
        )
        # change to log if we want info
        # print(f"root level logging is {log.root.level}")
        # TODO: NH need to debug as this goes through log4j and is kind of messy
        # if logging.DEBUG >= log.root.level:
        # logs even if we use warn or higher so commented out
        # log.info(df_p.printSchema())
    return df_p
