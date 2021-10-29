# copied from https://gist.github.com/thsutton/65f0ec3cf132495ef91dc22b9bc38aec
# Forward Python logging module to Spark log4j
import logging
import time
from contextlib import AbstractContextManager
from logging import Handler, LogRecord
from typing import Any, List, Optional

from pyspark.sql import SparkSession


# NH: want to control from the calling code
# logging.basicConfig(level=logging.WARN)
# log = logging.getLogger(__name__)
log = logging.getLogger("logging")

def main():
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    logging.info('Started')
    print('doing something')
    logging.info('Finished')


if __name__ == '__main__':
    main()

