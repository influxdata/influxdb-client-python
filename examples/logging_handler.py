"""
Show the usage of influx with python native logging.

This is useful if you
* want to log to influx and a local file.
* want to set up influx logging in a project without specifying it in submodules
"""
import datetime
import logging
import time

from influxdb_client import InfluxLoggingHandler, WritePrecision, Point

DATA_LOGGER_NAME = '…'


def setup_logger():
    """
    Set up data logger with the influx logging handler.

    This can happen in your core module.
    """
    influx_logging_handler = InfluxLoggingHandler(url="…", token="…", org="…", bucket="…",
                                                  client_args={'arg1': '…'},
                                                  write_api_args={'arg': '…'})
    influx_logging_handler.setLevel(logging.DEBUG)

    data_logger = logging.getLogger(DATA_LOGGER_NAME)
    data_logger.setLevel(logging.DEBUG)
    data_logger.addHandler(influx_logging_handler)
    # feel free to add other handlers here.
    # if you find yourself writing filters e.g. to only log points to influx, think about adding a PR :)


def use_logger():
    """Use the logger. This can happen in any submodule."""
    # `data_logger` will have the influx_logging_handler attached if setup_logger was called somewhere.
    data_logger = logging.getLogger(DATA_LOGGER_NAME)
    # write a line yourself
    data_logger.debug(f"my-measurement,host=host1 temperature=25.3 {int(time.time() * 1e9)}")
    # or make use of the influxdb helpers like Point
    data_logger.debug(
        Point('my-measurement')
            .tag('host', 'host1')
            .field('temperature', 25.3)
            .time(datetime.datetime.utcnow(), WritePrecision.MS)
    )
