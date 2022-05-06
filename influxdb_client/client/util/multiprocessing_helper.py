"""
Helpers classes to make easier use the client in multiprocessing environment.

For more information how the multiprocessing works see Python's
`reference docs <https://docs.python.org/3/library/multiprocessing.html>`_.
"""
import logging
import multiprocessing

from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.exceptions import InfluxDBError

logger = logging.getLogger('influxdb_client.client.util.multiprocessing_helper')


def _success_callback(conf: (str, str, str), data: str):
    """Successfully writen batch."""
    logger.debug(f"Written batch: {conf}, data: {data}")


def _error_callback(conf: (str, str, str), data: str, exception: InfluxDBError):
    """Unsuccessfully writen batch."""
    logger.debug(f"Cannot write batch: {conf}, data: {data} due: {exception}")


def _retry_callback(conf: (str, str, str), data: str, exception: InfluxDBError):
    """Retryable error."""
    logger.debug(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


class _PoisonPill:
    """To notify process to terminate."""

    pass


class MultiprocessingWriter(multiprocessing.Process):
    """
    The Helper class to write data into InfluxDB in independent OS process.

    Example:
        .. code-block:: python

            from influxdb_client import WriteOptions
            from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter


            def main():
                writer = MultiprocessingWriter(url="http://localhost:8086", token="my-token", org="my-org",
                                               write_options=WriteOptions(batch_size=100))
                writer.start()

                for x in range(1, 1000):
                    writer.write(bucket="my-bucket", record=f"mem,tag=a value={x}i {x}")

                writer.__del__()


            if __name__ == '__main__':
                main()


    How to use with context_manager:
        .. code-block:: python

            from influxdb_client import WriteOptions
            from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter


            def main():
                with MultiprocessingWriter(url="http://localhost:8086", token="my-token", org="my-org",
                                           write_options=WriteOptions(batch_size=100)) as writer:
                    for x in range(1, 1000):
                        writer.write(bucket="my-bucket", record=f"mem,tag=a value={x}i {x}")


            if __name__ == '__main__':
                main()


    How to handle batch events:
        .. code-block:: python

            from influxdb_client import WriteOptions
            from influxdb_client.client.exceptions import InfluxDBError
            from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter


            class BatchingCallback(object):

                def success(self, conf: (str, str, str), data: str):
                    print(f"Written batch: {conf}, data: {data}")

                def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
                    print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

                def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
                    print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


            def main():
                callback = BatchingCallback()
                with MultiprocessingWriter(url="http://localhost:8086", token="my-token", org="my-org",
                                           success_callback=callback.success,
                                           error_callback=callback.error,
                                           retry_callback=callback.retry) as writer:

                    for x in range(1, 1000):
                        writer.write(bucket="my-bucket", record=f"mem,tag=a value={x}i {x}")


            if __name__ == '__main__':
                main()


    """

    __started__ = False
    __disposed__ = False

    def __init__(self, **kwargs) -> None:
        """
        Initialize defaults.

        For more information how to initialize the writer see the examples above.

        :param kwargs: arguments are passed into ``__init__`` function of ``InfluxDBClient`` and ``write_api``.
        """
        multiprocessing.Process.__init__(self)
        self.kwargs = kwargs
        self.client = None
        self.write_api = None
        self.queue_ = multiprocessing.Manager().Queue()

    def write(self, **kwargs) -> None:
        """
        Append time-series data into underlying queue.

        For more information how to pass arguments see the examples above.

        :param kwargs: arguments are passed into ``write`` function of ``WriteApi``
        :return: None
        """
        assert self.__disposed__ is False, 'Cannot write data: the writer is closed.'
        assert self.__started__ is True, 'Cannot write data: the writer is not started.'
        self.queue_.put(kwargs)

    def run(self):
        """Initialize ``InfluxDBClient`` and waits for data to writes into InfluxDB."""
        # Initialize Client and Write API
        self.client = InfluxDBClient(**self.kwargs)
        self.write_api = self.client.write_api(write_options=self.kwargs.get('write_options', WriteOptions()),
                                               success_callback=self.kwargs.get('success_callback', _success_callback),
                                               error_callback=self.kwargs.get('error_callback', _error_callback),
                                               retry_callback=self.kwargs.get('retry_callback', _retry_callback))
        # Infinite loop - until poison pill
        while True:
            next_record = self.queue_.get()
            if type(next_record) is _PoisonPill:
                # Poison pill means break the loop
                self.terminate()
                self.queue_.task_done()
                break
            self.write_api.write(**next_record)
            self.queue_.task_done()

    def start(self) -> None:
        """Start independent process for writing data into InfluxDB."""
        super().start()
        self.__started__ = True

    def terminate(self) -> None:
        """
        Cleanup resources in independent process.

        This function **cannot be used** to terminate the ``MultiprocessingWriter``.
        If you want to finish your writes please call: ``__del__``.
        """
        if self.write_api:
            logger.info("flushing data...")
            self.write_api.__del__()
            self.write_api = None
        if self.client:
            self.client.__del__()
            self.client = None
            logger.info("closed")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object."""
        self.__del__()

    def __del__(self):
        """Dispose the client and write_api."""
        if self.__started__:
            self.queue_.put(_PoisonPill())
            self.queue_.join()
            self.join()
            self.queue_ = None
        self.__started__ = False
        self.__disposed__ = True
