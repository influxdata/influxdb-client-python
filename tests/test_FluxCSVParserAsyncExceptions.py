import asyncio
import unittest
from unittest.mock import Mock

from influxdb_client.client.flux_csv_parser import FluxCsvParser, FluxSerializationMode


class FluxCsvParserAsyncExceptionTest(unittest.TestCase):
    def test_preserves_exception_and_closes_response(self):
        async def check(error):
            response = Mock()
            parser = FluxCsvParser(response, FluxSerializationMode.stream)

            async def rows():
                raise error
                yield  # Make this an async iterator, like the CSV reader.

            parser._reader = rows()
            with self.assertRaises(type(error)) as raised:
                await parser.generator_async().__anext__()
            self.assertIs(raised.exception, error)
            response.close.assert_called_once()
            if hasattr(error, "add_note") and isinstance(error, (asyncio.CancelledError, asyncio.TimeoutError)):
                self.assertTrue(any("timeout" in note for note in error.__notes__))

        loop = asyncio.new_event_loop()
        try:
            for error in (asyncio.CancelledError(), asyncio.TimeoutError(), ValueError("invalid row")):
                with self.subTest(error=type(error).__name__):
                    loop.run_until_complete(check(error))
        finally:
            loop.close()
