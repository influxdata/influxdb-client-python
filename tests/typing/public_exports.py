"""Check the public imports of a Python client.

Run with:
    python -m mypy --no-implicit-reexport --follow-imports=silent tests/typing/public_exports.py
    python tests/typing/public_exports.py
"""

from influxdb_client import (
    Bucket,
    BucketsService,
    Configuration,
    InfluxDBClient,
    Point,
    WriteOptions,
    WritePrecision,
)


def check_exports() -> None:
    point = Point("temperature").tag("room", "office").field("value", 21)
    assert point.to_line_protocol() == "temperature,room=office value=21i"
    assert WriteOptions(batch_size=10).batch_size == 10
    assert WritePrecision.NS == "ns"
    assert Bucket is not None
    assert BucketsService is not None
    assert Configuration is not None
    assert InfluxDBClient is not None


if __name__ == "__main__":
    check_exports()
