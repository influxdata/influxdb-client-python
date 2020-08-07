"""Pandas date utils."""
from influxdb_client.client.util.date_utils import DateHelper
from influxdb_client.extras import pd


class PandasDateTimeHelper(DateHelper):
    """DateHelper that use Pandas library with nanosecond precision."""

    def parse_date(self, date_string: str):
        """Parse date string into `class 'pandas._libs.tslibs.timestamps.Timestamp`."""
        return pd.to_datetime(date_string)

    def to_nanoseconds(self, delta):
        """Get number of nanoseconds with nanos precision."""
        return super().to_nanoseconds(delta) + (delta.nanoseconds if hasattr(delta, 'nanoseconds') else 0)
