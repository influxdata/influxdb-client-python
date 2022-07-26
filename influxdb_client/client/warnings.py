"""The warnings message definition."""
import warnings


class MissingPivotFunction(UserWarning):
    """User warning about missing pivot() function."""

    @staticmethod
    def print_warning(query: str):
        """Print warning about missing pivot() function and how to deal with that."""
        if 'fieldsAsCols' in query or 'pivot' in query:
            return

        message = f"""The query doesn't contains the pivot() function.

The result will not be shaped to optimal processing by pandas.DataFrame. Use the pivot() function by:

    {query} |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")

You can disable this warning by:
    import warnings
    from influxdb_client.client.warnings import MissingPivotFunction

    warnings.simplefilter("ignore", MissingPivotFunction)

For more info see:
    - https://docs.influxdata.com/resources/videos/pivots-in-flux/
    - https://docs.influxdata.com/flux/latest/stdlib/universe/pivot/
    - https://docs.influxdata.com/flux/latest/stdlib/influxdata/influxdb/schema/fieldsascols/
"""
        warnings.warn(message, MissingPivotFunction)
