# coding: utf-8

"""
InfluxDB OSS API Service.

The InfluxDB v2 API provides a programmatic interface for all interactions with InfluxDB. Access the InfluxDB API using the `/api/v2/` endpoint.   # noqa: E501

OpenAPI spec version: 2.0.0
Generated by: https://openapi-generator.tech
"""


import pprint
import re  # noqa: F401

from influxdb_client.domain.view_properties import ViewProperties


class BandViewProperties(ViewProperties):
    """NOTE: This class is auto generated by OpenAPI Generator.

    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    """
    Attributes:
      openapi_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    openapi_types = {
        'adaptive_zoom_hide': 'bool',
        'time_format': 'str',
        'type': 'str',
        'queries': 'list[DashboardQuery]',
        'colors': 'list[DashboardColor]',
        'shape': 'str',
        'note': 'str',
        'show_note_when_empty': 'bool',
        'axes': 'Axes',
        'static_legend': 'StaticLegend',
        'x_column': 'str',
        'generate_x_axis_ticks': 'list[str]',
        'x_total_ticks': 'int',
        'x_tick_start': 'float',
        'x_tick_step': 'float',
        'y_column': 'str',
        'generate_y_axis_ticks': 'list[str]',
        'y_total_ticks': 'int',
        'y_tick_start': 'float',
        'y_tick_step': 'float',
        'upper_column': 'str',
        'main_column': 'str',
        'lower_column': 'str',
        'hover_dimension': 'str',
        'geom': 'XYGeom',
        'legend_colorize_rows': 'bool',
        'legend_hide': 'bool',
        'legend_opacity': 'float',
        'legend_orientation_threshold': 'int'
    }

    attribute_map = {
        'adaptive_zoom_hide': 'adaptiveZoomHide',
        'time_format': 'timeFormat',
        'type': 'type',
        'queries': 'queries',
        'colors': 'colors',
        'shape': 'shape',
        'note': 'note',
        'show_note_when_empty': 'showNoteWhenEmpty',
        'axes': 'axes',
        'static_legend': 'staticLegend',
        'x_column': 'xColumn',
        'generate_x_axis_ticks': 'generateXAxisTicks',
        'x_total_ticks': 'xTotalTicks',
        'x_tick_start': 'xTickStart',
        'x_tick_step': 'xTickStep',
        'y_column': 'yColumn',
        'generate_y_axis_ticks': 'generateYAxisTicks',
        'y_total_ticks': 'yTotalTicks',
        'y_tick_start': 'yTickStart',
        'y_tick_step': 'yTickStep',
        'upper_column': 'upperColumn',
        'main_column': 'mainColumn',
        'lower_column': 'lowerColumn',
        'hover_dimension': 'hoverDimension',
        'geom': 'geom',
        'legend_colorize_rows': 'legendColorizeRows',
        'legend_hide': 'legendHide',
        'legend_opacity': 'legendOpacity',
        'legend_orientation_threshold': 'legendOrientationThreshold'
    }

    def __init__(self, adaptive_zoom_hide=None, time_format=None, type=None, queries=None, colors=None, shape=None, note=None, show_note_when_empty=None, axes=None, static_legend=None, x_column=None, generate_x_axis_ticks=None, x_total_ticks=None, x_tick_start=None, x_tick_step=None, y_column=None, generate_y_axis_ticks=None, y_total_ticks=None, y_tick_start=None, y_tick_step=None, upper_column=None, main_column=None, lower_column=None, hover_dimension=None, geom=None, legend_colorize_rows=None, legend_hide=None, legend_opacity=None, legend_orientation_threshold=None):  # noqa: E501,D401,D403
        """BandViewProperties - a model defined in OpenAPI."""  # noqa: E501
        ViewProperties.__init__(self)  # noqa: E501

        self._adaptive_zoom_hide = None
        self._time_format = None
        self._type = None
        self._queries = None
        self._colors = None
        self._shape = None
        self._note = None
        self._show_note_when_empty = None
        self._axes = None
        self._static_legend = None
        self._x_column = None
        self._generate_x_axis_ticks = None
        self._x_total_ticks = None
        self._x_tick_start = None
        self._x_tick_step = None
        self._y_column = None
        self._generate_y_axis_ticks = None
        self._y_total_ticks = None
        self._y_tick_start = None
        self._y_tick_step = None
        self._upper_column = None
        self._main_column = None
        self._lower_column = None
        self._hover_dimension = None
        self._geom = None
        self._legend_colorize_rows = None
        self._legend_hide = None
        self._legend_opacity = None
        self._legend_orientation_threshold = None
        self.discriminator = None

        if adaptive_zoom_hide is not None:
            self.adaptive_zoom_hide = adaptive_zoom_hide
        if time_format is not None:
            self.time_format = time_format
        self.type = type
        self.queries = queries
        self.colors = colors
        self.shape = shape
        self.note = note
        self.show_note_when_empty = show_note_when_empty
        self.axes = axes
        if static_legend is not None:
            self.static_legend = static_legend
        if x_column is not None:
            self.x_column = x_column
        if generate_x_axis_ticks is not None:
            self.generate_x_axis_ticks = generate_x_axis_ticks
        if x_total_ticks is not None:
            self.x_total_ticks = x_total_ticks
        if x_tick_start is not None:
            self.x_tick_start = x_tick_start
        if x_tick_step is not None:
            self.x_tick_step = x_tick_step
        if y_column is not None:
            self.y_column = y_column
        if generate_y_axis_ticks is not None:
            self.generate_y_axis_ticks = generate_y_axis_ticks
        if y_total_ticks is not None:
            self.y_total_ticks = y_total_ticks
        if y_tick_start is not None:
            self.y_tick_start = y_tick_start
        if y_tick_step is not None:
            self.y_tick_step = y_tick_step
        if upper_column is not None:
            self.upper_column = upper_column
        if main_column is not None:
            self.main_column = main_column
        if lower_column is not None:
            self.lower_column = lower_column
        if hover_dimension is not None:
            self.hover_dimension = hover_dimension
        self.geom = geom
        if legend_colorize_rows is not None:
            self.legend_colorize_rows = legend_colorize_rows
        if legend_hide is not None:
            self.legend_hide = legend_hide
        if legend_opacity is not None:
            self.legend_opacity = legend_opacity
        if legend_orientation_threshold is not None:
            self.legend_orientation_threshold = legend_orientation_threshold

    @property
    def adaptive_zoom_hide(self):
        """Get the adaptive_zoom_hide of this BandViewProperties.

        :return: The adaptive_zoom_hide of this BandViewProperties.
        :rtype: bool
        """  # noqa: E501
        return self._adaptive_zoom_hide

    @adaptive_zoom_hide.setter
    def adaptive_zoom_hide(self, adaptive_zoom_hide):
        """Set the adaptive_zoom_hide of this BandViewProperties.

        :param adaptive_zoom_hide: The adaptive_zoom_hide of this BandViewProperties.
        :type: bool
        """  # noqa: E501
        self._adaptive_zoom_hide = adaptive_zoom_hide

    @property
    def time_format(self):
        """Get the time_format of this BandViewProperties.

        :return: The time_format of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._time_format

    @time_format.setter
    def time_format(self, time_format):
        """Set the time_format of this BandViewProperties.

        :param time_format: The time_format of this BandViewProperties.
        :type: str
        """  # noqa: E501
        self._time_format = time_format

    @property
    def type(self):
        """Get the type of this BandViewProperties.

        :return: The type of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._type

    @type.setter
    def type(self, type):
        """Set the type of this BandViewProperties.

        :param type: The type of this BandViewProperties.
        :type: str
        """  # noqa: E501
        if type is None:
            raise ValueError("Invalid value for `type`, must not be `None`")  # noqa: E501
        self._type = type

    @property
    def queries(self):
        """Get the queries of this BandViewProperties.

        :return: The queries of this BandViewProperties.
        :rtype: list[DashboardQuery]
        """  # noqa: E501
        return self._queries

    @queries.setter
    def queries(self, queries):
        """Set the queries of this BandViewProperties.

        :param queries: The queries of this BandViewProperties.
        :type: list[DashboardQuery]
        """  # noqa: E501
        if queries is None:
            raise ValueError("Invalid value for `queries`, must not be `None`")  # noqa: E501
        self._queries = queries

    @property
    def colors(self):
        """Get the colors of this BandViewProperties.

        Colors define color encoding of data into a visualization

        :return: The colors of this BandViewProperties.
        :rtype: list[DashboardColor]
        """  # noqa: E501
        return self._colors

    @colors.setter
    def colors(self, colors):
        """Set the colors of this BandViewProperties.

        Colors define color encoding of data into a visualization

        :param colors: The colors of this BandViewProperties.
        :type: list[DashboardColor]
        """  # noqa: E501
        if colors is None:
            raise ValueError("Invalid value for `colors`, must not be `None`")  # noqa: E501
        self._colors = colors

    @property
    def shape(self):
        """Get the shape of this BandViewProperties.

        :return: The shape of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._shape

    @shape.setter
    def shape(self, shape):
        """Set the shape of this BandViewProperties.

        :param shape: The shape of this BandViewProperties.
        :type: str
        """  # noqa: E501
        if shape is None:
            raise ValueError("Invalid value for `shape`, must not be `None`")  # noqa: E501
        self._shape = shape

    @property
    def note(self):
        """Get the note of this BandViewProperties.

        :return: The note of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._note

    @note.setter
    def note(self, note):
        """Set the note of this BandViewProperties.

        :param note: The note of this BandViewProperties.
        :type: str
        """  # noqa: E501
        if note is None:
            raise ValueError("Invalid value for `note`, must not be `None`")  # noqa: E501
        self._note = note

    @property
    def show_note_when_empty(self):
        """Get the show_note_when_empty of this BandViewProperties.

        If true, will display note when empty

        :return: The show_note_when_empty of this BandViewProperties.
        :rtype: bool
        """  # noqa: E501
        return self._show_note_when_empty

    @show_note_when_empty.setter
    def show_note_when_empty(self, show_note_when_empty):
        """Set the show_note_when_empty of this BandViewProperties.

        If true, will display note when empty

        :param show_note_when_empty: The show_note_when_empty of this BandViewProperties.
        :type: bool
        """  # noqa: E501
        if show_note_when_empty is None:
            raise ValueError("Invalid value for `show_note_when_empty`, must not be `None`")  # noqa: E501
        self._show_note_when_empty = show_note_when_empty

    @property
    def axes(self):
        """Get the axes of this BandViewProperties.

        :return: The axes of this BandViewProperties.
        :rtype: Axes
        """  # noqa: E501
        return self._axes

    @axes.setter
    def axes(self, axes):
        """Set the axes of this BandViewProperties.

        :param axes: The axes of this BandViewProperties.
        :type: Axes
        """  # noqa: E501
        if axes is None:
            raise ValueError("Invalid value for `axes`, must not be `None`")  # noqa: E501
        self._axes = axes

    @property
    def static_legend(self):
        """Get the static_legend of this BandViewProperties.

        :return: The static_legend of this BandViewProperties.
        :rtype: StaticLegend
        """  # noqa: E501
        return self._static_legend

    @static_legend.setter
    def static_legend(self, static_legend):
        """Set the static_legend of this BandViewProperties.

        :param static_legend: The static_legend of this BandViewProperties.
        :type: StaticLegend
        """  # noqa: E501
        self._static_legend = static_legend

    @property
    def x_column(self):
        """Get the x_column of this BandViewProperties.

        :return: The x_column of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._x_column

    @x_column.setter
    def x_column(self, x_column):
        """Set the x_column of this BandViewProperties.

        :param x_column: The x_column of this BandViewProperties.
        :type: str
        """  # noqa: E501
        self._x_column = x_column

    @property
    def generate_x_axis_ticks(self):
        """Get the generate_x_axis_ticks of this BandViewProperties.

        :return: The generate_x_axis_ticks of this BandViewProperties.
        :rtype: list[str]
        """  # noqa: E501
        return self._generate_x_axis_ticks

    @generate_x_axis_ticks.setter
    def generate_x_axis_ticks(self, generate_x_axis_ticks):
        """Set the generate_x_axis_ticks of this BandViewProperties.

        :param generate_x_axis_ticks: The generate_x_axis_ticks of this BandViewProperties.
        :type: list[str]
        """  # noqa: E501
        self._generate_x_axis_ticks = generate_x_axis_ticks

    @property
    def x_total_ticks(self):
        """Get the x_total_ticks of this BandViewProperties.

        :return: The x_total_ticks of this BandViewProperties.
        :rtype: int
        """  # noqa: E501
        return self._x_total_ticks

    @x_total_ticks.setter
    def x_total_ticks(self, x_total_ticks):
        """Set the x_total_ticks of this BandViewProperties.

        :param x_total_ticks: The x_total_ticks of this BandViewProperties.
        :type: int
        """  # noqa: E501
        self._x_total_ticks = x_total_ticks

    @property
    def x_tick_start(self):
        """Get the x_tick_start of this BandViewProperties.

        :return: The x_tick_start of this BandViewProperties.
        :rtype: float
        """  # noqa: E501
        return self._x_tick_start

    @x_tick_start.setter
    def x_tick_start(self, x_tick_start):
        """Set the x_tick_start of this BandViewProperties.

        :param x_tick_start: The x_tick_start of this BandViewProperties.
        :type: float
        """  # noqa: E501
        self._x_tick_start = x_tick_start

    @property
    def x_tick_step(self):
        """Get the x_tick_step of this BandViewProperties.

        :return: The x_tick_step of this BandViewProperties.
        :rtype: float
        """  # noqa: E501
        return self._x_tick_step

    @x_tick_step.setter
    def x_tick_step(self, x_tick_step):
        """Set the x_tick_step of this BandViewProperties.

        :param x_tick_step: The x_tick_step of this BandViewProperties.
        :type: float
        """  # noqa: E501
        self._x_tick_step = x_tick_step

    @property
    def y_column(self):
        """Get the y_column of this BandViewProperties.

        :return: The y_column of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._y_column

    @y_column.setter
    def y_column(self, y_column):
        """Set the y_column of this BandViewProperties.

        :param y_column: The y_column of this BandViewProperties.
        :type: str
        """  # noqa: E501
        self._y_column = y_column

    @property
    def generate_y_axis_ticks(self):
        """Get the generate_y_axis_ticks of this BandViewProperties.

        :return: The generate_y_axis_ticks of this BandViewProperties.
        :rtype: list[str]
        """  # noqa: E501
        return self._generate_y_axis_ticks

    @generate_y_axis_ticks.setter
    def generate_y_axis_ticks(self, generate_y_axis_ticks):
        """Set the generate_y_axis_ticks of this BandViewProperties.

        :param generate_y_axis_ticks: The generate_y_axis_ticks of this BandViewProperties.
        :type: list[str]
        """  # noqa: E501
        self._generate_y_axis_ticks = generate_y_axis_ticks

    @property
    def y_total_ticks(self):
        """Get the y_total_ticks of this BandViewProperties.

        :return: The y_total_ticks of this BandViewProperties.
        :rtype: int
        """  # noqa: E501
        return self._y_total_ticks

    @y_total_ticks.setter
    def y_total_ticks(self, y_total_ticks):
        """Set the y_total_ticks of this BandViewProperties.

        :param y_total_ticks: The y_total_ticks of this BandViewProperties.
        :type: int
        """  # noqa: E501
        self._y_total_ticks = y_total_ticks

    @property
    def y_tick_start(self):
        """Get the y_tick_start of this BandViewProperties.

        :return: The y_tick_start of this BandViewProperties.
        :rtype: float
        """  # noqa: E501
        return self._y_tick_start

    @y_tick_start.setter
    def y_tick_start(self, y_tick_start):
        """Set the y_tick_start of this BandViewProperties.

        :param y_tick_start: The y_tick_start of this BandViewProperties.
        :type: float
        """  # noqa: E501
        self._y_tick_start = y_tick_start

    @property
    def y_tick_step(self):
        """Get the y_tick_step of this BandViewProperties.

        :return: The y_tick_step of this BandViewProperties.
        :rtype: float
        """  # noqa: E501
        return self._y_tick_step

    @y_tick_step.setter
    def y_tick_step(self, y_tick_step):
        """Set the y_tick_step of this BandViewProperties.

        :param y_tick_step: The y_tick_step of this BandViewProperties.
        :type: float
        """  # noqa: E501
        self._y_tick_step = y_tick_step

    @property
    def upper_column(self):
        """Get the upper_column of this BandViewProperties.

        :return: The upper_column of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._upper_column

    @upper_column.setter
    def upper_column(self, upper_column):
        """Set the upper_column of this BandViewProperties.

        :param upper_column: The upper_column of this BandViewProperties.
        :type: str
        """  # noqa: E501
        self._upper_column = upper_column

    @property
    def main_column(self):
        """Get the main_column of this BandViewProperties.

        :return: The main_column of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._main_column

    @main_column.setter
    def main_column(self, main_column):
        """Set the main_column of this BandViewProperties.

        :param main_column: The main_column of this BandViewProperties.
        :type: str
        """  # noqa: E501
        self._main_column = main_column

    @property
    def lower_column(self):
        """Get the lower_column of this BandViewProperties.

        :return: The lower_column of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._lower_column

    @lower_column.setter
    def lower_column(self, lower_column):
        """Set the lower_column of this BandViewProperties.

        :param lower_column: The lower_column of this BandViewProperties.
        :type: str
        """  # noqa: E501
        self._lower_column = lower_column

    @property
    def hover_dimension(self):
        """Get the hover_dimension of this BandViewProperties.

        :return: The hover_dimension of this BandViewProperties.
        :rtype: str
        """  # noqa: E501
        return self._hover_dimension

    @hover_dimension.setter
    def hover_dimension(self, hover_dimension):
        """Set the hover_dimension of this BandViewProperties.

        :param hover_dimension: The hover_dimension of this BandViewProperties.
        :type: str
        """  # noqa: E501
        self._hover_dimension = hover_dimension

    @property
    def geom(self):
        """Get the geom of this BandViewProperties.

        :return: The geom of this BandViewProperties.
        :rtype: XYGeom
        """  # noqa: E501
        return self._geom

    @geom.setter
    def geom(self, geom):
        """Set the geom of this BandViewProperties.

        :param geom: The geom of this BandViewProperties.
        :type: XYGeom
        """  # noqa: E501
        if geom is None:
            raise ValueError("Invalid value for `geom`, must not be `None`")  # noqa: E501
        self._geom = geom

    @property
    def legend_colorize_rows(self):
        """Get the legend_colorize_rows of this BandViewProperties.

        :return: The legend_colorize_rows of this BandViewProperties.
        :rtype: bool
        """  # noqa: E501
        return self._legend_colorize_rows

    @legend_colorize_rows.setter
    def legend_colorize_rows(self, legend_colorize_rows):
        """Set the legend_colorize_rows of this BandViewProperties.

        :param legend_colorize_rows: The legend_colorize_rows of this BandViewProperties.
        :type: bool
        """  # noqa: E501
        self._legend_colorize_rows = legend_colorize_rows

    @property
    def legend_hide(self):
        """Get the legend_hide of this BandViewProperties.

        :return: The legend_hide of this BandViewProperties.
        :rtype: bool
        """  # noqa: E501
        return self._legend_hide

    @legend_hide.setter
    def legend_hide(self, legend_hide):
        """Set the legend_hide of this BandViewProperties.

        :param legend_hide: The legend_hide of this BandViewProperties.
        :type: bool
        """  # noqa: E501
        self._legend_hide = legend_hide

    @property
    def legend_opacity(self):
        """Get the legend_opacity of this BandViewProperties.

        :return: The legend_opacity of this BandViewProperties.
        :rtype: float
        """  # noqa: E501
        return self._legend_opacity

    @legend_opacity.setter
    def legend_opacity(self, legend_opacity):
        """Set the legend_opacity of this BandViewProperties.

        :param legend_opacity: The legend_opacity of this BandViewProperties.
        :type: float
        """  # noqa: E501
        self._legend_opacity = legend_opacity

    @property
    def legend_orientation_threshold(self):
        """Get the legend_orientation_threshold of this BandViewProperties.

        :return: The legend_orientation_threshold of this BandViewProperties.
        :rtype: int
        """  # noqa: E501
        return self._legend_orientation_threshold

    @legend_orientation_threshold.setter
    def legend_orientation_threshold(self, legend_orientation_threshold):
        """Set the legend_orientation_threshold of this BandViewProperties.

        :param legend_orientation_threshold: The legend_orientation_threshold of this BandViewProperties.
        :type: int
        """  # noqa: E501
        self._legend_orientation_threshold = legend_orientation_threshold

    def to_dict(self):
        """Return the model properties as a dict."""
        result = {}

        for attr, _ in self.openapi_types.items():
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def to_str(self):
        """Return the string representation of the model."""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`."""
        return self.to_str()

    def __eq__(self, other):
        """Return true if both objects are equal."""
        if not isinstance(other, BandViewProperties):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Return true if both objects are not equal."""
        return not self == other
