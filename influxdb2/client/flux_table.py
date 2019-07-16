import inspect


class FluxStructure:

    def __str__(self):
        return self.__to_string(str)

    def __repr__(self):
        return self.__to_string(repr)

    def __to_string(self, strfunc):
        sig = inspect.signature(self.__init__)
        values = []
        for attr in sig.parameters:
            value = getattr(self, attr)
            values.append(attr + ": " + strfunc(value))

        clsname = type(self).__name__
        args = ', '.join(values)
        return '{}({})'.format(clsname, args)


class FluxTable(FluxStructure):

    def __init__(self) -> None:
        self.columns = []
        self.records = []

    def get_group_key(self):
        return list(filter(lambda column: (column.group is True), self.columns))


class FluxColumn(FluxStructure):
    def __init__(self, index=None, label=None, data_type=None, group=None, default_value=None) -> None:
        self.default_value = default_value
        self.group = group
        self.data_type = data_type
        self.label = label
        self.index = index


class FluxRecord(FluxStructure):

    def __init__(self, table) -> None:
        self.table = table
        self.values = {}

    def get_start(self):
        return self.values.get("_start")

    def get_stop(self):
        return self.values.get("_stop")

    def get_time(self):
        return self.values.get("_time")

    def get_value(self):
        return self.values.get("_value")

    def get_field(self):
        return self.values.get("_field")

    def get_measurement(self):
        return self.values.get("_measurement")
