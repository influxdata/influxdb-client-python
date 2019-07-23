class FluxStructure:
    pass


class FluxTable(FluxStructure):

    def __init__(self) -> None:
        self.columns = []
        self.records = []

    def get_group_key(self):
        return list(filter(lambda column: (column.group is True), self.columns))

    def __str__(self):
        cls_name = type(self).__name__
        return cls_name + "() columns: " + str(len(self.columns)) + ", records: " + str(len(self.records))

    def __iter__(self):
        return iter(self.records)


class FluxColumn(FluxStructure):
    def __init__(self, index=None, label=None, data_type=None, group=None, default_value=None) -> None:
        self.default_value = default_value
        self.group = group
        self.data_type = data_type
        self.label = label
        self.index = index


class FluxRecord(FluxStructure):

    def __init__(self, table, values=None) -> None:
        if values is None:
            values = {}
        self.table = table
        self.values = values

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

    def __str__(self):
        cls_name = type(self).__name__
        return cls_name + "() table: " + str(self.table) + ", " + str(self.values)
