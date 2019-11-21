class qBuilder():
    def __init__(self):
        self.build_bucket = ""
        self.build_time = ""
        self.flatten_param = ""
        self.build_filters = ""
        self.build_flatten = ""
        self.build_flux_query = ""

    def bucket(self,bucket):
        self.build_bucket = f'from(bucket: "{bucket}")'
        return self

    def range(self, start, stop):
        self.build_time = f'|> range(start:{start}, stop:{stop})'
        return self 

    def build_equals(self, key, value, equality = "==", prefix="r",):
        if type(value) == str:
            return f'{prefix}.{key} == "{value}"' 
        if type(value) == int:
            return f'{prefix}.{key} {equality} {value}'

    def filters(self, filters, equality = "=="):
        filters_queries = []

        for filter in filters:
            or_query = ' or '.join([self.build_equals(filter[0], value, equality) for value in filter[1]])
            filters_queries.append(f'|> filter(fn: (r) => {or_query})')
            self.build_filters = "\n".join(filters_queries)
        return self

    def flatten(self, flat=True):
        if flat == True:
            self.build_flatten = f'|> group()'
            return self
        else: 
            pass 

    def do(self):
        self.build_flux_query = str(self.build_bucket) + str(self.build_time) + str(self.build_filters) + str(self.build_flatten)
        return self.build_flux_query

query = qBuilder()
query_bucket = query.bucket("my_bucket").do()
query_range = query.range("-1hr","now()").do()
# query_do = query.bucket("test").range("-1hr","now()").filters(filters=[("_measurement", ["abc", "def"]), ("key", ["1","2"])]).flatten(flat=True).do()
print(query_bucket)
# print(query_do)
print(query_range)
q2 = qBuilder()
print(q2.build_equals(key="key", value="value"))
