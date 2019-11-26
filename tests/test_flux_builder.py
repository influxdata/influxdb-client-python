from influxdb_client.client.flux_builder import qBuilder
import pytest 

def test_bucket():
    q = qBuilder().bucket("my_bucket")
    assert q.build_bucket == 'from(bucket: bucket_name)'

# def test_bucket_mandatory():
#     q = qBuilder()
#     with pytest.raises(Exception, match="Must provide bucket"):
#         q.do()

def test_range():
    q = qBuilder().range("-1h","-10m")
    assert q.build_time == '|> range(start: start_time, stop: stop_time)'

# def test_start_time_mandatory():
#     q = qBuilder().bucket("my_bucket")
#     with pytest.raises(Exception, match="Must provide start time"):
#         q.do()

def test_build_equals():
    q = qBuilder()
    assert q.build_equals(key="key", value="value") == 'r.key == "value"'
    assert q.build_equals(key="key", value="value", prefix="x") == 'x.key == "value"'
    assert q.build_equals(key="key", value=1, equality= ">", prefix="x") == 'x.key > 1'

# def test_filters():
#     q1 = qBuilder().filters(filters=[("_measurement", ["abc"])])
#     q2 = qBuilder().filters(filters=[("_value", [1])], equality = ">")
#     q3 = qBuilder().filters(filters=[("_measurement", ["abc", "def"]), ("key", ["value"])])
#     q4 = qBuilder().filters(filters=[("_measurement", ["abc"]),("key", ["value"])]) 
#     assert q1.build_filters == '|> filter(fn: (r) => r._measurement == "abc")'
#     assert q2.build_filters == '|> filter(fn: (r) => r._value > 1)'
#     assert q3.build_filters == '|> filter(fn: (r) => r._measurement == "abc" or r._measurement == "def")|> filter(fn: (r) => r.key == "value")'
#     assert q4.build_filters == '|> filter(fn: (r) => r._measurement == "abc")|> filter(fn: (r) => r.key == "value")'

# def test_do():
#     q = qBuilder().bucket("test").range("-1hr","now()").filters(filters=[("_measurement", ["abc", "def"]), ("key", ["1","2"])]).do()
#     assert q.build_flux_query == """bucket_name = "test"\nstart_time = "-1hr"\nstop_time = "now()"\nfilters = "[('_measurement', ['abc', 'def']), ('key', ['1', '2'])]"\nfrom(bucket: bucket_name)|> range(start: start_time, stop: stop_time)|> filter(fn: (r) => r._measurement == "abc" or r._measurement == "def")|> filter(fn: (r) => r.key == "1" or r.key == "2")"""


def test_identify_filter_type():
    q = qBuilder().bucket("test").range("-1hr","now()").filters(filters=("_measurement", ["1", "2"]))
    q1 = qBuilder().bucket("test").range("-1hr","now()").filters(filters=("_measurement", "1"))
    assert q.ls == ("_measurement", ["1", "2"])
    assert q1.single == ("_measurement", "1")

def test_list_filters():
    q = qBuilder().bucket("test").range("-1hr","now()").filters(filters=("_measurement", ["1", "2"]))
    q1 = qBuilder().bucket("test").range("-1hr","now()").filters(filters=("_measurement", "1"))
    assert q.build_filters == 'filter(fn: (r) => contains(value: r._measurement, set: _measurement_value))'
    assert q1.build_filters == 'filter(fn: (r) => _measurement = _measurement_value))'



