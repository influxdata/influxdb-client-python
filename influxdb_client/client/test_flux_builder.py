from flux_builder import qBuilder 

def test_bucket():
    q = qBuilder()
    assert str(q.bucket("my_bucket").do()) == 'from(bucket: "my_bucket")'

def test_range():
    q1 = qBuilder()
    assert str(q1.range(start="-1h", stop="-10m").do()) == '|> range(start:-1h, stop:-10m)'

def test_build_equals():
    q2a = qBuilder()
    q2b = qBuilder()
    q2c = qBuilder()
    assert q2a.build_equals(key="key", value="value") == 'r.key == "value"'
    assert str(q2b.build_equals(key="key", value="value", prefix="x",)) == 'x.key == "value"'
    assert str(q2c.build_equals(key="key", value=1, equality= ">", prefix="x",)) == 'x.key > 1'

def test_filters():
    q3a = qBuilder()
    q3b = qBuilder()
    q3c = qBuilder()
    q3d = qBuilder()
    assert str(q3a.filters(filters=[("_measurement", ["abc"])]).do()) == '|> filter(fn: (r) => r._measurement == "abc")'
    assert str(q3b.filters(filters=[("_value", [1])], equality = ">").do()) == '|> filter(fn: (r) => r._value > 1)'
    assert str(q3c.filters(filters=[("_measurement", ["abc"]),("key", ["value"])]).do()) == '|> filter(fn: (r) => r._measurement == "abc")|> filter(fn: (r) => r.key == "value")'
    assert str(q3d.filters(filters=[("_measurement", ["abc", "def"]), ("key", ["value"])]).do()) == '|> filter(fn: (r) => r._measurement == "abc" or r._measurement == "def")|> filter(fn: (r) => r.key == "value")'

def test_flatten():
    q4 = qBuilder()
    assert str(q4.flatten(flat=True).do()) == '|> group()'

def test_do():
    q4 = qBuilder()
    assert str(q4.bucket("test").range("-1hr","now()").filters(filters=[("_measurement", ["abc", "def"]), ("key", ["1","2"])]).flatten(flat=True).do()) == """from(bucket: "test")|> range(start:-1hr, stop:now())|> filter(fn: (r) => r._measurement == "abc" or r._measurement == "def")
|> filter(fn: (r) => r.key == "1" or r.key == "2")|> group()"""

