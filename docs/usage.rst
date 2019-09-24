User Guide
==========

.. contents::
   :local:

Debugging
^^^^^^^^^

For debug purpose you can enable verbose logging of http requests. Both request header and body will be logged to standard output.

.. code-block:: python

    _client = InfluxDBClient(url="http://localhost:9999", token="my-token", debug=True, org="my-org")

