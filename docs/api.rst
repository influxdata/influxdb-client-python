API Reference
=============

.. contents::
   :local:

InfluxDBClient
""""""""""""""
.. autoclass:: influxdb_client.InfluxDBClient
   :members:

QueryApi
""""""""
.. autoclass:: influxdb_client.QueryApi
   :members:

.. autoclass:: influxdb_client.client.flux_table.FluxTable
   :members:

.. autoclass:: influxdb_client.client.flux_table.FluxRecord
   :members:

WriteApi
""""""""
.. autoclass:: influxdb_client.WriteApi
   :members:

.. autoclass:: influxdb_client.client.write.point.Point
   :members:

.. autoclass:: influxdb_client.domain.write_precision.WritePrecision
   :members:

BucketsApi
""""""""""
.. autoclass:: influxdb_client.BucketsApi
   :members:

.. autoclass:: influxdb_client.domain.Bucket
   :members:

LabelsApi
"""""""""
.. autoclass:: influxdb_client.LabelsApi
   :members:

OrganizationsApi
""""""""""""""""
.. autoclass:: influxdb_client.OrganizationsApi
   :members:

.. autoclass:: influxdb_client.domain.Organization
   :members:

UsersApi
""""""""
.. autoclass:: influxdb_client.UsersApi
   :members:

.. autoclass:: influxdb_client.domain.User
   :members:

TasksApi
""""""""
.. autoclass:: influxdb_client.TasksApi
   :members:

.. autoclass:: influxdb_client.domain.Task
   :members:

InvokableScriptsApi
"""""""""""""""""""
.. autoclass:: influxdb_client.InvokableScriptsApi
   :members:

.. autoclass:: influxdb_client.domain.Script
   :members:

.. autoclass:: influxdb_client.domain.ScriptCreateRequest
   :members:

DeleteApi
"""""""""
.. autoclass:: influxdb_client.DeleteApi
   :members:

.. autoclass:: influxdb_client.domain.DeletePredicateRequest
   :members:

Helpers
"""""""
.. autoclass:: influxdb_client.client.util.date_utils.DateHelper
   :members:

.. autoclass:: influxdb_client.client.util.date_utils_pandas.PandasDateTimeHelper
   :members:

.. autoclass:: influxdb_client.client.util.multiprocessing_helper.MultiprocessingWriter
   :members:

