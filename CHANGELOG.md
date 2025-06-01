## 1.50.0 [unreleased]

### Features

1. [696](https://github.com/influxdata/influxdb-client-python/pull/696): Move "setuptools" package to build dependency.

## 1.49.0 [2025-05-22]

### Bug Fixes

1. [#682](https://github.com/influxdata/influxdb-client-python/pull/682): Check core types when creating Authentication instances.

### Examples

1. [#682](https://github.com/influxdata/influxdb-client-python/pull/682): New example for working with Authentication API.

## 1.48.0 [2024-11-27]

### Bug Fixes

1. [#679](https://github.com/influxdata/influxdb-client-python/pull/679): Add note to caught errors about need to check client timeout.

## 1.47.0 [2024-10-22]

### Bug Fixes

1. [#672](https://github.com/influxdata/influxdb-client-python/pull/672): Adding type validation to url attribute in client object
2. [#674](https://github.com/influxdata/influxdb-client-python/pull/674): Add type linting to client.flux_table.FluxTable, remove duplicated `from pathlib import Path` at setup.py
3. [#675](https://github.com/influxdata/influxdb-client-python/pull/675): Ensures WritePrecision in Point is preferred to `DEFAULT_PRECISION`

## 1.46.0 [2024-09-13]

### Bug Fixes
1. [#667](https://github.com/influxdata/influxdb-client-python/pull/667): Missing `py.typed` in distribution package

### Examples:
1. [#664](https://github.com/influxdata/influxdb-client-python/pull/664/): Multiprocessing example uses new source of data
1. [#665](https://github.com/influxdata/influxdb-client-python/pull/665): Shows how to leverage header fields in errors returned on write.

## 1.45.0 [2024-08-12]

### Bug Fixes
1. [#652](https://github.com/influxdata/influxdb-client-python/pull/652): Refactor to `timezone` specific `datetime` helpers to avoid use deprecated functions
1. [#663](https://github.com/influxdata/influxdb-client-python/pull/663): Accept HTTP 201 response to write request

## 1.44.0 [2024-06-24]

### Features
1. [#657](https://github.com/influxdata/influxdb-client-python/pull/657): Prefer datetime.fromisoformat over dateutil.parse in Python 3.11+ 
1. [#658](https://github.com/influxdata/influxdb-client-python/pull/658): Add `find_buckets_iter` function that allow iterate through all pages of buckets.

## 1.43.0 [2024-05-17]

### Bug Fixes
1. [#655](https://github.com/influxdata/influxdb-client-python/pull/655): Replace deprecated `urllib` calls `HTTPResponse.getheaders()` and `HTTPResponse.getheader()`.

### Others
1. [#654](https://github.com/influxdata/influxdb-client-python/pull/654): Enable packaging type information - `py.typed`

## 1.42.0 [2024-04-17]

### Bug Fixes
1. [#648](https://github.com/influxdata/influxdb-client-python/pull/648): Fix `DataFrame` serialization with `NaN` values

## 1.41.0 [2024-03-01]

### Features
1. [#643](https://github.com/influxdata/influxdb-client-python/pull/643): Add a support for Python 3.12

### Bug Fixes
1. [#636](https://github.com/influxdata/influxdb-client-python/pull/636): Handle missing data in data frames
1. [#638](https://github.com/influxdata/influxdb-client-python/pull/638), [#642](https://github.com/influxdata/influxdb-client-python/pull/642): Refactor DataFrame operations to avoid chained assignment and resolve FutureWarning in pandas, ensuring compatibility with pandas 3.0.
1. [#641](https://github.com/influxdata/influxdb-client-python/pull/641): Correctly dispose ThreadPoolScheduler in WriteApi

### Documentation
1. [#639](https://github.com/influxdata/influxdb-client-python/pull/639): Use Markdown for `README`

## 1.40.0 [2024-01-30]

### Features
1. [#625](https://github.com/influxdata/influxdb-client-python/pull/625): Make class `Point` equatable

### Bug Fixes
1. [#562](https://github.com/influxdata/influxdb-client-python/pull/562): Use `ThreadPoolScheduler` for `WriteApi`'s batch subject instead of `TimeoutScheduler` to prevent creating unnecessary threads repeatedly
1. [#631](https://github.com/influxdata/influxdb-client-python/pull/631): Logging HTTP requests without query parameters

### Documentation
1. [#635](https://github.com/influxdata/influxdb-client-python/pull/635): Fix render `README.rst` at GitHub

## 1.39.0 [2023-12-05]

### Features
1. [#616](https://github.com/influxdata/influxdb-client-python/pull/616): Add `find_tasks_iter` function that allow iterate through all pages of tasks.

## 1.38.0 [2023-10-02]

### Bug Fixes
1. [#601](https://github.com/influxdata/influxdb-client-python/pull/601): Use HTTResponse.headers to clear deprecation warning [urllib3]
1. [#610](https://github.com/influxdata/influxdb-client-python/pull/601): Use iloc to clear deprecation warning

### Documentation
1. [#566](https://github.com/influxdata/influxdb-client-python/pull/566): Fix Sphinx documentation build and add support `.readthedocs.yml` V2 configuration file

## 1.37.0 [2023-07-28]

### Breaking Changes

This release disables using of the HTTP proxy environment variables `HTTP_PROXY` and `HTTPS_PROXY` for the asynchronous HTTP client.
The proxy environment variables must be explicitly enabled in the client's configuration:

```python
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org",
                               client_session_kwargs={'trust_env': True}) as client:
    pass
```

This release introduces a support for new version of InfluxDB API definitions with following breaking changes:

- `User`, `UserResponse`, `ResourceMember` and `ResourceOwner` classes no longer supports `oauth_id` field
- `Task` class no longer supports `type` field
- `ScriptUpdateRequest` class no longer supports `name` field
- `UsersService.get_flags` operation is moved to `ConfigService`

### Features
1. [#586](https://github.com/influxdata/influxdb-client-python/pull/586): Add `config_name` key argument for ``from_config_file`` function to allow loading a specific configuration from a config file

### API
1. [#588](https://github.com/influxdata/influxdb-client-python/pull/588): Use the latest InfluxDB API definitions for generated APIs

### Bug Fixes
1. [#583](https://github.com/influxdata/influxdb-client-python/pull/583): Async HTTP client doesn't always use `HTTP_PROXY`/`HTTPS_PROXY` environment variables. [async/await]
1. [#584](https://github.com/influxdata/influxdb-client-python/pull/584): Parsing empty query result value as `numpy.NaN`
1. [#595](https://github.com/influxdata/influxdb-client-python/pull/595): The `Config-Encoding: identity` header will no longer be set by the `write_api` calls to a remote server

## 1.36.1 [2023-02-23]

### Bug Fixes
1. [#559](https://github.com/influxdata/influxdb-client-python/pull/559): Exceptions in callbacks can cause deadlocks

## 1.36.0 [2023-01-26]

### Features
1. [#536](https://github.com/influxdata/influxdb-client-python/pull/536): Query to `CSV` skip empty lines
1. [#538](https://github.com/influxdata/influxdb-client-python/pull/538): Configure types of `integer` fields when initializing `Point` from `dict` structure

## 1.35.0 [2022-12-01]

### Features
1. [#528](https://github.com/influxdata/influxdb-client-python/pull/528): Add `BucketSchemasService` to manage explicit bucket schemas to enforce column names, tags, fields, and data types for your data

### Bug Fixes
1. [#526](https://github.com/influxdata/influxdb-client-python/pull/526): Creating client instance from static configuration
1. [#531](https://github.com/influxdata/influxdb-client-python/pull/531): HTTP request return type for Management API [async/await]
1. [#534](https://github.com/influxdata/influxdb-client-python/pull/534): Use `HTTResponse.headers` to clear deprecation warning [urllib3]

### CI
1. [#523](https://github.com/influxdata/influxdb-client-python/pull/523): Add Python 3.11 to CI builds

## 1.34.0 [2022-10-27]

### Breaking Changes
1. [#509](https://github.com/influxdata/influxdb-client-python/pull/509): Rename `key_file` to `cert_key_file` inside the central [configuration class](https://github.com/influxdata/influxdb-client-python/blob/d011df72b528a45d305aa8accbe879b31be3280e/influxdb_client/configuration.py#L92)

### Features
1. [#510](https://github.com/influxdata/influxdb-client-python/pull/510): Allow to use client's optional configs for initialization from file or environment properties
1. [#509](https://github.com/influxdata/influxdb-client-python/pull/509): MTLS support for the InfluxDB Python client

### Bug Fixes
1. [#512](https://github.com/influxdata/influxdb-client-python/pull/512): Exception propagation for asynchronous `QueryApi` [async/await]
1. [#518](https://github.com/influxdata/influxdb-client-python/pull/518): Parsing query response with two-bytes UTF-8 character [async/await]
1. [#521](https://github.com/influxdata/influxdb-client-python/pull/521): Duplicated `debug` output

## 1.33.0 [2022-09-29]

### Features
1. [#498](https://github.com/influxdata/influxdb-client-python/pull/498): Add possibility to update user's password by `users_api`
1. [#502](https://github.com/influxdata/influxdb-client-python/pull/502): Add `FluxRecord.row` with response data stored in array

### Bug Fixes
1. [#497](https://github.com/influxdata/influxdb-client-python/pull/497): Parsing InfluxDB response with new line character in CSV column [async/await]

## 1.32.0 [2022-08-25]

:warning: This release drop supports for Python 3.6. As of 2021-12-23, 3.6 has reached the end-of-life phase of its release cycle. 3.6.15 was the final security release. For more info see: https://peps.python.org/pep-0494/#lifespan

### Bug Fixes
1. [#483](https://github.com/influxdata/influxdb-client-python/pull/483): Querying data if the `debug` is enabled
1. [#477](https://github.com/influxdata/influxdb-client-python/pull/477): Parsing date fails due to thread race
1. [#486](https://github.com/influxdata/influxdb-client-python/pull/486): Serializing DataFrames with columns starting with digits
1. [#491](https://github.com/influxdata/influxdb-client-python/pull/491): Creating `Tasks` with `import` statements

### Dependencies
1. [#472](https://github.com/influxdata/influxdb-client-python/pull/472): Update `RxPY` to `4.0.4`

### Others
1. [#472](https://github.com/influxdata/influxdb-client-python/pull/472): Drop supports for Python 3.6
1. [#495](https://github.com/influxdata/influxdb-client-python/pull/495): Add warning for measurement name starts with `#`

### Documentation
1. [#397](https://github.com/influxdata/influxdb-client-python/pull/397): Add an example: How to use RxPY to prepare batches by maximum bytes count
1. [#269](https://github.com/influxdata/influxdb-client-python/pull/269): Add new example: How to check connection credentials

## 1.31.0 [2022-07-29]

### Features
1. [#467](https://github.com/influxdata/influxdb-client-python/pull/467): Add possibility to initialize client by json file
1. [#450](https://github.com/influxdata/influxdb-client-python/pull/450): Improve Query UX - simplify serialization to JSON and add possibility to serialize query results as a flattened list of values

### Bug Fixes
1. [#462](https://github.com/influxdata/influxdb-client-python/pull/462): Redact the `Authorization` HTTP header from log

## 1.30.0 [2022-06-24]

### Features
1. [#440](https://github.com/influxdata/influxdb-client-python/pull/440): Add possibility to specify timestamp column and its timezone [DataFrame]

### Bug Fixes
1. [#457](https://github.com/influxdata/influxdb-client-python/pull/457): Formatting nanoseconds to Flux AST

### Dependencies
1. [#449](https://github.com/influxdata/influxdb-client-python/pull/449): Remove `pytz` library

## 1.29.1 [2022-05-23]

### Bug Fixes
1. [#443](https://github.com/influxdata/influxdb-client-python/pull/443): Initialization of the client without auth credentials

## 1.29.0 [2022-05-20]

### Breaking Changes
1. [#433](https://github.com/influxdata/influxdb-client-python/pull/433): Rename `InvocableScripts` to `InvokableScripts`

### Features
1. [#435](https://github.com/influxdata/influxdb-client-python/pull/435): Add possibility to authenticate by `username/password`

### Dependencies
1. [#439](https://github.com/influxdata/influxdb-client-python/pull/439): Remove `six` library

### Documentation
1. [#434](https://github.com/influxdata/influxdb-client-python/pull/434): How the client uses [logging](https://docs.python.org/3/library/logging.html)

## 1.28.0 [2022-04-19]

### Features
1. [#413](https://github.com/influxdata/influxdb-client-python/pull/413): Add support for `async/await` with asyncio via `InfluxDBClientAsync`, for more info see: **How to use Asyncio**

### Bug Fixes
1. [#425](https://github.com/influxdata/influxdb-client-python/pull/425): Improve error message if there is no `organization` with required `name`

## 1.27.0 [2022-03-18]

### Features
1. [#412](https://github.com/influxdata/influxdb-client-python/pull/412): `DeleteApi` uses default value from `InfluxDBClient.org` if an `org` parameter is not specified
1. [#405](https://github.com/influxdata/influxdb-client-python/pull/405): Add `InfluxLoggingHandler`. A handler to use the client in native python logging.
1. [#404](https://github.com/influxdata/influxdb-client-python/pull/404): Add `InvokableScriptsApi` to create, update, list, delete and invoke scripts by seamless way

### Bug Fixes
1. [#419](https://github.com/influxdata/influxdb-client-python/pull/419): Use `allowed_methods` to clear deprecation warning [urllib3]

### Dependencies
1. [#419](https://github.com/influxdata/influxdb-client-python/pull/419): Update dependencies:
   - `urllib3` to 1.26.0

### CI
1. [#411](https://github.com/influxdata/influxdb-client-python/pull/411): Use new Codecov uploader for reporting code coverage

## 1.26.0 [2022-02-18]

### Breaking Changes

This release introduces a support for new version of InfluxDB OSS API definitions - [oss.yml](https://github.com/influxdata/openapi/blob/master/contracts/oss.yml). The following breaking changes are in underlying API services and doesn't affect common apis such as - `WriteApi`, `QueryApi`, `BucketsApi`, `OrganizationsApi`...
- Add `LegacyAuthorizationsService` to deal with legacy authorizations
- Add `ResourceService` to retrieve all knows resources
- Add `BackupService` to represents the data backup functions of InfluxDB
- Add `ReplicationsService` to represents the replication functions of InfluxDB
- Add `RestoreService` to represents the data restore functions of InfluxDB
- Add `ConfigService` to retrieve InfluxDB's runtime configuration
- Add `RemoteConnectionsService` to deal with registered remote InfluxDB connections
- Add `TelegrafPluginsService` to retrieve all Telegraf's plugins
- Update `TemplatesService` to deal with `Stack` and `Template` API
- `DBRPsService`:
  - doesn't requires `org_id` parameter for operations
  - `get_dbr_ps_id` operation uses `DBRPGet` as a type of result
  - `patch_dbrpid` operation uses `DBRPGet` as a type of result
  - `post_dbrp` operation uses `DBRPCreate` as a type of request
- `DefaultService`:
  - `get_routes` operation is moved to `RoutesService`
  - `get_telegraf_plugin` operation is moved to `TelegrafsService`
  - `post_signin` operation is moved to `SigninService`
  - `post_signout` operation is moved to `SignoutService`
- `OrganizationsService`:
  - `get_orgs_id_secrets` operation is moved to `SecretsService`
  - `patch_orgs_id_secrets` operation is moved to `SecretsService`
  - `post_orgs_id_secrets` operation is moved to `SecretsService`
- Remove `DocumentApi` in favour of [InfluxDB Community Templates](https://github.com/influxdata/community-templates). For more info see - [influxdb#19300](https://github.com/influxdata/influxdb/pull/19300), [openapi#192](https://github.com/influxdata/openapi/pull/192)
- `TelegrafsService` uses `TelegrafPluginRequest` to create `Telegraf` configuration
- `TelegrafsService` uses `TelegrafPluginRequest` to update `Telegraf` configuration

### API
1. [#399](https://github.com/influxdata/influxdb-client-python/pull/399): Use the latest InfluxDB OSS API definitions to generated APIs

### Bug Fixes
1. [#408](https://github.com/influxdata/influxdb-client-python/pull/408): Improve error message when the client cannot find organization by name
1. [#407](https://github.com/influxdata/influxdb-client-python/pull/407): Use `pandas.concat()` instead of deprecated `DataFrame.append()` [DataFrame]

## 1.25.0 [2022-01-20]

### Features
1. [#393](https://github.com/influxdata/influxdb-client-python/pull/393): Add callback function for getting profilers output with example and test

### Bug Fixes
1. [#375](https://github.com/influxdata/influxdb-client-python/pull/375): Construct `InfluxDBError` without HTTP response
1. [#378](https://github.com/influxdata/influxdb-client-python/pull/378): Correct serialization DataFrame with nan values [DataFrame]
1. [#384](https://github.com/influxdata/influxdb-client-python/pull/384): Timeout can be specified as a `float`
1. [#380](https://github.com/influxdata/influxdb-client-python/pull/380): Correct data types for querying [DataFrame]
1. [#391](https://github.com/influxdata/influxdb-client-python/pull/391): Ping function uses debug for log

### Documentation
1. [#395](https://github.com/influxdata/influxdb-client-python/pull/395): Add an example How to use create a Task by API

### CI
1. [#370](https://github.com/influxdata/influxdb-client-python/pull/370): Add Python 3.10 to CI builds

## 1.24.0 [2021-11-26]

### Features
1. [#358](https://github.com/influxdata/influxdb-client-python/pull/358): Update management API:
   - `BucketsApi` - add possibility to: `update`
   - `OrganizationsApi` - add possibility to: `update`
   - `UsersApi` - add possibility to: `update`, `delete`, `find`
1. [#356](https://github.com/influxdata/influxdb-client-python/pull/356): Add `MultiprocessingWriter` to write data in independent OS process

### Bug Fixes
1. [#359](https://github.com/influxdata/influxdb-client-python/pull/359): Correct serialization empty columns into LineProtocol [DataFrame]

## 1.23.0 [2021-10-26]

### Deprecates
 - `InfluxDBClient.health()`: instead use `InfluxDBClient.ping()`

### Features
1. [#352](https://github.com/influxdata/influxdb-client-python/pull/352): Add `PingService` to check status of OSS and Cloud instance

### Documentation
1. [#344](https://github.com/influxdata/influxdb-client-python/pull/344): Add an example How to use Invokable scripts Cloud API

## 1.22.0 [2021-10-22]

### Features
1. [#330](https://github.com/influxdata/influxdb-client-python/pull/330): Add support for write structured data - `NamedTuple`, `Data Classes`
1. [#335](https://github.com/influxdata/influxdb-client-python/pull/335): Add support for custom precision for index specified as number [DataFrame]
1. [#341](https://github.com/influxdata/influxdb-client-python/pull/341): Add support for handling batch events

### Bug Fixes
1. [#348](https://github.com/influxdata/influxdb-client-python/pull/348): Optimize appending new columns to Pandas DataFrame [DataFrame]

### Documentation
1. [#331](https://github.com/influxdata/influxdb-client-python/pull/331): Add [Migration Guide](MIGRATION_GUIDE.rst)
1. [#341](https://github.com/influxdata/influxdb-client-python/pull/341): How to handle client errors

## 1.21.0 [2021-09-17]

### Features
1. [#319](https://github.com/influxdata/influxdb-client-python/pull/319): Add supports for array expressions in query parameters
2. [#320](https://github.com/influxdata/influxdb-client-python/pull/320): Add JSONEncoder to encode query results to JSON
1. [#317](https://github.com/influxdata/influxdb-client-python/pull/317): `delete_api` also accept `datetime` as a value for `start` and `stop`

### Bug Fixes
1. [#321](https://github.com/influxdata/influxdb-client-python/pull/321): Fixes return type for dashboard when `include=properties` is used

### CI
1. [#327](https://github.com/influxdata/influxdb-client-python/pull/327): Switch to next-gen CircleCI's convenience images

## 1.20.0 [2021-08-20]

### Features
1. [#281](https://github.com/influxdata/influxdb-client-python/pull/281): `FluxTable`, `FluxColumn` and `FluxRecord` objects have helpful reprs
1. [#293](https://github.com/influxdata/influxdb-client-python/pull/293): `dataframe_serializer` supports batching
1. [#301](https://github.com/influxdata/influxdb-client-python/pull/301): Add `proxy_headers` to configuration options
1. [#306](https://github.com/influxdata/influxdb-client-python/pull/306): Supports `numpy` type in serialization to Line protocol

### Documentation
1. [#301](https://github.com/influxdata/influxdb-client-python/pull/301): How to configure proxy

### Bug Fixes
1. [#283](https://github.com/influxdata/influxdb-client-python/pull/283): Set proxy server in config file
1. [#290](https://github.com/influxdata/influxdb-client-python/pull/290): `Threshold` domain models mapping
1. [#290](https://github.com/influxdata/influxdb-client-python/pull/290): `DashboardService` responses types
1. [#303](https://github.com/influxdata/influxdb-client-python/pull/303): Backslash escaping in serialization to Line protocol
1. [#312](https://github.com/influxdata/influxdb-client-python/pull/312): Zip structure for AWS Lambda

### CI
1. [#299](https://github.com/influxdata/influxdb-client-python/pull/299): Deploy package to [Anaconda.org](https://anaconda.org/influxdata/influxdb_client)

## 1.19.0 [2021-07-09]

### Features
1. [#264](https://github.com/influxdata/influxdb-client-python/pull/264): Org parameter can be specified as ID, Name or Organization Object [write, query]

### Deprecated
1. [#264](https://github.com/influxdata/influxdb-client-python/pull/264): Deprecated `org_id` options BucketsApi.create_bucket in favor of `org` parameter

### Bug Fixes
1. [#270](https://github.com/influxdata/influxdb-client-python/pull/270): Supports `write_precision` for write Pandas DataFrame

## 1.18.0 [2021-06-04]

### Breaking Changes

This release introduces a support for new InfluxDB OSS API definitions - [oss.yml](https://github.com/influxdata/openapi/blob/master/contracts/oss.yml). The following breaking changes are in underlying API services and doesn't affect common apis such as - `WriteApi`, `QueryApi`, `BucketsApi`, `OrganizationsApi`...
- `AuthorizationsService` uses `AuthorizationPostRequest` to create `Authorization`
- `BucketsService` uses `PatchBucketRequest` to update `Bucket`
- `DashboardsService` uses `PatchDashboardRequest` to update `Dashboard`
- `DeleteService` is used to delete time series date instead of `DefaultService`
- `DBRPs` contains list of `DBRP` in `content` property
- `OrganizationsService` uses `PostOrganizationRequest` to create `Organization`
- `Run` contains list of `LogEvent` in `log` property
- `OrganizationsService` uses `PatchOrganizationRequest` to update `Organization`
- `OnboardingResponse` uses `UserResponse` as `user` property
- `ResourceMember` and `ResourceOwner` inherits from `UserResponse`
- `Users` contains list of `UserResponse` in `users` property
- `UsersService` uses `UserResponse` as a response to requests

### Features
1. [#237](https://github.com/influxdata/influxdb-client-python/pull/237): Use kwargs to pass query parameters into API list call - useful for the ability to use pagination.
1. [#241](https://github.com/influxdata/influxdb-client-python/pull/241): Add detail error message for not supported type of `Point.field`
1. [#238](https://github.com/influxdata/influxdb-client-python/pull/238): Add possibility to specify default `timezone` for datetimes without `tzinfo`
1. [#262](https://github.com/influxdata/influxdb-client-python/pull/263): Add option `auth_basic` to allow proxied access to InfluxDB 1.8.x compatibility API

### Bug Fixes
1. [#254](https://github.com/influxdata/influxdb-client-python/pull/254): Serialize `numpy` floats into LineProtocol

### Documentation
1. [#255](https://github.com/influxdata/influxdb-client-python/pull/255): Fix invalid description for env var `INFLUXDB_V2_CONNECTION_POOL_MAXSIZE`

### API
1. [#261](https://github.com/influxdata/influxdb-client-python/pull/261): Use InfluxDB OSS API definitions to generated APIs

## 1.17.0 [2021-04-30]

### Features
1. [#203](https://github.com/influxdata/influxdb-client-python/issues/219): Bind query parameters
1. [#225](https://github.com/influxdata/influxdb-client-python/pull/225): Exponential random backoff retry strategy

### Bug Fixes
1. [#222](https://github.com/influxdata/influxdb-client-python/pull/222): Pass configured timeout to HTTP client
1. [#218](https://github.com/influxdata/influxdb-client-python/pull/218): Support for `with .. as ..` statement
1. [#232](https://github.com/influxdata/influxdb-client-python/pull/232): Specify package requirements in `setup.py`
1. [#235](https://github.com/influxdata/influxdb-client-python/pull/235): Write a dictionary-style object without tags

## 1.16.0 [2021-04-01]

### Features
1. [#203](https://github.com/influxdata/influxdb-client-python/pull/203): Configure a client via TOML file
1. [#215](https://github.com/influxdata/influxdb-client-python/pull/215): Configure a connection pool maxsize

### Bug Fixes
1. [#206](https://github.com/influxdata/influxdb-client-python/pull/207): Use default (system) certificates instead of Mozilla's root certificates (certifi.where())
1. [#217](https://github.com/influxdata/influxdb-client-python/pull/217): Fix clone_task function

### API
1. [#209](https://github.com/influxdata/influxdb-client-python/pull/209): Allow setting shard-group durations for buckets via API

### Documentation
1. [#202](https://github.com/influxdata/influxdb-client-python/pull/202): Added an example how to use RxPY and sync batching
1. [#213](https://github.com/influxdata/influxdb-client-python/pull/213): Added an example how to use Buckets API

## 1.15.0 [2021-03-05]

### Bug Fixes
1. [#193](https://github.com/influxdata/influxdb-client-python/pull/193): Fixed `tasks_api` to use proper function to get `Run`

### Documentation
1. [#200](https://github.com/influxdata/influxdb-client-python/pull/200): Updated docs, examples, tests: use `close` instead of `__del__`.

### CI
1. [#199](https://github.com/influxdata/influxdb-client-python/pull/199): Updated stable image to `influxdb:latest` and nightly to `quay.io/influxdb/influxdb:nightly`

## 1.14.0 [2021-01-29]

### Features
1. [#176](https://github.com/influxdata/influxdb-client-python/pull/179): Allow providing proxy option to InfluxDBClient

### CI
1. [#179](https://github.com/influxdata/influxdb-client-python/pull/179): Updated default docker image to v2.0.3

### Bug Fixes
1. [#183](https://github.com/influxdata/influxdb-client-python/pull/183): Fixes to DataFrame writing.
1. [#181](https://github.com/influxdata/influxdb-client-python/pull/181): Encode Point whole numbers without trailing `.0`

### Documentation
1. [#189](https://github.com/influxdata/influxdb-client-python/pull/189): Updated docs about `DeleteApi`.

## 1.13.0 [2020-12-04]

### Features
1. [#171](https://github.com/influxdata/influxdb-client-python/pull/171): CSV parser is able to parse export from UI

### Bug Fixes
1. [#170](https://github.com/influxdata/influxdb-client-python/pull/170): Skip DataFrame rows without data - all fields are nan.

### CI
1. [#175](https://github.com/influxdata/influxdb-client-python/pull/175): Updated default docker image to v2.0.2

## 1.12.0 [2020-10-30]

1. [#163](https://github.com/influxdata/influxdb-client-python/pull/163): Added support for Python 3.9

### Features
1. [#161](https://github.com/influxdata/influxdb-client-python/pull/161): Added logging message for retries

### Bug Fixes
1. [#164](https://github.com/influxdata/influxdb-client-python/pull/164): Excluded tests from packaging

## 1.11.0 [2020-10-02]

### Features
1. [#152](https://github.com/influxdata/influxdb-client-python/pull/152): WriteApi supports generic Iterable type
1. [#158](https://github.com/influxdata/influxdb-client-python/pull/158): Added possibility to specify certificate file path to verify the peer

### API
1. [#151](https://github.com/influxdata/influxdb-client-python/pull/151): Default port changed from 9999 -> 8086
1. [#156](https://github.com/influxdata/influxdb-client-python/pull/156): Removed labels in organization API, removed Pkg* structure and package service

### Bug Fixes
1. [#154](https://github.com/influxdata/influxdb-client-python/pull/154): Fixed escaping string fields in DataFrame serialization

## 1.10.0 [2020-08-14]

### Features
1. [#140](https://github.com/influxdata/influxdb-client-python/pull/140): Added exponential backoff strategy for batching writes, Allowed to configure default retry strategy. Default value for `retry_interval` is 5_000 milliseconds.
1. [#136](https://github.com/influxdata/influxdb-client-python/pull/136): Allows users to skip of verifying SSL certificate
1. [#143](https://github.com/influxdata/influxdb-client-python/pull/143): Skip of verifying SSL certificate could be configured via config file or environment properties
1. [#141](https://github.com/influxdata/influxdb-client-python/pull/141): Added possibility to use datetime nanoseconds precision by `pandas.Timestamp`
1. [#145](https://github.com/influxdata/influxdb-client-python/pull/145): Api generator was moved to influxdb-clients-apigen

## 1.9.0 [2020-07-17]

### Features
1. [#112](https://github.com/influxdata/influxdb-client-python/pull/113): Support timestamp with different timezone in _convert_timestamp
1. [#120](https://github.com/influxdata/influxdb-client-python/pull/120): ciso8601 is an optional dependency and has to be installed separably
1. [#121](https://github.com/influxdata/influxdb-client-python/pull/121): Added query_data_frame_stream method
1. [#132](https://github.com/influxdata/influxdb-client-python/pull/132): Use microseconds resolutions for data points

### Bug Fixes
1. [#117](https://github.com/influxdata/influxdb-client-python/pull/117): Fixed appending default tags for single Point
1. [#115](https://github.com/influxdata/influxdb-client-python/pull/115): Fixed serialization of `\n`, `\r` and `\t` to Line Protocol, `=` is valid sign for measurement name
1. [#118](https://github.com/influxdata/influxdb-client-python/issues/118): Fixed serialization of DataFrame with empty (NaN) values
1. [#130](https://github.com/influxdata/influxdb-client-python/pull/130): Use `Retry-After` header value for Retryable error codes

## 1.8.0 [2020-06-19]

### Features
1. [#92](https://github.com/influxdata/influxdb-client-python/issues/92): Optimize serializing Pandas DataFrame for writing

### API
1. [#110](https://github.com/influxdata/influxdb-client-python/pull/110): Removed log system from Bucket, Dashboard, Organization, Task and Users API - [influxdb#18459](https://github.com/influxdata/influxdb/pull/18459), Update swagger to latest version

### Bug Fixes
1. [#105](https://github.com/influxdata/influxdb-client-python/pull/105): Fixed mapping dictionary without timestamp and tags into LineProtocol
1. [#108](https://github.com/influxdata/influxdb-client-python/pull/108): The WriteApi uses precision from Point instead a default precision

## 1.7.0 [2020-05-15]

### Features
1. [#79](https://github.com/influxdata/influxdb-client-python/issues/79): Added support for writing Pandas DataFrame

### Bug Fixes
1. [#85](https://github.com/influxdata/influxdb-client-python/issues/85): Fixed a possibility to generate empty write batch
2. [#86](https://github.com/influxdata/influxdb-client-python/issues/86): BREAKING CHANGE: Fixed parameters in delete api - now delete api accepts also bucket name and org name instead of only ids
1. [#93](https://github.com/influxdata/influxdb-client-python/pull/93): Remove trailing slash from connection URL

## 1.6.0 [2020-04-17]

### Documentation
1. [#75](https://github.com/influxdata/influxdb-client-python/issues/75): Updated docs to clarify how to use an org parameter
1. [#84](https://github.com/influxdata/influxdb-client-python/pull/84): Clarify how to use a client with InfluxDB 1.8

### Bug Fixes
1. [#72](https://github.com/influxdata/influxdb-client-python/issues/72): Optimize serializing data into Pandas DataFrame

## 1.5.0 [2020-03-13]

### Features
1. [#59](https://github.com/influxdata/influxdb-client-python/issues/59): Set User-Agent to influxdb-client-python/VERSION for all requests

### Bug Fixes
1. [#61](https://github.com/influxdata/influxdb-client-python/issues/61): Correctly parse CSV where multiple results include multiple tables
1. [#66](https://github.com/influxdata/influxdb-client-python/issues/66): Correctly close connection pool manager at exit
1. [#69](https://github.com/influxdata/influxdb-client-python/issues/69): `InfluxDBClient` and `WriteApi` could serialized by [pickle](https://docs.python.org/3/library/pickle.html#object.__getstate__) (python3.7 or higher)

## 1.4.0 [2020-02-14]

### Features
1. [#52](https://github.com/influxdata/influxdb-client-python/issues/52): Initialize client library from config file and environmental properties

### CI
1. [#54](https://github.com/influxdata/influxdb-client-python/pull/54): Add Python 3.7 and 3.8 to CI builds

### Bug Fixes
1. [#56](https://github.com/influxdata/influxdb-client-python/pull/56): Fix default tags for write batching, added new test
1. [#58](https://github.com/influxdata/influxdb-client-python/pull/58): Source distribution also contains: requirements.txt, extra-requirements.txt and test-requirements.txt

## 1.3.0 [2020-01-17]

### Features
1. [#50](https://github.com/influxdata/influxdb-client-python/issues/50): Implemented default tags

### API
1. [#47](https://github.com/influxdata/influxdb-client-python/pull/47): Updated swagger to latest version

### CI
1. [#49](https://github.com/influxdata/influxdb-client-python/pull/49): Added beta release to continuous integration

### Bug Fixes
1. [#48](https://github.com/influxdata/influxdb-client-python/pull/48): InfluxDBClient default org is used by WriteAPI

## 1.2.0 [2019-12-06]

### Features
1. [#44](https://github.com/influxdata/influxdb-client-python/pull/44): Optimized serialization into LineProtocol, Clarified how to use client for import large amount of data

### API
1. [#42](https://github.com/influxdata/influxdb-client-python/pull/42): Updated swagger to latest version

### Bug Fixes
1. [#45](https://github.com/influxdata/influxdb-client-python/pull/45): Pandas is a optional dependency and has to installed separably

## 1.1.0 [2019-11-19]

### Features
1. [#29](https://github.com/influxdata/influxdb-client-python/issues/29): Added support for serialise response into Pandas DataFrame

## 1.0.0 [2019-11-11]

### Features
1. [#24](https://github.com/influxdata/influxdb-client-python/issues/24): Added possibility to write dictionary-style object
1. [#27](https://github.com/influxdata/influxdb-client-python/issues/27): Added possibility to write bytes type of data
1. [#30](https://github.com/influxdata/influxdb-client-python/issues/30): Added support for streaming a query response
1. [#35](https://github.com/influxdata/influxdb-client-python/pull/35): FluxRecord supports dictionary-style access
1. [#31](https://github.com/influxdata/influxdb-client-python/issues/31): Added support for delete metrics

### API
1. [#28](https://github.com/bonitoo-io/influxdb-client-python/pull/28): Updated swagger to latest version

### Bug Fixes
1. [#19](https://github.com/bonitoo-io/influxdb-client-python/pull/19): Removed strict checking of enum values

### Documentation
1. [#22](https://github.com/bonitoo-io/influxdb-client-python/issues/22): Documented how to connect to InfluxCloud

## 0.0.2 [2019-09-26]

### Features
1. [#2](https://github.com/bonitoo-io/influxdb-client-python/issues/2): The write client is able to write data in batches (configuration: `batch_size`, `flush_interval`, `jitter_interval`, `retry_interval`)
1. [#5](https://github.com/bonitoo-io/influxdb-client-python/issues/5): Added support for gzip compression of query response and write body

### API
1. [#10](https://github.com/bonitoo-io/influxdb-client-python/pull/10): Updated swagger to latest version

### Bug Fixes
1. [#3](https://github.com/bonitoo-io/influxdb-client-python/issues/3): The management API correctly supports inheritance defined in Influx API
1. [#7](https://github.com/bonitoo-io/influxdb-client-python/issues/7): Drop NaN and infinity values from fields when writing to InfluxDB

### CI
1. [#11](https://github.com/bonitoo-io/influxdb-client-python/pull/11): Switch CI to CircleCI
1. [#12](https://github.com/bonitoo-io/influxdb-client-python/pull/12): CI generate code coverage report on CircleCI
