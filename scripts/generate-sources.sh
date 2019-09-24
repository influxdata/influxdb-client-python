#!/usr/bin/env bash

#!/usr/bin/env bash

SCRIPT_PATH="$( cd "$(dirname "$0")" || exit ; pwd -P )"

# Generate OpenAPI generator
cd "${SCRIPT_PATH}"/../openapi-generator/ || exit
mvn clean install -DskipTests

# delete old sources
rm "${SCRIPT_PATH}"/../influxdb_client/domain/*.py
rm "${SCRIPT_PATH}"/../influxdb_client/service/*.py

# Generate client
cd "${SCRIPT_PATH}"/ || exit
mvn org.openapitools:openapi-generator-maven-plugin:generate

cp "${SCRIPT_PATH}"/../influxdb_client/service/__init__.py "${SCRIPT_PATH}"/../influxdb_client/client/
cp "${SCRIPT_PATH}"/../influxdb_client/service/__init__.py "${SCRIPT_PATH}"/../influxdb_client/client/write/
cp "${SCRIPT_PATH}"/../influxdb_client/service/__init__.py "${SCRIPT_PATH}"/../tests/