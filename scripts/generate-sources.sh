#!/usr/bin/env bash

#!/usr/bin/env bash

SCRIPT_PATH="$( cd "$(dirname "$0")" || exit ; pwd -P )"

# Generate OpenAPI generator
cd "${SCRIPT_PATH}"/../openapi-generator/ || exit
mvn clean install -DskipTests

# delete old sources
rm "${SCRIPT_PATH}"/../influxdb2/models/*.py
rm "${SCRIPT_PATH}"/../influxdb2/api/*.py

# Generate client
cd "${SCRIPT_PATH}"/ || exit
mvn org.openapitools:openapi-generator-maven-plugin:generate


