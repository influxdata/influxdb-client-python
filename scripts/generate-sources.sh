#!/usr/bin/env bash

#!/usr/bin/env bash

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

# Generate OpenAPI generator
cd ${SCRIPT_PATH}/../openapi-generator/
mvn clean install -DskipTests

# delete old sources
#rm ${SCRIPT_PATH}/../Client/InfluxDB.Client.Api/Domain/*.cs

# Generate client
cd ${SCRIPT_PATH}/
mvn org.openapitools:openapi-generator-maven-plugin:generate
#mvn -Dlibrary=asyncio org.openapitools:openapi-generator-maven-plugin:generate
#mvn -Dlibrary=tornado org.openapitools:openapi-generator-maven-plugin:generate


