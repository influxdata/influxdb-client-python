#!/usr/bin/env bash

#
# How to run script from ROOT path:
#   docker run --rm -it -v "${PWD}":/code/client -v ~/.m2:/root/.m2 -w /code maven:3-openjdk-8 /code/client/scripts/generate-sources.sh
#

#
# Download customized generator
#
git clone --single-branch --branch master https://github.com/bonitoo-io/influxdb-clients-apigen "/code/influxdb-clients-apigen"
mkdir -p /code/influxdb-clients-apigen/build/
ln -s /code/client /code/influxdb-clients-apigen/build/influxdb-client-python
cd /code/influxdb-clients-apigen/

#
# Download APIs contracts
#
wget https://raw.githubusercontent.com/influxdata/openapi/master/contracts/oss.yml -O "/code/influxdb-clients-apigen/oss.yml"
wget https://raw.githubusercontent.com/influxdata/openapi/master/contracts/cloud.yml -O "/code/influxdb-clients-apigen/cloud.yml"
wget https://raw.githubusercontent.com/influxdata/openapi/master/contracts/invocable-scripts.yml -O "/code/influxdb-clients-apigen/invocable-scripts.yml"

#
# Build generator
#
mvn -DskipTests -f /code/influxdb-clients-apigen/openapi-generator/pom.xml clean install

#
# Prepare customized contract
#
mvn -f /code/influxdb-clients-apigen/openapi-generator/pom.xml compile exec:java -Dexec.mainClass="com.influxdb.AppendCloudDefinitions" -Dexec.args="oss.yml cloud.yml"
mvn -f /code/influxdb-clients-apigen/openapi-generator/pom.xml compile exec:java -Dexec.mainClass="com.influxdb.MergeContracts" -Dexec.args="oss.yml invocable-scripts.yml"

#
# Generate sources
#
./generate-python.sh
