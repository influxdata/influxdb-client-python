#!/usr/bin/env bash
#
# The MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

set -e

DEFAULT_DOCKER_REGISTRY="quay.io/influxdb/"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-$DEFAULT_DOCKER_REGISTRY}"

DEFAULT_INFLUXDB_V2_VERSION="nightly"
INFLUXDB_V2_VERSION="${INFLUXDB_V2_VERSION:-$DEFAULT_INFLUXDB_V2_VERSION}"
INFLUXDB_V2_IMAGE=${DOCKER_REGISTRY}influx:${INFLUXDB_V2_VERSION}

SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"


#
# InfluxDB 2.0
#
echo
echo "Restarting InfluxDB 2.0 [${INFLUXDB_V2_IMAGE}] ... "
echo

docker kill my-influxdb2 || true
docker rm my-influxdb2 || true
docker pull ${INFLUXDB_V2_IMAGE} || true
docker run \
       --detach \
       --name my-influxdb2 \
       --publish 9999:9999 \
       ${INFLUXDB_V2_IMAGE}

echo "Wait 5s to start InfluxDB 2.0"
sleep 5

echo
echo "Post onBoarding request, to setup initial user (my-user@my-password), org (my-org) and bucketSetup (my-bucket)"
echo

docker exec -it my-influxdb2 influx setup --username my-user --password my-password \
    --token my-token --org my-org --bucket my-bucket --retention 48 --force

## show created orgId
ORGID=`docker exec -it my-influxdb2 influx org find | grep my-org  | awk '{ print $1 }'`
echo "orgId="${ORGID}


