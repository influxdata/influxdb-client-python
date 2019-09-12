#!/usr/bin/env bash

set -e

#
# Install requirements
#
python --version
pip install -r requirements.txt
pip install -r test-requirements.txt

#
# Prepare for test results
#
mkdir test-reports || true

#
# Test
#
pytest influxdb2_test --junitxml=test-reports/junit.xml

