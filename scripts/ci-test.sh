#!/usr/bin/env bash

set -e

ENABLED_CISO_8601="${ENABLED_CISO_8601:-true}"

#
# Install requirements
#
python --version
pip install -e . --user
pip install -e .\[extra\] --user
pip install -e .\[test\] --user
if [ "$ENABLED_CISO_8601" = true ] ; then
  echo "ciso8601 is enabled"
  pip install -e .\[ciso\] --user
else
  echo "ciso8601 is disabled"
fi
pip install pytest pytest-cov --user

#
# Prepare for test results
#
mkdir test-reports || true

#
# Test
#
pytest tests --junitxml=test-reports/junit.xml --cov=./ --cov-report xml:coverage.xml

