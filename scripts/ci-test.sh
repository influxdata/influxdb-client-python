#!/usr/bin/env bash

set -e

ENABLED_CISO_8601="${ENABLED_CISO_8601:-true}"

#
# Install requirements
#
python --version
pip install . --user
pip install .\[extra\] --user
pip install .\[test\] --user
pip install .\[async\] --user
if [ "$ENABLED_CISO_8601" = true ] ; then
  echo "ciso8601 is enabled"
  pip install .\[ciso\] --user
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

