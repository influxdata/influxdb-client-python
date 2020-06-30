#!/usr/bin/env bash

set -e

ENABLED_CISO_8601="${ENABLED_CISO_8601:-true}"

#
# Install requirements
#
python --version
pip install -r requirements.txt --user
pip install -r extra-requirements.txt --user
pip install -r test-requirements.txt --user
if [ "$ENABLED_CISO_8601" = true ] ; then
  echo "ciso8601 is enabled"
  pip install -r ciso-requirements.txt --user
else
  echo "ciso8601 is disabled"
fi
pip install pytest pytest-cov --user
pip install twine --user
python setup.py sdist bdist_wheel
twine check dist/*

#
# Prepare for test results
#
mkdir test-reports || true

#
# Test
#
pytest tests --junitxml=test-reports/junit.xml --cov=./ --cov-report xml:coverage.xml

