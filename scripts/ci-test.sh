#!/usr/bin/env bash

set -e

#
# Install requirements
#
python --version
pip install -r requirements.txt
pip install -r test-requirements.txt
pip install pytest pytest-cov
pip install twine
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

