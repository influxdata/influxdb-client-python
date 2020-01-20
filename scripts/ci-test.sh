#!/usr/bin/env bash

set -e

#
# Install requirements
#
python --version
pip install -r requirements.txt --user
pip install -r extra-requirements.txt --user
pip install -r test-requirements.txt --user
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

