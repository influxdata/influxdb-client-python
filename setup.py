#!/usr/bin/env python

from pathlib import Path

from setuptools import setup, find_packages  # noqa: H301

with open('requirements.txt', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

with open('test-requirements.txt', 'r') as f:
    test_requires = [x.strip() for x in f if x.strip()]

with open('extra-requirements.txt', 'r') as f:
    extra_requires = [x.strip() for x in f if x.strip()]

with open('README.rst', 'r') as f:
    readme = f.read()


NAME = "influxdb_client"

meta = {}
with open(Path(__file__).parent / 'influxdb_client' / '__init__.py') as f:
    exec('\n'.join(l for l in f if l.startswith('__')), meta)

setup(
    name=NAME,
    version=meta['__version__'],
    description="InfluxDB 2.0 Python client library",
    long_description=readme,
    url="https://github.com/influxdata/influxdb-client-python",
    keywords=["InfluxDB", "InfluxDB Python Client"],
    tests_require=test_requires,
    install_requires=requires,
    extras_require={'extra': extra_requires},
    long_description_content_type="text/x-rst",
    packages=find_packages(),
    test_suite='tests',
    python_requires='>=3.6',
    include_package_data=True,
    data_files=['requirements.txt', 'extra-requirements.txt', 'test-requirements.txt'],
    classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])

