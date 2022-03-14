#!/usr/bin/env python

from pathlib import Path

from setuptools import setup, find_packages  # noqa: H301

requires = [
    'rx >= 3.0.1',
    'certifi >= 14.05.14',
    'six >= 1.10',
    'python_dateutil >= 2.5.3',
    'setuptools >= 21.0.0',
    'urllib3 >= 1.26.0',
    'pytz>=2019.1'
]

test_requires = [
    'coverage>=4.0.3',
    'nose>=1.3.7',
    'pluggy>=0.3.1',
    'py>=1.4.31',
    'randomize>=0.13',
    'pytest>=5.0.0',
    'httpretty==1.0.5',
    'psutil>=5.6.3'
]

extra_requires = [
    'pandas>=0.25.3',
    'numpy'
]

ciso_requires = [
    'ciso8601>=2.1.1'
]

with open('README.rst', 'r') as f:
    readme = f.read()

NAME = "influxdb_client"

meta = {}
with open(Path(__file__).parent / 'influxdb_client' / 'version.py') as f:
    exec('\n'.join(l for l in f if l.startswith('CLIENT_VERSION')), meta)

setup(
    name=NAME,
    version=meta['CLIENT_VERSION'],
    description="InfluxDB 2.0 Python client library",
    long_description=readme,
    url="https://github.com/influxdata/influxdb-client-python",
    keywords=["InfluxDB", "InfluxDB Python Client"],
    tests_require=test_requires,
    install_requires=requires,
    extras_require={'extra': extra_requires, 'ciso': ciso_requires, 'test': test_requires},
    long_description_content_type="text/x-rst",
    packages=find_packages(exclude=('tests*',)),
    test_suite='tests',
    python_requires='>=3.6',
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])
