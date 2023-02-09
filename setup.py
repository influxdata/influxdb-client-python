#!/usr/bin/env python

from pathlib import Path

from setuptools import setup, find_packages  # noqa: H301

requires = [
    'reactivex >= 4.0.4',
    'certifi >= 14.05.14',
    'python_dateutil >= 2.5.3',
    'setuptools >= 21.0.0',
    'urllib3 >= 1.26.0'
]

test_requires = [
    'flake8>=5.0.3',
    'coverage>=4.0.3',
    'nose>=1.3.7',
    'pluggy>=0.3.1',
    'py>=1.4.31',
    'randomize>=0.13',
    'pytest>=5.0.0',
    'pytest-cov>=3.0.0',
    'pytest-timeout>=2.1.0',
    'httpretty==1.0.5',
    'psutil>=5.6.3',
    'aioresponses>=0.7.3',
    'sphinx==1.8.5',
    'sphinx_rtd_theme',
    'jinja2==3.1.2'
]

extra_requires = [
    'pandas>=0.25.3',
    'numpy'
]

ciso_requires = [
    'ciso8601>=2.1.1'
]

async_requires = [
    'aiohttp>=3.8.1',
    'aiocsv>=1.2.2'
]

with open('README.rst', 'r') as f:
    # Remove `class` text role as it's not allowed on PyPI
    lines = []
    for line in f:
        lines.append(line.replace(":class:`~", "`"))

    readme = "".join(lines)

NAME = "influxdb_client"

meta = {}
with open(Path(__file__).parent / 'influxdb_client' / 'version.py') as f:
    exec('\n'.join(line for line in f if line.startswith('VERSION')), meta)

setup(
    name=NAME,
    version=meta['VERSION'],
    description="InfluxDB 2.0 Python client library",
    long_description=readme,
    url="https://github.com/influxdata/influxdb-client-python",
    keywords=["InfluxDB", "InfluxDB Python Client"],
    tests_require=test_requires,
    install_requires=requires,
    extras_require={'extra': extra_requires, 'ciso': ciso_requires, 'async': async_requires, 'test': test_requires},
    long_description_content_type="text/x-rst",
    packages=find_packages(exclude=('tests*',)),
    test_suite='tests',
    python_requires='>=3.7',
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ])
