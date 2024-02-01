Development
===========

The following document covers how to develop the InfluxDB client library
locally. Including how to run tests and build the docs.

.. contents::
   :local:

tl;dr
^^^^^

.. code-block:: bash

    # from your forked repo, create and activate a virtualenv
    python -m virtualenv venv
    . venv/bin/activate
    # install the library as editable with all dependencies
    make install
    # make edits
    # run lint and tests
    make lint test

Getting Started With Development
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Install Python

    Most distributions include Python by default, so before going too far, try
    running ``python --version`` to see if it already exists. You may
    also have to specify ``python3 --version``, for example, on Ubuntu.

2. Fork and clone the repo

    The rest of this assumes you have cloned your fork of the upstream
    `client library <https://github.com/influxdata/influxdb-client-python>`_
    and are in the same directory of the forked repo.

3. Set up a virtual environment.

    Python virtual environments let you install specific versioned dependencies
    in a contained manner. This way, you do not pollute or have conflicts on
    your system with different versions.

    .. code-block:: bash

        python -m virtualenv venv
        . venv/bin/activate

    Having a shell prompt change via `starship <https://starship.rs/>`_
    or something similar is nice as it will let you know when and which
    virtual environment in you are in.

    To exit the virtual environment, run ``deactivate``.

4. Install the client library

    To install the local version of the client library run:

    .. code-block:: bash

        make install

    This will install the library as editable with all dependencies. This
    includes all dependencies that are used for all possible features as well
    as testing requirements.

5. Make changes and test

    At this point, a user can make the required changes necessary and run
    any tests or scripts they have.

    Before putting up a PR, the user should attempt to run the `lint` and `tests`
    locally. Lint will ensure the formatting of the code, while tests will run
    integration tests against an InfluxDB instance. For details on that set up
    see the next section.

    .. code-block:: bash

        make lint test

Linting
^^^^^^^

The library uses flake8 to do linting and can be run with:

    .. code-block:: bash

        make lint

Testing
^^^^^^^

The built-in tests assume that there is a running instance of InfluxDB 2.x up
and running. This can be accomplished by running the
``scripts/influxdb-restart.sh`` script. It will launch an InfluxDB 2.x instance
with Docker and make it available locally on port 8086.

Once InfluxDB is available, run all the tests with:

    .. code-block:: bash

        make test

Code Coverage
-------------

After running the tests, an HTML report of the tests is available in the
``htmlcov`` directory. Users can open ``html/index.html`` file in a browser
and see a full report for code coverage across the whole project. Clicking
on a specific file will show a line-by-line report of what lines were or
were not covered.

Documentation Building
^^^^^^^^^^^^^^^^^^^^^^^^

The docs are built using Sphinx. To build all the docs run:

    .. code-block:: bash

        make docs

This will build and produce a sample version of the web docs at
``docs/_build/html/index.html``. From there the user can view the entire site
and ensure changes are rendered correctly.
