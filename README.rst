py-rabbit
===================================

A wrapper around Kombu to use RabbitMQ, specialy on docker images

Started on 2023-08-18


Features (Optional)
-----

- List some features
- From this repository

Installation
-----
To install py-rabbit, follow these steps:

    1. Clone the repository: ``git clone https://github.com/vliz-be-opsci/py-rabbit.git``
    2. Navigate to the project directory: ``cd py-rabbit``
    3. Install the required dependencies: ``make init``
    4. (Optional) Install additional dependencies for development: ``make init-dev``

Usage
-----
Here's an example of how to use py-rabbit:

.. code-block:: python

    import py-rabbit

    # Implement a code example



For more detailed information on the usage of py-rabbit, refer to the `official documentation <https://open-science.vliz.be/py-rabbit/>`.



Contributing
-----

We welcome contributions from the community to enhance py-rabbit. If you'd like to contribute, please follow these guidelines:

    1. Fork the repository and create a new branch for your feature or bug fix.
    2. Make your changes and ensure that the code adheres to the project's coding style.
    3. Write unit tests to cover your changes and ensure they pass.
    4. Submit a pull request with a clear description of your changes and the problem they solve.

For more information on contributing to Pykg2tbl, please refer to the `contribution guidelines </CONTRIBUTING.rst>`.


Getting Started
-----
Start using this project with poetry


.. code-block:: bash

    $ make init       # install dependencies
    $ make init-dev   # includes the previous + adds dependencies for developers

Build Docs

.. code-block:: bash

    $ make docs


Developers
----------

Run Tests

.. code-block:: bash

    $ make test                                                   # to run all tests
    $ PYTEST_LOGCONF=debug-logconf.yml python tests/test_demo.py  # to run a specific test with specific logging
    $ make test-coverage                                          # to run all tests and check the test coverage


Check the code-style and syntax (flake8, black, isort)

.. code-block:: bash

    $ make check


