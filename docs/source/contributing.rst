Contributing to Pandaflow
-------------------------

We welcome contributions to **Pandaflow**, a schema-driven transformation framework for pandas workflows.  
Whether you're fixing bugs, adding strategies, improving documentation, or streamlining automation, your input helps make Pandaflow better for everyone.

Getting Started
~~~~~~~~~~~~~~~

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:

   .. code-block:: bash

      git clone https://github.com/your-username/pandaflow
      cd pandaflow

3. **Install dependencies** using Poetry:

   .. code-block:: bash

      poetry install

4. **Run tests** to verify your environment:

   .. code-block:: bash

      poetry run pytest

Strategy Development
~~~~~~~~~~~~~~~~~~~~

To add a new transformation strategy:

1. Create a new file in `pandaflow/strategies/` (e.g. `normalize.py`)
2. Define a `BaseRule` subclass for schema validation
3. Implement a `TransformationStrategy` subclass with `meta`, `validate_rule()`, and `apply()`
4. Add test cases in `tests/strategies/test_normalize.py`
5. Document the strategy in `docs/source/strategies/normalize.rst`

Documentation
~~~~~~~~~~~~~

- Use `.rst` files in `docs/source/` with Sphinx directives (`.. csv-table::`, `.. literalinclude::`)
- Store example transformations in `docs/source/rules/` and data in `docs/source/data/`
- Run the docs locally:

  .. code-block:: bash

     sphinx-build -b html docs/source docs/_build/html
     python -m http.server --directory docs/_build/html

- Or use live reload:

  .. code-block:: bash

     poetry run sphinx-autobuild docs/source docs/_build/html

Automation & CI
~~~~~~~~~~~~~~~

- All strategies must be covered by tests and validated in CI
- `.rst`-based test cases are auto-validated using `pandaflow.docs.testcases`
- Use `pandaflow.cli` commands to serve docs, export examples, and scaffold new strategies