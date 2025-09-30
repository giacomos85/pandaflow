Overview
--------

Pandaflow is a schema-driven transformation framework for pandas DataFrames.  
It enables reproducible, rule-based data workflows using modular strategies, declarative configuration, and automated documentation.

Goals
~~~~~

- ✅ Simplify data transformation with reusable, testable strategies
- ✅ Empower contributors with clear onboarding and documentation
- ✅ Automate validation and CI integration for rule-based workflows
- ✅ Support schema evolution and strategy extensibility

Architecture
~~~~~~~~~~~~

Pandaflow is built around three core concepts:

- **Strategies**: Modular transformation classes that implement a specific logic (e.g. `copy`, `drop`, `filter`)
- **Rules**: Declarative JSON objects that define how a strategy should behave
- **Engine**: Applies strategies to input DataFrames using validated transformations

Each strategy is self-contained, versioned, and documented with input/output examples and test cases.

