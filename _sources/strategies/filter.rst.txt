Filter
------

The **filter** strategy selects rows from a DataFrame based on a user-defined formula.  
It supports logical expressions involving column values and can optionally format the output field.

Metadata
~~~~~~~~

- **Name**: `filter`
- **Version**: `1.0.0`
- **Author**: Pandaflow Team
- **Description**: Filters rows using a formula-based condition.

Transformation Format
~~~~~~~~~~~

The rule must include a `formula` that evaluates to a boolean mask.  
The `field` key is optional and used for formatting or targeting a specific column.

.. literalinclude:: ../data/filter/pandaflow-config.json
   :language: json
   :linenos:
   :caption: Filter Rule Example

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/filter/input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Filtered Output
   :file: ../data/filter/output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
~~~~~~~~~~~~~~

- The `formula` must return a boolean Series when evaluated.
- If `formula` is missing, a `ValueError` is raised.
- If the result of the formula is not boolean, a `ValueError` is raised.
- If `field` is not present in the DataFrame, no formatting is applied.
- Complex conditions using `and`, `or`, or `&`, `|` are supported.