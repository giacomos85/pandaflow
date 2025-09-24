Constant
--------

The **constant** strategy sets a specified column in a DataFrame to a fixed value across all rows.  
This is useful for injecting default flags, labels, or placeholder values during data transformation.

Metadata
~~~~~~~~

- **Name**: `constant`
- **Version**: `1.0.0`
- **Author**: Pandaflow Team
- **Description**: Sets a specified column to a constant value.

Rule Format
~~~~~~~~~~~

Rules for this strategy must define the target field and the constant value to assign.

.. literalinclude:: ../rules/constant.json
   :language: json
   :linenos:
   :caption: Example Rule Definition

Input Data
~~~~~~~~~~

The following table shows the input DataFrame before applying the strategy:

.. csv-table:: Input DataFrame
   :file: ../data/constant_input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

After applying the constant strategy, the output DataFrame includes the new column with the specified constant value:

.. csv-table:: Transformed Output
   :file: ../data/constant_output.csv
   :header-rows: 1
   :widths: auto

Usage Notes
~~~~~~~~~~~

- If the target field already exists, its values will be overwritten.
- If no `value` is provided in the rule, an empty string (`""`) will be used by default.
- This strategy is schema-safe and does not alter other columns.