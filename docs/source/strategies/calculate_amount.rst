Calculate Amount Strategy
=========================

The **calculate_amount** strategy computes a new column using a pandas-compatible formula.  
It’s ideal for deriving values from existing columns, applying arithmetic, or generating totals.

Metadata:
   - **Name**: `calculate_amount`
   - **Version**: `1.0.0`
   - **Author**: Pandaflow Team
   - **Description**: Calculates a new column using a formula and optional output formatting.

Rule Format:
   - `field`: The target column to store the result
   - `formula`: A pandas-compatible expression (e.g. `"price * quantity"`)
   - `output_rule`: Optional formatter (e.g. `"float_2dec"`)

.. literalinclude:: ../rules/calculate_amount.json
   :language: json
   :linenos:
   :caption: CalculateAmount Rule Example

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/calculate_amount_input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Output with Calculated Amount
   :file: ../data/calculate_amount_output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
~~~~~~~~~~~~~~

- The formula is evaluated using `pandas.eval()`
- If `output_rule` is provided, it formats the result (e.g. rounding)
- If the formula references missing columns, an error is raised
- The strategy overwrites the target column if it already exists

