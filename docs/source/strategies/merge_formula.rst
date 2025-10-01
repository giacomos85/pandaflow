merge_formula
-------------

Merges values from multiple columns into one using a formula or concatenation.

Metadata:
    - **Name**: `merge_formula`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


merge_formula schema
~~~~~~~~~~~~~~~~~~~~

.. list-table:: merge_formula Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'merge_formula'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will store the result of the formula or merged values.

   * - ``formula``
     - Optional
     - False
     - Optional pandas-compatible formula to compute the output value. If provided, overrides source/merge behavior.

   * - ``formatter``
     - Optional
     - False
     - Optional formatter function name to apply to the result after formula or merge.

   * - ``source``
     - Optional
     - False
     - Optional list of column names to merge if no formula is provided.

   * - ``separator``
     - Optional
     - False
     - String used to separate values when merging multiple columns. Defaults to a single space.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/merge_formula/input.csv
   :header-rows: 1
   :widths: auto

merge_formula example
~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/merge_formula/pandaflow-config.json
   :language: json
   :linenos:
   :caption: merge_formula Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/merge_formula/output.csv
   :header-rows: 1
   :widths: auto