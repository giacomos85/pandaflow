sort_by_column
--------------

Sorts a DataFrame by one or more columns, with optional direction and NaN positioning.

Metadata:
    - **Name**: `sort_by_column`
    - **Version**: `1.0.0`
    - **Author**: pandaflow team


sort_by_column schema
~~~~~~~~~~~~~~~~~~~~~

.. list-table:: sort_by_column Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'sort_by_column'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``columns``
     - List
     - True
     - List of column names to sort by, in order of precedence.

   * - ``ascending``
     - Optional
     - False
     - List of booleans indicating sort direction for each column. True for ascending, False for descending. If None, defaults to ascending for all.

   * - ``na_position``
     - Literal
     - False
     - Position for NaN values in the sort order. 'first' places NaNs at the top, 'last' at the bottom.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/sort_by_column/input.csv
   :header-rows: 1
   :widths: auto

sort_by_column example
~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/sort_by_column/pandaflow-config.json
   :language: json
   :linenos:
   :caption: sort_by_column Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/sort_by_column/output.csv
   :header-rows: 1
   :widths: auto