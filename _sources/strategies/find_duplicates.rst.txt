find_duplicates
---------------

Identifies duplicate rows based on specified columns and marks retained entries using pandas-style keep semantics. Optionally resets the index for downstream compatibility.

Metadata:
    - **Name**: `find_duplicates`
    - **Version**: `1.0.0`
    - **Author**: pandaflow team


find_duplicates schema
~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: find_duplicates Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'find_duplicates'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``subset``
     - Optional
     - False
     - Optional list of column names to consider when identifying duplicates. If None, all columns are used.

   * - ``keep``
     - Optional
     - False
     - Determines which duplicate to mark as retained: 'first', 'last', or False to mark all duplicates.

   * - ``reset_index``
     - Optional
     - False
     - Whether to reset the DataFrame index after identifying duplicates. Defaults to False.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/find_duplicates/input.csv
   :header-rows: 1
   :widths: auto

find_duplicates example
~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/find_duplicates/pandaflow-config.json
   :language: json
   :linenos:
   :caption: find_duplicates Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/find_duplicates/output.csv
   :header-rows: 1
   :widths: auto