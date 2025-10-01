drop_duplicates
---------------

Drops duplicate rows from a DataFrame, optionally based on a subset of columns.

Metadata:
    - **Name**: `drop_duplicates`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


drop_duplicates schema
~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: drop_duplicates Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'drop_duplicates'.

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
     - Determines which duplicate to keep: 'first', 'last', or False to drop all duplicates.

   * - ``reset_index``
     - Optional
     - False
     - Whether to reset the DataFrame index after dropping duplicates. Defaults to False.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/drop_duplicates/input.csv
   :header-rows: 1
   :widths: auto

drop_duplicates example
~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/drop_duplicates/pandaflow-config.json
   :language: json
   :linenos:
   :caption: drop_duplicates Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/drop_duplicates/output.csv
   :header-rows: 1
   :widths: auto