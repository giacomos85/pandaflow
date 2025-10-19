drop_columns
------------

Drops one or more columns from the DataFrame.

Metadata:
    - **Name**: `drop_columns`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


drop_columns schema
~~~~~~~~~~~~~~~~~~~

.. list-table:: drop_columns Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'drop_columns'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``columns``
     - List
     - True
     - List of column names to drop from the DataFrame. All names must be strings.

   * - ``errors``
     - str
     - False
     - Behavior when a specified column is missing. Use 'raise' to throw an error or 'ignore' to skip silently.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/drop_columns/input.csv
   :header-rows: 1
   :widths: auto

drop_columns example
~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/drop_columns/pandaflow-config.json
   :language: json
   :linenos:
   :caption: drop_columns Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/drop_columns/output.csv
   :header-rows: 1
   :widths: auto