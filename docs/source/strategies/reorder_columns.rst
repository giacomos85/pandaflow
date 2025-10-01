reorder_columns
---------------

Reorders columns in a DataFrame to match a specified sequence,\ 
ensuring consistent layout for downstream processing or export.

Metadata:
    - **Name**: `reorder_columns`
    - **Version**: `1.0.0`
    - **Author**: pandaflow team


reorder_columns schema
~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: reorder_columns Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'reorder_columns'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``columns``
     - List
     - True
     - List of column names specifying the desired order. All listed columns must exist in the DataFrame.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/reorder_columns/input.csv
   :header-rows: 1
   :widths: auto

reorder_columns example
~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/reorder_columns/pandaflow-config.json
   :language: json
   :linenos:
   :caption: reorder_columns Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/reorder_columns/output.csv
   :header-rows: 1
   :widths: auto