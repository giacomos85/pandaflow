merge
-----

Merges values from multiple columns into one

Metadata:
    - **Name**: `merge`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


merge schema
~~~~~~~~~~~~

.. list-table:: merge Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'merge'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will store the merged result.

   * - ``source``
     - Union
     - True
     - Single column name or list of column names whose values will be merged.

   * - ``replace``
     - Optional
     - False
     - Optional dictionary of string replacements to apply before merging. Keys are substrings to replace; values are their replacements.

   * - ``separator``
     - str
     - False
     - String used to separate values when merging multiple columns. Defaults to a single space.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/merge/input.csv
   :header-rows: 1
   :widths: auto

merge example
~~~~~~~~~~~~~
.. literalinclude:: ../data/merge/pandaflow-config.json
   :language: json
   :linenos:
   :caption: merge Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/merge/output.csv
   :header-rows: 1
   :widths: auto