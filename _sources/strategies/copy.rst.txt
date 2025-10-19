copy
----

The **copy** strategy transfers values from one column to another, optionally applying input parsing, output formatting, and fill-in logic.  
It is useful for duplicating fields, normalizing formats, or creating derived columns with fallback behavior.

Metadata:
    - **Name**: `copy`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


copy schema
~~~~~~~~~~~

.. list-table:: copy Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'copy'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will receive the copied or transformed value.

   * - ``source``
     - Optional
     - False
     - Name of the source column to copy from. If None, the field will be filled using 'fillna' or left empty.

   * - ``fillna``
     - Union
     - False
     - Optional fallback value to use if the source column is missing or contains nulls.

   * - ``parser``
     - Optional
     - False
     - Optional parser function name to apply to the source value before copying.

   * - ``formatter``
     - Optional
     - False
     - Optional formatter function name to apply after parsing, before assigning to the output column.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/copy/input.csv
   :header-rows: 1
   :widths: auto

copy example
~~~~~~~~~~~~
.. literalinclude:: ../data/copy/pandaflow-config.json
   :language: json
   :linenos:
   :caption: copy Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/copy/output.csv
   :header-rows: 1
   :widths: auto