lookup_external
---------------

Looks up values from an external CSV file based on a key column

Metadata:
    - **Name**: `lookup_external`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


lookup_external schema
~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: lookup_external Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'lookup_external'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will store the looked-up value.

   * - ``source``
     - str
     - True
     - Name of the column in the current DataFrame whose values will be used as lookup keys.

   * - ``file``
     - str
     - True
     - Path to the external file containing the lookup table (e.g., CSV or JSON).

   * - ``key``
     - str
     - True
     - Name of the column in the external file that contains the lookup keys.

   * - ``value``
     - str
     - True
     - Name of the column in the external file that contains the values to assign.

   * - ``not_found``
     - Optional
     - False
     - Optional fallback value to assign when a key is not found in the external file. If None, missing keys will result in nulls.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/lookup_external/input.csv
   :header-rows: 1
   :widths: auto

lookup_external example
~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/lookup_external/pandaflow-config.json
   :language: json
   :linenos:
   :caption: lookup_external Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/lookup_external/output.csv
   :header-rows: 1
   :widths: auto