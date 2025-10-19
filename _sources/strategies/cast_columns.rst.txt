cast_columns
------------

The **cast_columns** strategy converts multiple columns to a specified type with optional error handling and fallback values.  
It supports numeric, string, and boolean casting and is useful for preparing data for filtering, merging, or exporting.

Metadata:
    - **Name**: `cast_columns`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


cast_columns schema
~~~~~~~~~~~~~~~~~~~

.. list-table:: cast_columns Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'cast_columns'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``fields``
     - List
     - True
     - List of column names to cast.

   * - ``target_type``
     - Literal
     - True
     - Target type to cast each column to.

   * - ``errors``
     - Literal
     - False
     - Error handling mode: 'raise' for strict, 'coerce' to convert invalid values to NaN, 'ignore' to skip errors.

   * - ``fallback``
     - Optional
     - False
     - Optional fallback value to fill in for failed conversions (e.g., 0, '').



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/cast_columns/input.csv
   :header-rows: 1
   :widths: auto

cast_columns example
~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/cast_columns/pandaflow-config.json
   :language: json
   :linenos:
   :caption: cast_columns Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/cast_columns/output.csv
   :header-rows: 1
   :widths: auto