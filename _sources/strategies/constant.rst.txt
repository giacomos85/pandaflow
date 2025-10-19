constant
--------

The **constant** strategy sets a specified column in a DataFrame to a fixed value across all rows.  
This is useful for injecting default flags, labels, or placeholder values during data transformation.

Metadata:
    - **Name**: `constant`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


constant schema
~~~~~~~~~~~~~~~

.. list-table:: constant Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'constant'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will receive the constant value.

   * - ``value``
     - str
     - False
     - Constant value to assign to the specified output column. Defaults to an empty string.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/constant/input.csv
   :header-rows: 1
   :widths: auto

constant example
~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/constant/pandaflow-config.json
   :language: json
   :linenos:
   :caption: constant Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/constant/output.csv
   :header-rows: 1
   :widths: auto