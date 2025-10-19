calculate_ratio
---------------

Calculates the ratio between two numeric columns and stores the result in a new column.

Metadata:
    - **Name**: `calculate_ratio`
    - **Version**: `1.0.0`
    - **Author**: pandaflow team


calculate_ratio schema
~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: calculate_ratio Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'calculate_ratio'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will store the calculated ratio.

   * - ``numerator``
     - str
     - True
     - Name of the column to use as the numerator in the ratio calculation.

   * - ``denominator``
     - str
     - True
     - Name of the column to use as the denominator in the ratio calculation.

   * - ``round_digits``
     - int
     - False
     - Optional number of digits to round the result to. If None, no rounding is applied.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/calculate_ratio/input.csv
   :header-rows: 1
   :widths: auto

calculate_ratio example
~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/calculate_ratio/pandaflow-config.json
   :language: json
   :linenos:
   :caption: calculate_ratio Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/calculate_ratio/output.csv
   :header-rows: 1
   :widths: auto