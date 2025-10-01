calculate_amount
----------------

The **calculate_amount** strategy computes a new column using a pandas-compatible formula.  
It’s ideal for deriving values from existing columns, applying arithmetic, or generating totals.

Metadata:
    - **Name**: `calculate_amount`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


calculate_amount schema
~~~~~~~~~~~~~~~~~~~~~~~

.. list-table:: calculate_amount Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - strategy name, must be 'calculate_amount'

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``formula``
     - str
     - True
     - A pandas-compatible expression (e.g. `"price * quantity"`)

   * - ``formatter``
     - Optional
     - False
     - Optional formatter (e.g. `"float_2dec"`)

   * - ``field``
     - str
     - False
     - The target column to store the result



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/calculate_amount/input.csv
   :header-rows: 1
   :widths: auto

calculate_amount example
~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/calculate_amount/pandaflow-config.json
   :language: json
   :linenos:
   :caption: calculate_amount Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/calculate_amount/output.csv
   :header-rows: 1
   :widths: auto