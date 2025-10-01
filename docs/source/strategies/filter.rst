filter
------

The **filter** strategy selects rows from a DataFrame based on a user-defined formula.  
It supports logical expressions involving column values and can optionally format the output field.

Metadata:
    - **Name**: `filter`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


filter schema
~~~~~~~~~~~~~

.. list-table:: filter Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'filter'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the column to apply the filtering formula to.

   * - ``formula``
     - str
     - True
     - Filtering expression to evaluate. Should be a valid pandas-compatible formula (e.g., 'value > 10').

   * - ``formatter``
     - Optional
     - False
     - Optional formatter function name to apply to the field before evaluating the formula.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/filter/input.csv
   :header-rows: 1
   :widths: auto

filter example
~~~~~~~~~~~~~~
.. literalinclude:: ../data/filter/pandaflow-config.json
   :language: json
   :linenos:
   :caption: filter Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/filter/output.csv
   :header-rows: 1
   :widths: auto