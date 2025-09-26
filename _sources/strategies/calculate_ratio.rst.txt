Calculate Ratio Strategy
========================

Calculates the ratio between two numeric columns and stores the result in a new column.

Metadata:
    - name: "calculate_ratio"
    - version: "1.0.0"
    - author: "pandaflow team"

Rule Format:
    - numerator: str — Column name for numerator
    - denominator: str — Column name for denominator
    - field: str — Name of the output column
    - round_digits: Optional[int] — Number of decimal places to round to

Example Rule
------------

.. literalinclude:: ../rules/calculate_ratio.json
   :language: json
   :linenos:
   :caption: calculate_ratio Rule Example

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/calculate_ratio_input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Output with Calculated Amount
   :file: ../data/calculate_ratio_output.csv
   :header-rows: 1
   :widths: auto
