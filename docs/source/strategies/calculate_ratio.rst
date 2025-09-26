Calculate Ratio
===============

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

.. code-block:: json

    {
        "strategy": "calculate_ratio",
        "numerator": "sales",
        "denominator": "cost",
        "field": "margin",
        "round_digits": 2
    }

Input DataFrame
---------------

.. csv-table:: Input DataFrame
   :header-rows: 1

   sales,cost
   100,50
   200,80
   300,120
   ,0

Expected Output
---------------

.. csv-table:: Output DataFrame
   :header-rows: 1

   sales,cost,margin
   100,50,2.00
   200,80,2.50
   300,120,2.50
   ,0,
