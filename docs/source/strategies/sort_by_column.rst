Sort By Column Strategy
=======================

Sorts a DataFrame by one or more columns, with optional direction and NaN positioning.

Metadata:
    - name: "sort_by_column"
    - version: "1.0.0"
    - author: "pandaflow team"

Rule Format:
    - columns: List[str] — Columns to sort by (in priority order)
    - ascending: Optional[List[bool]] — Sort direction per column (default: True for all)
    - na_position: Optional[str] — "first" or "last" (default: "last")

Example Rule
------------

.. literalinclude:: ../rules/sort_by_column.json
   :language: json
   :linenos:
   :caption: Sort_by_column Rule Example

Input DataFrame
---------------

.. csv-table:: Input DataFrame
   :file: ../data/sort_by_column_input.csv
   :header-rows: 1
   :widths: auto

Expected Output
---------------

.. csv-table:: Output DataFrame
   :header-rows: 1

   name,score,age
   Bob,92,25
   Alice,85,30
   Charlie,85,40
   Dana,,22
