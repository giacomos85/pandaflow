Drop Columns Strategy
=====================

Drops one or more columns from the DataFrame.

Metadata:
    - name: "dropcolumns"
    - version: "1.0.0"
    - author: "pandaflow team"

Rule Format:
    - columns: List[str] — List of column names to drop
    - errors: Optional[str] — "raise" or "ignore" if column is missing (default: "raise")

Example Rule
------------

.. code-block:: json

    {
        "strategy": "dropcolumns",
        "columns": ["gender", "country"],
        "errors": "ignore"
    }

Input DataFrame
---------------

.. csv-table:: Input DataFrame
   :header-rows: 1

   name,age,gender,country
   Alice,30,F,IT
   Bob,25,M,FR

Expected Output
---------------

.. csv-table:: Output DataFrame
   :header-rows: 1

   name,age
   Alice,30
   Bob,25
