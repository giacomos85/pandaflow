Drop Duplicates Strategy
========================

Drops duplicate rows from a DataFrame, optionally based on a subset of columns.

Metadata:
    - name: "drop_duplicates"
    - version: "1.0.0"
    - author: "pandaflow team"

Transformation Format:
    - subset: Optional[List[str]] — Columns to consider for identifying duplicates
    - keep: Optional[str] — "first", "last", or False (default: "first")
    - reset_index: Optional[bool] — Whether to reset the index after dropping

Example Transformation
------------

.. code-block:: json

    {
        "strategy": "drop_duplicates",
        "subset": ["name", "age"],
        "keep": "first",
        "reset_index": true
    }

Input DataFrame
---------------

.. csv-table:: Input DataFrame
   :header-rows: 1

   name,age
   Alice,30
   Bob,25
   Alice,30
   Charlie,40
   Bob,25

Expected Output
---------------

.. csv-table:: Output DataFrame
   :header-rows: 1

   name,age
   Alice,30
   Bob,25
   Charlie,40
