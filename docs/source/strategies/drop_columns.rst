Drop Columns Strategy
=====================

Drops one or more columns from the DataFrame.

Metadata:
    - name: "dropcolumns"
    - version: "1.0.0"
    - author: "pandaflow team"

Transformation Format:
    - columns: List[str] — List of column names to drop
    - errors: Optional[str] — "raise" or "ignore" if column is missing (default: "raise")

Example Transformation
------------

.. literalinclude:: ../data/drop_columns/pandaflow-config.json
   :language: json
   :linenos:
   :caption: Example Transformation Definition

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/drop_columns/input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/drop_columns/output.csv
   :header-rows: 1
   :widths: auto
