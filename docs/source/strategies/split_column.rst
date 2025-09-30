Split Column
============

Splits a string column into multiple columns using a delimiter.

Metadata:
    - name: "split_column"
    - version: "1.0.0"
    - author: "pandaflow team"

Rule Format:
    - column: str — Column to split
    - delimiter: str — Delimiter to use
    - maxsplit: Optional[int] — Max number of splits (-1 = no limit)
    - prefix: Optional[str] — Prefix for new columns (default: "split")
    - drop_original: Optional[bool] — Whether to drop the original column

Example Rule
------------

.. literalinclude:: ../data/split_column/pandaflow-config.json
   :language: json
   :linenos:
   :caption: Split_column Rule Example

Input DataFrame
---------------

.. csv-table:: Input DataFrame
   :file: ../data/split_column/input.csv
   :header-rows: 1
   :widths: auto

Expected Output
---------------

.. csv-table:: Input DataFrame
   :file: ../data/split_column/output.csv
   :header-rows: 1
   :widths: auto
