split_column
------------

Splits a string column into multiple columns using a delimiter.

Metadata:
    - **Name**: `split_column`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


split_column schema
~~~~~~~~~~~~~~~~~~~

.. list-table:: split_column Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'split_column'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``column``
     - str
     - True
     - Name of the column whose string values will be split using the specified delimiter.

   * - ``delimiter``
     - str
     - True
     - Delimiter string used to split the column values (e.g., ',' or '|').

   * - ``maxsplit``
     - int
     - False
     - Maximum number of splits to perform. Use -1 for no limit.

   * - ``prefix``
     - str
     - False
     - Prefix to use when naming the new columns created from the split operation.

   * - ``drop_original``
     - bool
     - False
     - Whether to drop the original column after splitting. Defaults to False.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/split_column/input.csv
   :header-rows: 1
   :widths: auto

split_column example
~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../data/split_column/pandaflow-config.json
   :language: json
   :linenos:
   :caption: split_column Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/split_column/output.csv
   :header-rows: 1
   :widths: auto