replace
-------

The **replace** strategy searches for a substring or value in a column and replaces it with another string.  
This is useful for cleaning up labels, standardizing values, or removing unwanted characters.

Metadata:
    - **Name**: `replace`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


replace schema
~~~~~~~~~~~~~~

.. list-table:: replace Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'replace'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the column in which the replacement will be performed.

   * - ``find``
     - Union
     - True
     - Value to search for in the specified column. Can be a string or numeric value.

   * - ``replace``
     - str
     - True
     - Replacement value to assign when a match with 'find' is found.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/replace/input.csv
   :header-rows: 1
   :widths: auto

replace example
~~~~~~~~~~~~~~~
.. literalinclude:: ../data/replace/pandaflow-config.json
   :language: json
   :linenos:
   :caption: replace Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/replace/output.csv
   :header-rows: 1
   :widths: auto