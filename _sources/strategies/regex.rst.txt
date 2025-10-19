regex
-----

The **regex** strategy extracts data from a column using a regular expression.  
It’s ideal for parsing structured strings, extracting identifiers, or cleaning up noisy fields.

Metadata:
    - **Name**: `regex`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


regex schema
~~~~~~~~~~~~

.. list-table:: regex Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'regex'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will store the extracted or transformed value.

   * - ``source``
     - str
     - True
     - Name of the source column whose values will be processed using the regular expression.

   * - ``regex``
     - str
     - True
     - Regular expression pattern to apply to the source column's values.

   * - ``group_id``
     - int
     - True
     - Index of the capturing group to extract from the regex match.

   * - ``replace``
     - Optional
     - False
     - Optional dictionary of string replacements to apply after regex extraction. Keys are substrings to replace; values are their replacements.

   * - ``formatter``
     - Optional
     - False
     - Optional formatter function name to apply after regex and replacement steps.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/regex/input.csv
   :header-rows: 1
   :widths: auto

regex example
~~~~~~~~~~~~~
.. literalinclude:: ../data/regex/pandaflow-config.json
   :language: json
   :linenos:
   :caption: regex Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/regex/output.csv
   :header-rows: 1
   :widths: auto