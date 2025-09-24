RegEx
-----

The **regex** strategy extracts data from a column using a regular expression.  
It’s ideal for parsing structured strings, extracting identifiers, or cleaning up noisy fields.

Metadata
~~~~~~~~

- **Name**: `regex`
- **Version**: `1.0.0`
- **Author**: Pandaflow Team
- **Description**: Extracts data from a column using a regular expression.

Rule Format
~~~~~~~~~~~

The rule must specify:

- `field`: Target column to store the extracted value
- `source`: Column to apply the regex to
- `regex`: Regular expression pattern
- `group_id`: Group index to extract from the match
- `replace`: Optional dictionary to map extracted values
- `output_rule`: Optional formatter to apply to the result

.. literalinclude:: ../rules/regex.json
   :language: json
   :linenos:
   :caption: Regex Rule Example

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/regex_input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Extracted Output
   :file: ../data/regex_output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
~~~~~~~~~~~~~~

- If the regex does not match, the result is an empty string.
- If `group_id` is out of bounds, the result is empty.
- If `replace` is provided, extracted values are mapped accordingly.
- If `output_rule` is defined, it is applied after extraction and replacement.

