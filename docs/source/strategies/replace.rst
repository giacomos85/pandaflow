Replace Strategy
================

The **replace** strategy searches for a substring or value in a column and replaces it with another string.  
This is useful for cleaning up labels, standardizing values, or removing unwanted characters.

Metadata:
   - **Name**: `replace`
   - **Version**: `1.0.0`
   - **Author**: Pandaflow Team
   - **Description**: Replaces occurrences of a substring in a specified column with another substring.

Rule Format:
   - `field`: The column to apply the replacement to
   - `find`: The substring or value to search for
   - `replace`: The string to replace it with

.. literalinclude:: ../rules/replace.json
   :language: json
   :linenos:
   :caption: Replace Rule Example

Input Example
-------------

.. csv-table:: Input DataFrame
   :file: ../data/replace_input.csv
   :header-rows: 1
   :widths: auto

Result
------

.. csv-table:: Output with Replacements
   :file: ../data/replace_output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
--------------

- The replacement is applied using pandas `.str.replace()`.
- All values are cast to strings before replacement.
- If the column is missing, a `ValueError` is raised.
- Partial matches are replaced globally within each cell.

