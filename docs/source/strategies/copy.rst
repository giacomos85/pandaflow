Copy
----

The **copy** strategy transfers values from one column to another, optionally applying input parsing, output formatting, and fill-in logic.  
It is useful for duplicating fields, normalizing formats, or creating derived columns with fallback behavior.

Metadata
~~~~~~~~

- **Name**: `copy`
- **Version**: `1.0.0`
- **Author**: Pandaflow Team
- **Description**: Copies values from one column to another.

Rule Format
~~~~~~~~~~~

The rule must specify the target field and source column. Optional keys allow for parsing, formatting, and fallback values.

.. literalinclude:: ../data/copy/pandaflow-config.json
   :language: json
   :linenos:
   :caption: Example Rule Definition

- `field`: Target column to populate.
- `source`: Source column to copy from. Defaults to `field` if omitted.
- `parser`: Optional parser to apply to source values.
- `formatter`: Optional formatter to apply to parsed values.
- `fillna`: Optional fallback value for empty or missing entries.

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/copy/input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/copy/output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
~~~~~~~~~~~~~~

- If `source` is missing, the strategy assumes `source == field`.
- If `parser` or `formatter` are provided, they are applied in sequence.
- If `fillna` is defined, empty strings and `NaN` values are replaced accordingly.
- Raises `ValueError` if the source column is not found in the input DataFrame.