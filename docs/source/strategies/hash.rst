Hash
----

The **hash** strategy generates a hash value from one or more columns in a DataFrame.  
This is useful for creating unique identifiers, anonymizing sensitive fields, or tracking row-level changes.

Metadata
~~~~~~~~

- **Name**: `hash`
- **Version**: `1.0.0`
- **Author**: Pandaflow Team
- **Description**: Generates a hash from specified columns.

Rule Format
~~~~~~~~~~~

The rule must specify:

- `field`: The target column to store the hash
- `source`: A list of columns to combine and hash
- `function`: The hash function to use (currently only `"md5"` is supported)

.. literalinclude:: ../data/hash/pandaflow-config.json
   :language: json
   :linenos:
   :caption: Hash Rule Example

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/hash/input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Output with Hashes
   :file: ../data/hash/output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
~~~~~~~~~~~~~~

- Columns listed in `source` are concatenated with `;` and hashed using the specified function.
- Missing values are treated as empty strings.
- If any `source` column is missing from the input, a `ValueError` is raised.
- The strategy currently supports only `md5` hashing.