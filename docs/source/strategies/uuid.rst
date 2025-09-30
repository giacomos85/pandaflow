UUID
----

The **uuid** strategy generates a unique UUIDv7 value for each row in a DataFrame.  
This is useful for assigning identifiers, anonymizing records, or tracking row-level lineage.

Metadata
~~~~~~~~

- **Name**: `uuid`
- **Version**: `1.0.0`
- **Author**: Pandaflow Team
- **Description**: Generates UUIDv7 values for a specified column.

Rule Format
~~~~~~~~~~~

The rule must specify:

- `field`: The column name where UUIDs will be stored

.. literalinclude:: ../data/uuid/pandaflow-config.json
   :language: json
   :linenos:
   :caption: UUID Rule Example

Input Example
~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/uuid/input.csv
   :header-rows: 1
   :widths: auto

Result
~~~~~~

.. csv-table:: Output with UUIDs
   :file: ../data/uuid/output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
~~~~~~~~~~~~~~

- Each row receives a unique UUIDv7 value.
- The strategy does not depend on existing column values.
- If the `field` already exists, it will be overwritten.
- Requires the `uuid_extension` package for UUIDv7 generation.

