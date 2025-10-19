uuid
----

The **uuid** strategy generates a unique UUIDv7 value for each row in a DataFrame.  
This is useful for assigning identifiers, anonymizing records, or tracking row-level lineage.

Metadata:
    - **Name**: `uuid`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


uuid schema
~~~~~~~~~~~

.. list-table:: uuid Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'uuid'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will store the generated UUID value.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/uuid/input.csv
   :header-rows: 1
   :widths: auto

uuid example
~~~~~~~~~~~~
.. literalinclude:: ../data/uuid/pandaflow-config.json
   :language: json
   :linenos:
   :caption: uuid Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/uuid/output.csv
   :header-rows: 1
   :widths: auto