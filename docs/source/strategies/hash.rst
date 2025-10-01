hash
----

The **hash** strategy generates a hash value from one or more columns in a DataFrame.  
This is useful for creating unique identifiers, anonymizing sensitive fields, or tracking row-level changes.

Metadata:
    - **Name**: `hash`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


hash schema
~~~~~~~~~~~

.. list-table:: hash Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'hash'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.

   * - ``field``
     - str
     - True
     - Name of the output column that will store the computed hash value.

   * - ``source``
     - List
     - True
     - List of column names whose values will be combined and hashed.

   * - ``function``
     - str
     - True
     - Name of the hash function to apply (e.g., 'md5', 'sha256'). Must be supported by the hashing backend.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/hash/input.csv
   :header-rows: 1
   :widths: auto

hash example
~~~~~~~~~~~~
.. literalinclude:: ../data/hash/pandaflow-config.json
   :language: json
   :linenos:
   :caption: hash Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/hash/output.csv
   :header-rows: 1
   :widths: auto