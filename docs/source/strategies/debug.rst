debug
-----

A strategy that prints debug information

Metadata:
    - **Name**: `debug`
    - **Version**: `1.0.0`
    - **Author**: PandaFlow Team


debug schema
~~~~~~~~~~~~

.. list-table:: debug Fields
   :header-rows: 1
   :widths: 20 20 20 60

   * - Field
     - Type
     - Required
     - Description

   * - ``strategy``
     - Literal
     - True
     - Strategy identifier used to select this transformation. Must be 'debug'.

   * - ``version``
     - str | None
     - False
     - Optional version string to track the strategy implementation or schema evolution.



Example input Dataset
~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Input DataFrame
   :file: ../data/debug/input.csv
   :header-rows: 1
   :widths: auto

debug example
~~~~~~~~~~~~~
.. literalinclude:: ../data/debug/pandaflow-config.json
   :language: json
   :linenos:
   :caption: debug Rule Example

Result
~~~~~~

.. csv-table:: Transformed Output
   :file: ../data/debug/output.csv
   :header-rows: 1
   :widths: auto