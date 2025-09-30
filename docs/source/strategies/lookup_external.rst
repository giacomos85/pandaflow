Lookup External Strategy
========================

Looks up values from an external CSV file based on a key column.

Metadata:
    - name: "lookup_external"
    - version: "1.0.0"
    - author: "pandaflow team"
    - description: "Looks up values from an external CSV file based on a key column"

Transformation Format:
    - field: str — Name of the output column to store mapped values
    - source: str — Column in the input DataFrame to match (defaults to `field`)
    - file: str — Path to external CSV file (supports `${output}` and `${year}` placeholders)
    - key: str — Column name in the CSV to match against `source`
    - value: str — Column name in the CSV to use as the mapped value
    - not_found: Optional[str] — Fallback value if no match is found

Example Transformation
------------

.. code-block:: json

    {
        "strategy": "lookup_external",
        "field": "country_name",
        "source": "country_code",
        "file": "data/${year}/${output}_lookup.csv",
        "key": "code",
        "value": "name",
        "not_found": "Unknown"
    }

Input DataFrame
---------------

.. csv-table:: Input DataFrame
   :header-rows: 1

   country_code
   IT
   FR
   DE
   ES

External CSV (`data/2025/countries_lookup.csv`)
-----------------------------------------------

.. csv-table:: Lookup Table
   :header-rows: 1

   code,name
   IT,Italy
   FR,France
   DE,Germany

Expected Output
---------------

.. csv-table:: Output DataFrame
   :header-rows: 1

   country_code,country_name
   IT,Italy
   FR,France
   DE,Germany
   ES,Unknown
