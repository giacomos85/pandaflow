Drop
----

The **drop** strategy removes one or more columns from a DataFrame.  
This is useful for cleaning up unused fields, removing sensitive data, or simplifying downstream processing.

Metadata
~~~~~~~~

- **Name**: `drop`
- **Version**: `1.0.0`
- **Author**: Pandaflow Team
- **Description**: Drops specified columns from the DataFrame.

Rule Format
~~~~~~~~~~~

The rule must specify the column(s) to drop using the `field` key.  
You can provide a single string or a list of column names.

.. code-block:: json
   
   {
      "rules": [
         {
               "field": ["__email__", "__status__"],
               "strategy": "drop"
         }
      ]
   }

Input Example
~~~~~~~~~~~~~

The following table shows the input DataFrame before applying the strategy:

.. csv-table:: Input DataFrame
   :header-rows: 1
   :widths: auto

   __id__,__name__,__email__,__status__
   1,Alice,alice@example.com,active
   2,Bob,bob@example.com,inactive
   3,Charlie,charlie@example.com,active


Result
~~~~~~

After applying the drop strategy, the specified columns are removed:

.. csv-table:: Output DataFrame
   :file: ../data/drop_output.csv
   :header-rows: 1
   :widths: auto

Behavior Notes
~~~~~~~~~~~~~~

- If `field` is a string, it is converted to a list internally.
- If any specified column is missing, `pandas.drop()` will raise a `KeyError`.
- The strategy returns a copy of the DataFrame with the columns removed.

