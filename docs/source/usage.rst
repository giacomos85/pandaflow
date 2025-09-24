Using Pandaflow
===============

Pandaflow helps you transform pandas DataFrames using simple, rule-based instructions.  
No need to write custom functions — just define what you want in a JSON rule, and Pandaflow does the rest.

Installation
------------

Install Pandaflow using pip:

.. code-block:: bash

   pip install pandaflow

Or with Poetry if you're using it:

.. code-block:: bash

   poetry add pandaflow

Basic Example
-------------

Let’s say you want to copy values from one column to another and format them to two decimal places.

1. **Create a rules files** (save as `my_rules.json`):

   .. code-block:: json

    {
        "rules": [
            {
                "strategy": "copy",
                "field": "__amount__",
                "source": "__total__",
                "output_rule": "float_2dec"
            }
        ]
    }
      

2. **Prepare your data**:

   .. code-block:: python

      import pandas as pd

      df = pd.DataFrame({
          "__total__": ["100.123", "200.456", ""]
      })

3. **Run PandaFlow**:

   .. code-block:: bash

      pandaflow run --input mydata.csv --output results.csv --config my_rules.json

What You Can Do
---------------

Pandaflow supports many built-in strategies:

- **constant**: Set a column to a fixed value
- **copy**: Copy and format values from another column
- **drop**: Remove unwanted columns
- **filter**: Keep only rows that match a condition

Each strategy has its own rule format. You can find examples in the [Strategy Reference](strategies/index).

Using Rules from Files
----------------------

You can store your rules in `.json` files and reuse them across projects:

.. code-block:: python

   import json
   from pandaflow.engine import apply_rule

   with open("rules/drop.json") as f:
       rule = json.load(f)

   df = apply_rule(df, rule)

Tips for End Users
------------------

- ✅ You don’t need to write custom code — just edit the rule files
- ✅ You can preview transformations before saving results
- ✅ You can combine multiple rules in a pipeline (coming soon!)
- ✅ You can validate rules using Pandaflow’s built-in checks

Need Help?
----------

- Check the [overview](overview) for how Pandaflow works
- Browse the [strategy docs](strategies/index) for examples
- Reach out via GitHub if you run into issues

