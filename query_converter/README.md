# query_converter

This project delivers a set of useful functions for profiling and transpiling basic SQL queries from a legacy MSSQL environment into Databricks SQL.  
Because not only the environment but also the warehouse architecture changed, a **column mapping file** is required to translate queries correctly.  

The package also contains a simple HTML template that you can render to view results in a more user-friendly way.  
An example of this is available in `example.ipynb` (Databricks notebook is preferred as Jupyter may not render it correctly due some issues with js packages).

## Prerequisites

Download all required packages with:
```bash
poetry install
```
Make sure you have a mapping file (Excel) with the following columns:
- Legacy db	
- Legacy schema	
- Legacy table	
- Legacy column	
- CDL-STC schema	
- CDL-STC table	
- CDL-STC column	
- Comment


### Example usage
```python
from query_converter.config import load_mapping
from query_converter.functions.transpile import replace_legacy_with_cdl
from query_converter.functions.html_parsing import pretty_print_sql


legacy_query = """
SELECT DISTINCT
PRODUCT_DETAILS.[NAME],
PRODUCT_DETAILS.[DESCRIPTION],
PRODUCT_DETAILS.[ITEM_GTIN],
SUM(SHIPMENT_DATA.[VOL_STAT_UNIT])/1000 AS TOTAL_DELIVERED_MSU1,
SUM(SHIPMENT_DATA.[VOL_BASE_UNIT])/1000 AS TOTAL_DELIVERED_MSU2,
SUM(SHIPMENT_DATA.[VOL_PLAN_UNIT])/1000 AS TOTAL_DELIVERED_MSU3,
SUM(SHIPMENT_DATA.[VOL_TRNXL_UNIT])/1000 AS TOTAL_DELIVERED_MSU4,
    GEO_LIST.COUNTRY
FROM
WMW.DBO.PRODUCT_D AS PRODUCT_DETAILS
INNER JOIN WMW.DBO.SHIP_PROD_F AS SHIPMENT_DATA ON PRODUCT_DETAILS.PRODUCT_SID = SHIPMENT_DATA.PRODUCT_SID
INNER JOIN WMW.DBO.GEO_D AS GEO_LIST ON GEO_LIST.GEO_SID = SHIPMENT_DATA.GEO_SID
WHERE
PRODUCT_DETAILS.[NAME] IN ('82281104','82297604','82247351','82247494','82275081','82283786','82242541','82294101','82303374','81690203','81690205','81682913','81682906','81682909','81665060','81729363','81729364','81729365','81689869','81682832','81671867','81677600','81692129','81668240','81538908','81538823','81681079','81681064','81692333','81685653','81687068','80320865','80322948','82284642','82284643','82239084','82243927','82245581','82247350','82283832','82285456','82285457') AND
GEO_LIST.COUNTRY IN ('ISRAEL','THAILAND','INDONESIA','MALAYSIA','Philippines','Singapore','POLAND','ROMANIA','FRANCE','UNITED KINGDOM','TURKEY','GERMANY','CYPRUS','EGYPT','MEXICO','BRAZIL','CHINA','INDIA')  AND
SHIPMENT_DATA.DATE_ID BETWEEN 20190101 AND  20191031  
GROUP BY PRODUCT_DETAILS.[NAME],PRODUCT_DETAILS.[DESCRIPTION],PRODUCT_DETAILS.[ITEM_GTIN], GEO_LIST.COUNTRY

"""

mapping_df = load_mapping("mapping_2025_08_20.xlsx")
result = replace_legacy_with_cdl(legacy_query, mapping_df)

print(pretty_print_sql(result.query))

```



## Code Tree

```bash

│   example.ipynb
│   poetry.lock
│   pyproject.toml
│   README.md
├───query_converter
│   │   config.py
│   ├───functions
│   │   │   data_profiling.py
│   │   │   helpers.py
│   │   │   html_parsing.py
│   │   │   transpile.py
│   ├───models
│   │   │   query_response.py
│   ├───static
│   │   │   converter_template.tmpl
│   │   │   style.css
│
├───tests

```
- /static - static files (currently only HTML/CSS for the UI)

- /models - dataclasses used in code

- /functions - main logic for the converter

    - data_profiling.py - functions for extract table and column names from input T-SQL query and map them

    - transpile.py - functions main transpile sql(convert_from_tsql_to_databricks, replace_legacy_with_cdl)

    - helpers.py - helper functions for convert_from_tsql_to_databricks and replace_legacy_with_cdl functions

    - html_parsing.py - functions for render results as HTML

- config.py – contains basic functions to load static files such as the mapping file or HTML templates.

## Limitations

- The HTML frontend is built in a Databricks-specific way. If you change it, verify that it still renders correctly.

- Searching through the mapping file UI works only in Databricks, not in Jupyter.

- Transpilation quality depends heavily on the mapping file and the query itself. Very complex T-SQL constructs or poor mappings may fail.

- Currently, column names in the mapping file must be exactly the same as described above.

## Use case - Databricks

This library was primarily developed to handle query conversion in Databricks notebooks with nice UI.

To reduce the amount of code shared with end users, we split the logic across a couple of notebooks, which are later executed by the main one.
You can check the code for these here:

https://adb-2859171346797906.6.azuredatabricks.net/browse/folders/667069447676384?o=2859171346797906

As you can see, we have two notebooks:

load_static – installs the package from the wheel, loads a mapping file, and sets up some static configuration.

init_ui – displays the result of the query conversion provided above.

This folder is also the actual place where we should replace the wheel if we make any changes to the code or the mapping file.

The actual usage of these notebooks is here:
https://adb-2859171346797906.6.azuredatabricks.net/editor/notebooks/667069447676385?o=2859171346797906
