# Data_Pipelines

## License
As of March 11, 2025, Data_Pipelines is governed by a proprietary license (see LICENSE). This applies to all versions and uses of the software on or after this date. Versions released before this date remain under their original license terms.

Used to create ETL pipelines into local SQL Server.

Tables and Callouts:
1. ZW_Rent_Values
   2. This data starts in 2015 and has lots of holes or null values for majority of zipcodes.  This script replaces nulls with 0 so be cautious when using this data in aggregations.
   3. 