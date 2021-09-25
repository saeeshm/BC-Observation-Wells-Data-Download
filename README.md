# BC Observation Wells Data Download
A collection of scripts that download all well data from the [BC Provincial Observation Well Network](https://catalogue.data.gov.bc.ca/dataset/provincial-groundwater-observation-well-network-groundwater-levels-data/resource/43398efb-5646-4dcc-bf61-1f6def0a7c63), collecting them into a PostgresSQL database. The data are stored at [this location](https://www.env.gov.bc.ca/wsd/data_searches/obswell/map/data/). The program is designed to be running periodically and regularly update the database as new data becomes available.

## Structure
The program contains 3 primary files, for the following uses

### obswell_scraping_reset.py
This script downloads the entire record of available data for all wells in the observation network. These data are stored on the server as individual csv files for each station, labelled as ***-data.csv. It overwrites any existing database with the new download of well data. This script is useful for creating a database from scratch if not already present. For updating an existing database it is recommended to use the `pacfish_scraping_update.py` script

### obswell_scraping_update.py
This script downloads recent (past 1-year) data for all wells in the observation network. These data are stored on the server as individual csv files for each station, labelled as ***-recent.csv. It checks whether any downloaded data are already present in the existing database, filtering these out to ensure that only new data are appended to the database. It requires an existing PostgreSQL database to update.

### create_ancil_data.R
The preceding two scripts create a data-table named `hourly` within the specified schema to contain all downloaded hourly data. This script generates two additional tables: `daily_mean` contains the average records by day for each station. `hourly_recent` contains only the hourly data for the preceding 1 year. Both tables are generated within the same schema.

## Usage notes
A working installation of PostgreSQL is required for using this script. The database should contain a schema titled `obswell` within which data will be added. A file titled `credentials.json` must be placed in the home directory, which contains the parameters for connecting to the Postgres database. This script should be structured as follows:
```
{
  "user": "<USERNAME>",
  "host": "<HOST IP: can be 'localhost'>",
  "port": "<PORT: default is usually 5432>",
  "dbname": "<DATABASE NAME>",
  "password": "<USER PASSWORD>"
}
```
The `credentials.json` file may optionally contain a parameter `"schema": "<SCHEMA NAME>"` which specifies the database schema. This parameter is required in case the preferred schema is named something other than `obswell`.

The scripts can simply be called from the command prompt/terminal to run in the background. Usually, either of the python scripts are run first, followed by the R script, which creates the secondary data tables from the downloaded primary data. The easiest method is to put calls to both scripts in a single `.bat` or `.sh` file:
```
cd /path/to/workingDir

python obswell_scraping_reset.py

Rscript create_ancil_data.R --vanilla
```
