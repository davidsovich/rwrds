library(tidyverse)
library(devtools)
library(DBI)
library(RPostgres)
library(dbplyr)

# Connect to WRDS
wrds = wrds_connect(username = Sys.getenv("WRDS_NAME"), password = Sys.getenv("WRDS_PASS"))

# Test functions in general_wrds.R
schema_list = wrds_schema_list(wrds)
table_list = wrds_table_list(wrds = wrds, schema = "compa")
variable_list = wrds_variable_list(wrds = wrds, schema = "compa", table = "funda")
