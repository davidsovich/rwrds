library(tidyverse)
library(devtools)
library(DBI)
library(RPostgres)
library(dbplyr)
library(sqldf)

# Connect to WRDS
wrds = wrds_connect(username = Sys.getenv("WRDS_NAME"), password = Sys.getenv("WRDS_PASS"))

# Test functions in general_wrds.R
schema_list = wrds_schema_list(wrds)
table_list = wrds_table_list(wrds = wrds, schema = "compa")
variable_list = wrds_variable_list(wrds = wrds, schema = "compa", table = "funda")

# Test Compustat functions
comp_names = compustat_names(wrds = wrds, subset = FALSE, dl = FALSE)
compa_annual = compustat_annual(wrds = wrds, begin_year = 2010, end_year = 2011) #removes dups

# Test CRSP functions

# Test Compustat-CRSP functions
linking_df = compustat_crsp_linking_table(wrds = wrds)
comp_linked = compustat_append_crsp_links(wrds = wrds, comp_df = compa_annual)

# Test Mergent functions
