library(tidyverse)
library(devtools)
library(DBI)
library(RPostgres)
library(dbplyr)
library(sqldf)
devtools::load_all()

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
crsp_names = crsp_header(wrds = wrds, subset = TRUE, dl = FALSE)
crspa_df = crsp_annual(wrds = wrds, begin_year = 2010, end_year = 2012)
crspm_df = crsp_monthly(wrds = wrds, begin_year = 2010, end_year = 2012, dl = TRUE)
crspd_df = crsp_daily(wrds = wrds, begin_year = 2010, end_year = 2010, dl = TRUE)

# Test Compustat-CRSP functions
linking_df = compustat_crsp_linking_table(wrds = wrds)
comp_linked = compustat_append_crsp_links(wrds = wrds, comp_df = compa_annual)
comp_crsp_df = compustat_crsp_annual(wrds = wrds, begin_year = 2010, end_year = 2012)

# Test Mergent functions
mergent_issues_df = mergent_issues(wrds, clean = TRUE, vanilla = TRUE, subset = TRUE, dl = TRUE)
mergent_corps = mergent_corporates(wrds, clean = TRUE, vanilla = TRUE, subset = TRUE, dl = TRUE)
mergent_ao_panel = deprecated_mergent_yearly_ao(wrds, begin_year = 1989, end_year = 1995, dl = TRUE)
mergent_ao_panel = mergent_yearly_ao(wrds, begin_year = 1989, end_year = 1995, dl = TRUE)
mergent_ratings_panel = mergent_yearly_ratings(wrds, begin_year = 1995, end_year = 2000, dl = TRUE)
mergent_df = mergent_issuer_panel(wrds = wrds, begin_year = 2000, end_year = 2002,
                                  corps_only = TRUE, clean = TRUE, vanilla = TRUE, min_offering_amt = 1000)


# Mergent amount outstanding time tests
system.time(mergent_yearly_ao(wrds, begin_year = 2000, end_year = 2008) %>%
              summarise(n=n()) %>%
              collect())
system.time(deprecated_mergent_yearly_ao(wrds, begin_year = 2000, end_year = 2008) %>%
              summarise(n=n()) %>%
              collect())
