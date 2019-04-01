
<!-- README.md is generated from README.Rmd. Please edit that file -->
rwrds
=====

An R package for remotely obtaining and cleaning data from WRDS. Requires a valid WRDS account.

Highlights
----------

-   Simple-to-use functions for downloading data from the most commonly used WRDS databases.

-   Wrappers for submitting custom queries to the WRDS cloud.

-   Helper functions for data cleaning and linking to Compustat and CRSP.

Supported WRDS databases
------------------------

In general, this package supports all WRDS databases.

However, cleaning and linking functions are only available for a subset of the databases.

Cleaning functions are available for the following databases:

-   Compustat

-   CRSP

-   Mergent

-   TRACE

Linking functions are available for the following databases:

-   Compustat and CRSP

-   TRACE, Compustat, and CRSP

-   Mergent, Compustat, and CRSP

Usage
-----

A detailed usage description can be found in the [vignette](https://github.com/davidsovich/rwrds/blob/master/vignettes/rwrds.pdf).

Examples:

``` r

# Connect to WRDS
wrds = wrds_connect(username = Sys.getenv("WRDS_NAME"), password = Sys.getenv("WRDS_PASS"))

#Download data from Compustat

   # Annual Compustat data
   comp_df = compustat_annual(wrds = wrds, 
                              begin_year = 2010, 
                              end_year = 2011)
   
   # Annual Compustat data with CRSP returns
   comp_crsp_df = compustat_crsp_annual(wrds = wrds, 
                                        begin_year = 2010, 
                                        end_year = 2012)

#Download data from CRSP

   # Annual CRSP returns
   crspa_df = crsp_annual(wrds = wrds, begin_year = 2010, end_year = 2012)
   
   # Monthly CRSP returns
   crspm_df = crsp_monthly(wrds = wrds, begin_year = 2010, end_year = 2012, dl = TRUE)
   
   # Daily CRSP returns
   crspd_df = crsp_daily(wrds = wrds, begin_year = 2010, end_year = 2010, dl = FALSE)
   
# Download Fama-French factors
   
# Download Mergent data
   
# Custom WRDS query
   
# Submission of a text file to WRDS cloud
```

Installation
------------

The rwrds package is not available on CRAN. You can install the development version from Github:

``` r
library("devtools")
devtools::install_github("davidsovich/rwrds")
```

Contact
-------

dsovich `AT` wustl.edu

History
-------

-   April 5, 2019: Developmental release
