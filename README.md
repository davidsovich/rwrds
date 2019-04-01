
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

   # Custom data series for CES state data
   ces_df = ces_download(bls_key = Sys.getenv("BLS_KEY"),
                      start_year = 2010,
                      end_year = 2015,
                      adjustment = "U",
                      industries = "05000000",
                      data_types = c("01", "03", "11"),
                      states = "1900000")
   
   # Pre-build CES data series for national employment
   ces_df = ces_employment(bls_key = Sys.getenv("BLS_KEY"),
                           start_year = 2010,
                           end_year = 2015)

#Download data from CRSP

   # Custom data series for JOLTS regional data
   jolts_df = jolts_download(bls_key = Sys.getenv("BLS_KEY"),
                             start_year = 2010,
                             end_year = 2015,
                             adjustment = "S",
                             industries = "000000",
                             data_types = "HI",
                             data_levels = "L",
                             regions = c("MW", "NE", "SO", "WE"))
   
   # Pre-built JOLTS data series
   jolts_df = jolts_hiring(bls_key = Sys.getenv("BLS_KEY"),
                           start_year = 2010,
                           end_year = 2015)
   
# Custom WRDS query
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
