% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/crsp.R
\name{crsp_annual}
\alias{crsp_annual}
\title{Download CRSP annual returns and end-of-year prices}
\usage{
crsp_annual(wrds, begin_year, end_year, dl = TRUE)
}
\arguments{
\item{wrds}{WRDS connection object from \code{wrds_connect} function.}

\item{begin_year}{Numeric.}

\item{end_year}{Numeric.}

\item{dl}{Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
lazy \code{dplyr} table reference.}
}
\description{
\code{crsp_annual} downloads CRSP annual returns and end-of-year prices.
}
\details{
Constructs annual returns from the monthly stock file, located at crspa.msf. Only keeps permnos
with a full year of data with non-missing values for returns and strictly positive values
for shares outstanding and price. The function merges on identifying information from the
CRSP header table.
}
\examples{
wrds = wrds_connect(username = "testing", password = "123456")
crspa_df = crsp_annual(wrds = wrds, begin_year = 2010, end_year = 2012)
}
