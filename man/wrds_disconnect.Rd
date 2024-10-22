% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/general_wrds.R
\name{wrds_disconnect}
\alias{wrds_disconnect}
\title{Disconnect from WRDS cloud}
\usage{
wrds_disconnect(wrds)
}
\arguments{
\item{wrds}{WRDS connection.}
}
\description{
\code{wrds_disconnect} closes a remote connection to the WRDS cloud.
}
\details{
Note: WRDS cloud runs PostgreSQL.
}
\examples{
wrds = wrds_connect(username = Sys.getenv("WRDS_NAME"), password = Sys.getenv("WRDS_PASS"))
wrds_disconnect(wrds = wrds)
}
