

#' Download Mergent-CRSP-Compustat linking table
#'
#' \code{mergent_linking_table} downloads the cleaned Mergent to CRSP-Compustat linking table.
#'
#' Downloads the cleaned Mergent to CRSP-Compustat linking table from remote repository. Linking
#' table is created by cleaning the TRACE-CRSP linking table from WRDS and converting cusip to
#' permno links into Mergent issuer_id to permno and gvkey links. Performance of linking
#' table is suspect for issuers who have not issued a bond during existence of TRACE (2002+).
#' Linking table is constructed in create_dropboxdata.R.
#'
#' @export
#'
#' @examples
#' link_df = trace_crsp_linking_table()
mergent_linking_table = function() {
  get(load(url("https://www.dropbox.com/s/2hlye8s5txykjpn/clean_mergent_links.Rda?dl=1")))
}

#' Download TRACE-CRSP linking table
#'
#' \code{trace_crsp_linking_table} downloads the cleaned TRACE cusip to CRSP permno linking table.
#'
#' Downloads the cleaned TRACE-CRSP linking table from remote repository. Linking table from
#' WRDS is cleaned using the procedure outlined in create_dropboxdata.R. Contact author for
#' access to this source code. Cleaning procedure removes erroneous links and extends link
#' start dates to prior to introduction of TRACE in 2002.
#'
#' @export
#'
#' @examples
#' link_df = trace_crsp_linking_table()
trace_crsp_linking_table = function() {
  get(load(url("https://www.dropbox.com/s/2hlye8s5txykjpn/clean_trace_crsp_links.Rda?dl=1")))
}

compustat_append_bond_links = function(wrds) {



}


# This one here does annual summaries at issuer id level
compustat_crsp_bonds_annual = function( ) {



}
