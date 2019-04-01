
#' Download Compustat-CRSP linking table
#'
#' \code{compustat_crsp_linking_table} downloads the Compustat-CRSP linking table.
#'
#' Downloads the standard Compustat-CRSP linking table. The table address is crspa.ccmxpf_linktable.
#' By default, the function downloads the data. A lazy \code{dplyr} table reference can be returned
#' by setting the \code{dl} argument to \code{FALSE}.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' linking_tbl = compustat_crsp_linking_table(wrds = wrds, dl = FALSE)
compustat_crsp_linking_table = function(wrds, dl = TRUE) {
  link_df = dplyr::tbl(wrds, dbplyr::in_schema("crspa", "ccmxpf_linktable")) %>%
    dplyr::filter(linktype %in% c("LC", "LU"),
                  linkprim %in% c("C", "P") )
  if(dl == TRUE) {
    link_df %>% dplyr::collect()
  } else {
    link_df
  }
}

#' Append CRSP identifiers onto a Compustat data frame
#'
#' \code{compustat_append_crsp_links} appends on CRSP permno and permco identifiers onto Compustat
#' gvkey identifiers.
#'
#' Appends on CRSP permno and permcos based on the standard Compsutat-CRSP linking table. The
#' linking table address is crspa.ccmxpf_linktable. Requires that the Compustat data.frame be
#' physically located in memory (not a lazy \code{dplyr} table reference). Requires that the gvkey
#' and datadate fields are present in the Compustat data.frame.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param comp_df Compustat data.frame. Requires the gkvey and datadate fields to be present.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' compa_annual = compustat_annual(wrds = wrds, begin_year = 2010, end_year = 2011)
#' comp_linked = compustat_append_crsp_links(wrds = wrds, comp_df = compa_annual)
compustat_append_crsp_links = function(wrds, comp_df) {
  if(sum(c("gvkey", "datadate") %in% names(comp_df)) != 2) {
    stop("Error! The following fields must be in comp_df: gvkey and datadate.")
  }
  link_df = compustat_crsp_linking_table(wrds = wrds)
  merge_df = sqldf::sqldf(paste("SELECT",
                                  "a.*, b.lpermno, b.lpermco, b.linkdt, b.linkenddt",
                                "FROM",
                                  "comp_df AS a",
                                "LEFT JOIN",
                                  "link_df AS b",
                                "ON",
                                  "(a.gvkey = b.gvkey) AND (a.datadate >= b.linkdt) AND",
                                  "( (a.datadate <= b.linkenddt) OR (b.linkenddt IS NULL))"))
  merge_df
}


#' Download annual Compustat with CRSP annual returns
#'
#' \code{compustat_annual} downloads the annual Compustat table and merges on annual CRSP returns.
#'
#' Downloads the annual Compustat file using the \code{compustat_annual} function. Appends on
#' CRSP links and returns using the functions \code{compustat_append_crsp_links} and
#' \code{crsp_annual}, respectively. The annual return merge is naive and is based on a fiscal year
#' to calendar year match.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric.
#' @param end_year Numeric.
#' @param subset Optional Boolean. Download recommended subset of variables? Defaults to \code{TRUE}.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' comp_crsp_df = compustat_crsp_annual(wrds = wrds, begin_year = 2010, end_year = 2012)
compustat_crsp_annual = function(wrds, begin_year, end_year, subset = TRUE) {
  comp_df = compustat_annual(wrds = wrds,
                             begin_year = begin_year,
                             end_year = end_year,
                             subset = subset)
  comp_df = compustat_append_crsp_links(wrds = wrds, comp_df = comp_df)
  crsp_df = crsp_annual(wrds = wrds,
                        begin_year = begin_year,
                        end_year = end_year,
                        dl = TRUE) %>%
    dplyr::select(year, permno, permco, annual_ret, prc_eoy, shrout_eoy, total_volume) %>%
    dplyr::rename(lpermno = permno, lpermco = permco)
  comp_df = comp_df %>%
    dplyr::left_join(y = crsp_df,
              by = c("lpermno"="lpermno", "lpermco"="lpermco", "fyear"="year"))
  comp_df
}






