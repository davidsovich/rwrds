#' Download Compustat names table
#'
#' \code{compustat_names} downloads the Compustat names table.
#'
#' The names table contains general gvkey-level information, such as NAICS industry. The table
#' address is compa.names. By default, the function only downloads a subset of the variables.
#' You can download all the variables by setting the \code{subset} argument to \code{FALSE}.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param subset Optional Boolean. Download recommended subset of variables? Defaults to \code{TRUE}.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' names_df = compustat_names(wrds = wrds, subset = TRUE, dl = TRUE)
compustat_names = function(wrds, subset = TRUE, dl = TRUE) {
  if(subset == TRUE) {
    variables = compustat_names_variables()
  } else {
    variables = wrds_variable_list(wrds = wrds, schema = "compa", table = "names")$column_name
  }
  names_df = dplyr::tbl(wrds, dbplyr::in_schema("compa", "names")) %>%
    dplyr::select(dplyr::one_of(variables))
  if(dl == TRUE) {
    names_df %>% dplyr::collect()
  } else {
    names_df
  }
}

#' Download annual Compustat
#'
#' \code{compustat_annual} downloads the Compustat table with annual observations.
#'
#' Downloads the annual observation Compustat table that is updated at an annual frequency. The
#' table address is compa.funda. By default, the function only downloads a subset of the variables.
#' You can download all the variables by setting the \code{subset} argument to \code{FALSE}.
#' By default, merges on industry information from the names table and removes duplicate gvkey
#' fiscal year observations. The de-duping logic is to keep the observation with the fewest NA
#' values.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric.
#' @param end_year Numeric.
#' @param subset Optional Boolean. Download recommended subset of variables? Defaults to \code{TRUE}.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' compa_df = compustat_annual(wrds = wrds, begin_year = 2010, end_year = 2012)
compustat_annual = function(wrds, begin_year, end_year, subset = TRUE) {
  if(subset == TRUE) {
    variables = compustat_annual_standard_variables()
  } else {
    variables = wrds_variable_list(wrds = wrds, schema = "compa", table = "funda")$column_name
  }
  comp_df = dplyr::tbl(wrds, dbplyr::in_schema("compa", "funda")) %>%
    dplyr::filter(consol == 'C',
                  datafmt == 'STD',
                  indfmt %in% c('INDL','FS'),
                  popsrc == 'D',
                  curncd %in% c("USD", "CAD"),
                  fyear >= begin_year,
                  fyear <= end_year ) %>%
    dplyr::select(dplyr::one_of(variables)) %>%
    dplyr::inner_join(y = compustat_names(wrds = wrds, subset = TRUE, dl = FALSE),
                     by = c("gvkey" = "gvkey")) %>%
    dplyr::collect() %>%
    compustat_annual_remove_dups()
  return(comp_df)
}


