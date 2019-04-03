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


#' Download Compustat
#'
#' \code{compustat} downloads the annual or quarterly Compustat table.
#'
#' Downloads the annual or quarterly Compustat table. The table address is either compa.funda or
#' compa.fundq. By default, the function downloads a subset of the variables. You can specify your
#' own variable list by providing a string vector to the \code{vars} function. You can download all
#' the variables by setting \code{vars} equal to 'all'.  By default, mergeos on industry
#' information from the names table and removes duplicate gvkey fiscal quarter or
#' fiscal year observations. The de-duping logic is to keep the observation with the fewest NA
#' values. See function documentation for \code{compustat_annual} and \code{compustat_quarterly}.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric.
#' @param end_year Numeric.
#' @param frequency Character. Input either 'annual' or 'quarterly'.
#' @param vars Optional character. Defaults to a recommended subset of variables. Choose 'all' if
#' you want to download all variables. Otherwise, provide a list of variables.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' compa_df = compustat(wrds = wrds, begin_year = 2010, end_year = 2012, frequency = 'annual', vars = c('gvkey','fyear', 'revt'))
compustat = function(wrds, begin_year, end_year, frequency, vars = "default") {
  if(missing(frequency)) {
    stop("Error! Must choose either 'annual' or 'quarterly' frequency.")
  }
  if(!(frequency %in% c("annual", "quarterly"))) {
    stop("Error! Must choose either 'annual' or 'quarterly' frequency.")
  }
  if(frequency == "annual") {
    compustat_annual(wrds = wrds, begin_year = begin_year, end_year = end_year, vars = vars)
  } else {
    compustat_quarterly(wrds = wrds, begin_year = begin_year, end_year = end_year, vars = vars)
  }
}



