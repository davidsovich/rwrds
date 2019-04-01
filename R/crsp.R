
#' Download CRSP header table
#'
#' \code{crsp_header} downloads the CRSP header table.
#'
#' The header table contains permno-level information, such as SIC industry. The table
#' address is crspa.msfhdr. By default, the function only downloads a subset of the variables.
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
#' crsp_names = crsp_header(wrds = wrds, subset = TRUE, dl = FALSE)
crsp_header = function(wrds, subset = TRUE, dl = TRUE) {
  header_df = dplyr::tbl(wrds, dbplyr::in_schema("crspa", "msfhdr"))
  if(subset == TRUE ) {
    header_df = header_df %>%
      dplyr::select(permno, hshrcd, hnaics, hsiccd, htsymbol, hcomnam, hexcd)
  }
  if(dl == TRUE) {
    header_df %>% dplyr::collect()
  } else {
    header_df
  }
}

#' Download CRSP annual returns and end-of-year prices
#'
#' \code{crsp_annual} downloads CRSP annual returns and end-of-year prices.
#'
#' Constructs annual returns from the monthly stock file, located at crspa.msf. Only keeps permnos
#' with a full year of data with non-missing values for returns and strictly positive values
#' for shares outstanding and price. The function merges on identifying information from the
#' CRSP header table.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric.
#' @param end_year Numeric.
#' @param subset Optional Boolean. Download recommended subset of variables? Defaults to \code{TRUE}.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' crspa_df = crsp_annual(wrds = wrds, begin_year = 2010, end_year = 2012)
crsp_annual = function(wrds, begin_year, end_year, dl = TRUE) {
  crsp_df = dplyr::tbl(wrds, dbplyr::in_schema("crspa", "msf")) %>%
    dplyr::mutate(year = date_part('year', date),
                  log_ret = log(1+ret)) %>%
    dplyr::filter(year >= begin_year,
                  year <= end_year,
                  prc > 0,
                  shrout > 0,
                  !is.na(ret),
                  !is.na(log_ret),
                  ret > -1) %>%
    dplyr::arrange(permno, date) %>%
    dplyr::group_by(permno, year) %>%
    dplyr::mutate(num_months = n(),
                  annual_ret = exp(sum(log_ret, na.rm = TRUE))-1,
                  total_volume = sum(vol, na.rm = TRUE),
                  row_number = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(num_months == 12,
                  row_number == 12) %>%
    dplyr::rename(prc_eoy = prc,
                  shrout_eoy = shrout) %>%
    dplyr::select(year, permno, permco, cusip, annual_ret, prc_eoy, shrout_eoy, total_volume) %>%
    dplyr::left_join(y = crsp_header(wrds = wrds, subset = TRUE, dl = FALSE),
                     by = c("permno"="permno"))
  if(dl == TRUE) {
    crsp_df %>% dplyr::collect()
  } else {
    crsp_df
  }
}

#' Download CRSP monthly return and price data
#'
#' \code{crsp_monthly} downloads CRSP monthly returns and end-of-month prices.
#'
#' Downloads data from table crspa.msf. Removes observations with weakly negative prices and
#' shares outstanding. The function merges on identifying information from the
#' CRSP header table.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric.
#' @param end_year Numeric.
#' @param subset Optional Boolean. Download recommended subset of variables? Defaults to \code{TRUE}.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' crspm_df = crsp_monthly(wrds = wrds, begin_year = 2010, end_year = 2012)
crsp_monthly = function(wrds, begin_year, end_year, dl = TRUE) {
  crsp_df = dplyr::tbl(wrds, dbplyr::in_schema("crspa", "msf")) %>%
    dplyr::mutate(year = date_part('year', date),
                  month = date_part('month', date)) %>%
    dplyr::filter(year >= begin_year,
                  year <= end_year,
                  prc > 0,
                  shrout > 0,
                  ret > -1) %>%
    dplyr::select(permno, permco, cusip, year, month, date,
                  prc, vol, ret, retx, shrout, bid, ask) %>%
    dplyr::left_join(y = crsp_header(wrds = wrds, subset = TRUE, dl = FALSE),
                     by = c("permno"="permno"))
  if(dl == TRUE) {
    crsp_df %>% dplyr::collect()
  } else {
    crsp_df
  }
}


#' Download CRSP daily return and price data
#'
#' \code{crsp_daily} downloads CRSP daily returns and end-of-month prices.
#'
#' Downloads data from table crspa.dsf. Removes observations with weakly negative prices and
#' shares outstanding. The function merges on identifying information from the
#' CRSP header table.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric.
#' @param end_year Numeric.
#' @param subset Optional Boolean. Download recommended subset of variables? Defaults to \code{TRUE}.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' crspd_df = crsp_daily(wrds = wrds, begin_year = 2010, end_year = 2010)
crsp_daily = function(wrds, begin_year, end_year, dl = TRUE) {
  crsp_df = dplyr::tbl(wrds, dbplyr::in_schema("crspa", "dsf")) %>%
    dplyr::mutate(year = date_part('year', date),
                  month = date_part('month', date),
                  day = date_part('day', date)) %>%
    dplyr::filter(year >= begin_year,
                  year <= end_year,
                  prc > 0,
                  shrout > 0,
                  ret > -1) %>%
    dplyr::select(permno, permco, cusip, year, month, day, date,
                  prc, vol, ret, retx, shrout, bid, ask) %>%
    dplyr::left_join(y = crsp_header(wrds = wrds, subset = TRUE, dl = FALSE),
                     by = c("permno"="permno"))
  if(dl == TRUE) {
    crsp_df %>% dplyr::collect()
  } else {
    crsp_df
  }
}

