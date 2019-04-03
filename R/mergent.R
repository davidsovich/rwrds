
#' Download Mergent bond issues table
#'
#' \code{mergent_issues} downloads the Mergent bond issues table.
#'
#' Downloads issue-level bond data from the table fisd.fisd_mergedissue. Appends on information
#' from the callable, agentid, and exchangeable tables. By default, the function only downloads a
#' subset of the variables. You can download all the variables by setting the \code{subset}
#' argument to \code{FALSE}. By default, this function also cleans the data and restricts to
#' vanilla bonds, which removes: floating rates notes, convertible bonds, foreign domiciled
#' issuers, foreign currency notes, private placements, perpetuals, preferred securities,
#' retail notes, slobs, exchangeables, unit deals, pay-in-kinds, defeased bonds, and bonds with
#' non-strictly positive offering amounts and missing coupon types, issue ids, maturity, and
#' offering date. By default, also merges on initial credit ratings for the issue and makes
#' simple variable transformations.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param clean Optional Boolean. Clean the data to remove data errors and missings? Defaults to
#' \code{TRUE}.
#' @param vanilla Optional Boolean. Restrict data to common vanilla bonds? Defaults to \code{TRUE}.
#' @param subset Optional Boolean. Clean the data for errors and missing fields? Defaults to
#' \code{TRUE}.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' mergent_issues_df = mergent_issues(wrds, clean = TRUE, vanilla = TRUE, subset = TRUE, dl = FALSE)
mergent_issues = function(wrds, clean = TRUE, vanilla = TRUE, subset = TRUE, dl = TRUE) {
  if(subset == TRUE) {
    variables = mergent_standard_variables()
  } else {
    variables = wrds_variable_list(wrds = wrds,
                                   schema = "fisd",
                                   table = "fisd_mergedissue")$column_name
  }
  mergent_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_mergedissue")) %>%
    dplyr::select(dplyr::one_of(variables)) %>%
    dplyr::left_join(y = mergent_callable(wrds = wrds, dl = FALSE),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::left_join(y = mergent_agent(wrds = wrds, dl = FALSE),
                     by = c("issuer_id" = "issuer_id")) %>%
    dplyr::left_join(y = mergent_ticker(wrds = wrds, dl = FALSE),
                     by = c("issuer_id" = "issuer_id")) %>%
    dplyr::mutate(maturity_year = date_part('year', maturity),
                  offering_year = date_part('year', offering_date),
                  maturity_length = as.numeric(maturity - offering_date)/365.25,
                  callable = ifelse(is.na(callable), "N", callable),
                  sinking_fund = ifelse(is.na(sinking_fund), "N", sinking_fund),
                  offering_date = offering_date,
                  finance_or_utility_flag = ifelse(industry_group %in% c(2,3), 1, 0),
                  foreign_domiciled_issuer_flag = ifelse(country_domicile != "USA", 1, 0),
                  has_options_flag = ifelse(convertible == "Y" | putable == "Y" |
                                            callable == "Y" | sinking_fund == "Y", 1, 0)) %>%
    dplyr::left_join(y = mergent_initial_ratings(wrds, dl = FALSE),
                     by = c("issue_id" = "issue_id")) %>%
    # dbplyr does not provide distinct(, .keep_all = TRUE) functionality
    dplyr::group_by(issue_id) %>%
    dplyr::mutate(row_num = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(row_num == 1) %>%
    dplyr::select(-row_num)
  if(clean == TRUE) {
    mergent_df = mergent_df %>%
      dplyr::filter(offering_amt > 0,
                    !is.na(coupon_type),
                    !is.na(issue_id),
                    !is.na(maturity),
                    !is.na(offering_date),
                    !is.na(offering_price),
                    offering_price >= 10,
                    !is.na(coupon),
                    (!is.na(offering_yield) & offering_yield > 0) |
                      (!is.na(treasury_spread) & treasury_spread > 0 ))
  }
  if(vanilla == TRUE) {
    mergent_df = mergent_df %>%
      dplyr::filter(foreign_currency == "N",
                    coupon_type %in% c("F", "Z"),
                    private_placement == "N",
                    perpetual == "N",
                    preferred_security == "N",
                    bond_type != "RNT",
                    slob == "N",
                    exchangeable == "N",
                    unit_deal == "N",
                    pay_in_kind == "N",
                    defeased == "N",
                    convertible == "N",
                    foreign_domiciled_issuer_flag == 0,
                    maturity_length >= 1)

  }
  if(dl == TRUE) {
    mergent_df %>% dplyr::collect()
  } else {
    mergent_df
  }
}

#' Download Mergent corporate bonds
#'
#' \code{mergent_corporates} downloads corporate bond issues from the Mergent issues table
#'
#' Downloads issue-level bond data from the table fisd.fisd_mergedissue for corporate bonds.
#' Corporates are defined by being in utility, financial, or industrial Mergent industry groups,
#' having bond types in CDEB, CMTN, CZ, CMTZ, UCID, or CP, and having security levels in
#' SEN, SENS, SUB, or SS. This function calls the \code{mergent_issues} function to download the
#' data You can download all the variables by setting the \code{subset}
#' argument to \code{FALSE}. By default, this function also cleans the data and restricts to
#' vanilla corporates, which removes: floating rates notes, convertible bonds, foreign domiciled
#' issuers, foreign currency notes, private placements, perpetuals, preferred securities,
#' retail notes, slobs, exchangeables, unit deals, pay-in-kinds, defeased bonds, and bonds with
#' non-strictly positive offering amounts and missing coupon types, issue ids, maturity, and
#' offering date. By default, also merges on initial credit ratings for the issue and makes
#' simple variable transformations.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param clean Optional Boolean. Clean the data to remove data errors and missings? Defaults to
#' \code{TRUE}.
#' @param vanilla Optional Boolean. Restrict data to common vanilla bonds? Defaults to \code{TRUE}.
#' @param subset Optional Boolean. Clean the data for errors and missing fields? Defaults to
#' \code{TRUE}.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' mergent_corps = mergent_corporates(wrds, clean = TRUE, vanilla = TRUE, subset = TRUE, dl = FALSE)
mergent_corporates = function(wrds, clean = TRUE, vanilla = TRUE, subset = TRUE, dl = TRUE) {
  mergent_df = mergent_issues(wrds = wrds,
                              clean = clean,
                              vanilla = vanilla,
                              subset = subset,
                              dl = FALSE) %>%
    dplyr::filter(!(industry_group %in% c(4,5)),
                  bond_type %in% c("CDEB", "CMTN", "CZ", "CMTZ", "UCID", "CP"),
                  security_level %in% c("SEN", "SENS", "SUB", "SS" ))
  if(dl == TRUE) {
    mergent_df %>% dplyr::collect()
  } else {
    mergent_df
  }
}







