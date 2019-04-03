
#' Download Mergent bond issues table
#'
#' \code{mergent_issues} downloads the Mergent bond issues table.
#'
#' Downloads issue-level bond data from the table fisd.fisd_mergedissue. Appends on information
#' from the callable, agentid, and exchangeable tables. By default, the function only downloads a
#' subset of the variables. You can download all the variables by setting the \code{subset}
#' argument to \code{FALSE}. By default, this function also cleans the data of the following
#' types of bonds: foreign currency notes, private placements, perpetuals, preferred securities,
#' retail notes, slobs, exchangeables, unit deals, pay-in-kinds, defeased bonds, and bonds with
#' non-strictly positive offering amounts and missing coupon types, issue ids, maturity, and
#' offering date. By default, also merges on initial credit ratings for the issue and makes
#' simple variable transformations.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param clean Optional Boolean. Clean the data to receive common bonds? Defaults to
#' \code{TRUE}.
#' @param subset Optional Boolean. Download recommended subset of variables? Defaults to
#' \code{TRUE}.
#' @param dl Optional Boolean. Download the data? Defaults to \code{TRUE}. \code{FALSE} outputs a
#' lazy \code{dplyr} table reference.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' mergent_issues_df = mergent_issues(wrds = wrds, clean = TRUE, subset = TRUE, dl = FALSE)
mergent_issues = function(wrds, clean = TRUE, subset = TRUE, dl = TRUE) {
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
                     by = c("issue_id" = "issue_id"))
  if(clean == TRUE) {
    mergent_df = mergent_df %>%
      dplyr::filter(foreign_currency == "N",
                    private_placement == "N",
                    perpetual == "N",
                    preferred_security == "N",
                    bond_type != "RNT",
                    slob == "N",
                    exchangeable == "N",
                    unit_deal == "N",
                    pay_in_kind == "N",
                    defeased == "N",
                    offering_amt > 0,
                    !is.na(coupon_type),
                    !is.na(issue_id),
                    !is.na(maturity),
                    !is.na(offering_date))
  }
  if(dl == TRUE) {
    mergent_df %>% dplyr::collect()
  } else {
    mergent_df
  }
}
