
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
#' simple variable transformations. Resticts to SEN, SENS, SUB, oR SS on campital structure,
#' although this is mostly without loss of generality (JUN, JUNS, NON comprise only 150 issues)
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


#' Mergent yearly amount outstanding panel
#'
#' \code{mergent_yearly_ao} constructs a issue-year panel of amount outstandings.
#'
#' Combines the Mergent issue table with the Mergent historical amount outstanding panel to
#' create a year-by-year summary of each issue's amount outstanding inclusive of bond event
#' (e.g., calling the bond). Includes yearly entries for issues only if their maturity is
#' less than the year of interest and their offering date is at least the year of interest.
#' Flags issue-year observations whenever there is zero estimated amount oustanding (e.g., the
#' bond has been called) but does not remove the observations. Construction is memory intensive
#' (several one-to-many merges) and requires support from the WRDS cloud. Constructs panel for
#' all bond issues and does not filter on any fields except positive initial amount oustanding
#' and non-missing maturity and offering years. Run-time is fast if WRDS cloud is leveraged.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric. Default is 1995 (first year of updates to amount outstanding table).
#' @param end_year Numeric. Default is 2019.
#' @param dl Optional Boolean. Download the data? Defaults to \code{FALSE}.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' mergent_ao_panel = mergent_yearly_ao(wrds, begin_year = 1995, end_year = 2019, dl = TRUE)
#' table(mergent_ao_panel$year)
#' head(mergent_ao_panel %>% filter(issue_id == 1) %>% data.frame())
mergent_yearly_ao = function(wrds, begin_year = 1995, end_year = 2019, dl = FALSE) {
  mergent_df = mergent_issues(wrds = wrds, clean = FALSE, vanilla = FALSE, dl = FALSE) %>%
    dplyr::filter(offering_amt > 0,
                  !is.na(maturity_year),
                  !is.na(offering_year),
                  maturity_year >= begin_year,
                  offering_year <= end_year) %>%
    dplyr::select(issue_id, issuer_id, offering_year, maturity_year, offering_amt) %>%
    dplyr::mutate(merge_dummy = 1) %>%
    dplyr::left_join(y = year_table(wrds = wrds, begin_year = begin_year, end_year = end_year),
                     by = c("merge_dummy" = "merge_dummy")) %>%
    dplyr::select(issue_id, issuer_id, year, offering_year, maturity_year, offering_amt) %>%
    dplyr::filter(year <= maturity_year) %>%
    dplyr::left_join(y = mergent_historical_ao(wrds = wrds, dl = FALSE) %>%
                       dplyr::select(issue_id, effective_year, min_effective_year,
                                     amount_outstanding),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::filter((is.na(effective_year) | effective_year <= year | year <= min_effective_year),
                  year >= offering_year) %>%
    dplyr::mutate(dist_effective = ifelse(year <= min_effective_year,
                                          abs(year - effective_year),
                                          year - effective_year)) %>%
    dplyr::group_by(issue_id, year) %>%
    dplyr::mutate(min_dist = min(dist_effective, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::filter((is.na(min_dist) | dist_effective == min_dist)) %>%
    dplyr::mutate(amount_outstanding = ifelse(year < effective_year, NA, amount_outstanding)) %>%
    dplyr::mutate(estimated_amount_outstanding = dplyr::case_when(
      year == offering_year      ~ offering_amt,
      !is.na(amount_outstanding) ~ amount_outstanding,
      TRUE                       ~ offering_amt)) %>%
    dplyr::select(issue_id, issuer_id, year, offering_year, maturity_year, min_effective_year,
                  estimated_amount_outstanding, offering_amt, amount_outstanding) %>%
    dplyr::rename(table_amount_outstanding = amount_outstanding) %>%
    dplyr::mutate(flag_zero_ao = ifelse(estimated_amount_outstanding == 0, 1, 0),
                  flag_new_issue_year = ifelse(year == offering_year, 1, 0),
                  flag_maturity_year = ifelse(year == maturity_year, 1, 0))
  if(dl == TRUE) {
    mergent_df %>% dplyr::collect()
  } else {
    mergent_df
  }
}

# There are gaps in this panel below: Do you want to cartesian the space up? Need to download
# intermitently to add on the ratings numbers (and it slows down for some reason).

mergent_issuer_panel = function(wrds, begin_year, end_year, corps_only = FALSE, clean = FALSE,
                                vanilla = FALSE, min_offering_amt = 1) {
  ao_df = mergent_yearly_ao(wrds=wrds, begin_year=begin_year, end_year=end_year, dl=TRUE) %>%
    dplyr::filter(offering_amt >= min_offering_amt)
  if(corps_only == TRUE) {
    temp_df = mergent_corporates(wrds = wrds, clean = clean, vanilla = vanilla, dl = TRUE)
  } else {
    temp_df = mergent_issues(wrds = wrds, clean = clean, vanilla = vanilla, dl = TRUE)
  }
  temp_df = temp_df %>%
    mergent_add_ratings_no(sp_col_name = "sp_initial_rating",
                           moodys_col_name = "moodys_initial_rating",
                           fitch_col_name = "fitch_initial_rating",
                           sp_name = "new_sp_rating_num",
                           fitch_name = "new_fitch_rating_num",
                           moodys_name = "new_moodys_rating_num") %>%
    dplyr::select(issue_id, issuer_id, coupon, maturity_length, has_options_flag,
                  offering_yield, treasury_spread, offering_price,
                  new_sp_rating_num, new_moodys_rating_num, new_fitch_rating_num) %>%
    dplyr::mutate(sp_new_ig = ifelse(is.na(new_sp_rating_num), NA,
                                 ifelse(new_sp_rating_num <= 10, 1, 0)),
                  m_new_ig = ifelse(is.na(new_moodys_rating_num), NA,
                                ifelse(new_moodys_rating_num <= 10, 1, 0)),
                  f_new_ig = ifelse(is.na(new_fitch_rating_num), NA,
                                ifelse(new_fitch_rating_num <= 10, 1, 0)))
  ao_df = ao_df %>%
    dplyr::inner_join(y = temp_df,
                      by = c("issue_id" = "issue_id", "issuer_id" = "issuer_id"))
  ao_df = ao_df %>%
    dplyr::filter(flag_zero_ao == 0) %>%
    dplyr::mutate(maturity_left = maturity_year - year) %>%
    dplyr::group_by(issuer_id, year) %>%
    dplyr::summarise(num_bonds = dplyr::n(),
                     amount_oustanding = sum(estimated_amount_outstanding, na.rm = TRUE),
                     wavg_coupon = weighted.mean(coupon,
                                                 estimated_amount_outstanding, na.rm = TRUE),
                     wavg_mat_left = weighted.mean(maturity_left,
                                                   estimated_amount_outstanding, na.rm = TRUE),
                     num_maturing_bonds = sum(flag_maturity_year, na.rm = TRUE),
                     amount_maturing_bonds = sum(flag_maturity_year*estimated_amount_outstanding,
                                                 na.rm = TRUE),
                     num_new_bonds = sum(flag_new_issue_year, na.rm = TRUE),
                     amount_new_bonds = sum(flag_new_issue_year*offering_amt, na.rm = TRUE),
                     wavg_coupon_new_bonds = weighted.mean(coupon*flag_new_issue_year,
                                                           offering_amt*flag_new_issue_year,
                                                           na.rm = TRUE),
                     wavg_mat_new_bonds = weighted.mean(maturity_length*flag_new_issue_year,
                                                        maturity_length*flag_new_issue_year,
                                                        na.rm = TRUE),
                     wavg_yield_new_bonds = weighted.mean(offering_yield*flag_new_issue_year,
                                                          offering_yield*flag_new_issue_year,
                                                          na.rm = TRUE),
                     wavg_price_new_bonds = weighted.mean(offering_price*flag_new_issue_year,
                                                          offering_price*flag_new_issue_year,
                                                          na.rm = TRUE),
                     wavg_sp_new_bonds = weighted.mean(new_sp_rating_num*flag_new_issue_year,
                                                       new_sp_rating_num*flag_new_issue_year,
                                                       na.rm = TRUE),
                     wavg_moodys_new_bonds = weighted.mean(new_moodys_rating_num*flag_new_issue_year,
                                                           new_moodys_rating_num*flag_new_issue_year,
                                                           na.rm = TRUE),
                     wavg_fitch_new_bonds = weighted.mean(new_fitch_rating_num*flag_new_issue_year,
                                                          new_fitch_rating_num*flag_new_issue_year,
                                                          na.rm = TRUE),
                     sp_ig_new_bonds = sum(sp_new_ig*flag_new_issue_year, na.rm = TRUE),
                     moodys_ig_new_bonds = sum(m_new_ig*flag_new_issue_year, na.rm = TRUE),
                     fitch_ig_new_bonds = sum(f_new_ig*flag_new_issue_year, na.rm = TRUE)) %>%
    dplyr::mutate(wavg_coupon_new_bonds = ifelse(is.nan(wavg_coupon_new_bonds),
                                                 NA, wavg_coupon_new_bonds),
                  wavg_mat_new_bonds = ifelse(is.nan(wavg_mat_new_bonds),
                                                 NA, wavg_mat_new_bonds),
                  wavg_yield_new_bonds = ifelse(is.nan(wavg_yield_new_bonds),
                                                 NA, wavg_yield_new_bonds),
                  wavg_price_new_bonds = ifelse(is.nan(wavg_price_new_bonds),
                                                 NA, wavg_price_new_bonds),
                  wavg_sp_new_bonds = ifelse(is.nan(wavg_sp_new_bonds),
                                                 NA, wavg_sp_new_bonds),
                  wavg_moodys_new_bonds = ifelse(is.nan(wavg_moodys_new_bonds),
                                                 NA, wavg_moodys_new_bonds),
                  wavg_fitch_new_bonds = ifelse(is.nan(wavg_fitch_new_bonds),
                                                 NA, wavg_fitch_new_bonds))
  ao_df
}











