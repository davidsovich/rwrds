
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
    dplyr::left_join(y = sp_initial_ratings(wrds = wrds, dl = FALSE, date_distance = 1) %>%
                         dplyr::select(-rating_date),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::left_join(y = moodys_initial_ratings(wrds = wrds, dl = FALSE, date_distance = 1) %>%
                         dplyr::select(-rating_date),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::left_join(y = fitch_initial_ratings(wrds = wrds, dl = FALSE, date_distance = 1) %>%
                         dplyr::select(-rating_date),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::mutate(sp_initial_rating = ifelse(is.na(sp_initial_rating), "NR", sp_initial_rating),
                  sp_initial_num_rating = ifelse(is.na(sp_initial_num_rating),
                                                 23, sp_initial_num_rating),
                  moodys_initial_rating = ifelse(is.na(moodys_initial_rating),
                                                 "NR", moodys_initial_rating),
                  moodys_initial_num_rating = ifelse(is.na(moodys_initial_num_rating),
                                                     23, moodys_initial_num_rating),
                  fitch_initial_rating = ifelse(is.na(fitch_initial_rating),
                                                "NR", fitch_initial_rating),
                  fitch_initial_num_rating = ifelse(is.na(fitch_initial_num_rating),
                                                    23, fitch_initial_num_rating)) %>%
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


#' Mergent yearly credit ratings
#'
#' \code{mergent_yearly_ao} constructs an issue-year panel of S&P, Moodys, and Fitch ratings.
#'
#' Constructs a year-by-year account of issue-level credit ratings from the Mergent issues table
#' and the historical credit ratings table. Considers only credit ratings from S&P, Moodys, and
#' Fitch. Only keeps issue observations between the offering and maturity date. Does not remove
#' a bond if the entire issue is called. Only filter applied is offering amounts above zero.
#' Assigns a numeric rating to each agency's letter rating. Lower numeric ratings signal higher
#' perceived credit quality; numeric ratings at-or-below 10 are considered investment grade.
#' Ratings are on a scale of 1 (highest perceived quality) to 22 (lowest perceived quality).
#' The following items are worth noting: (1) issues that are not rated by an agency are assigned
#' a rating of 'NR' for that agency and a numeric code of 23, (2) the Mergent database states that
#' the ratings data is only supported after 1995, despite there being pre-1995 ratings in the data.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric. Default is 1995 (first year of updates to ratings table).
#' @param end_year Numeric. Default is 2019.
#' @param dl Optional Boolean. Download the data? Defaults to \code{FALSE}.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' mergent_ratings_panel = mergent_yearly_ratings(wrds, begin_year = 1995, end_year = 2000,
#' dl = TRUE)
#' table(mergent_ratings_panel$year)
#' head(mergent_ratings_panel %>% filter(issue_id == 1) %>% data.frame())
mergent_yearly_ratings = function(wrds, begin_year = 1995, end_year = 2019, dl = FALSE) {
  rating_df = mergent_issues(wrds = wrds, clean = FALSE, vanilla = FALSE, dl = FALSE) %>%
    dplyr::filter(offering_amt > 0,
                  offering_amt < 10000000,
                  !is.na(maturity_year),
                  !is.na(offering_year),
                  maturity_year >= begin_year,
                  offering_year <= end_year) %>%
    dplyr::select(issue_id, issuer_id, offering_year, maturity_year) %>%
    dplyr::mutate(merge_dummy = 1) %>%
    dplyr::left_join(y = year_table(wrds = wrds, begin_year = begin_year, end_year = end_year),
                     by = c("merge_dummy" = "merge_dummy")) %>%
    dplyr::filter(year <= maturity_year, year >= offering_year) %>%
    # SP ratings (>= and < join - not explictly supported by dplyr)
    dplyr::left_join(y = sp_historical_ratings(wrds, dl = FALSE) %>%
                       dplyr::select(issue_id, rating_year, sp_rating, sp_num_rating),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::mutate(dist_effective = dplyr::case_when(
      is.na(rating_year) ~ 100,
      year < rating_year ~ 100 + (rating_year - year),
      TRUE               ~ year - rating_year)) %>%
    dplyr::group_by(issue_id, year) %>%
    dplyr::filter(dist_effective == min(dist_effective, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(sp_rating = ifelse(year < rating_year, "NR", sp_rating),
                  sp_num_rating = ifelse(year < rating_year, 23, sp_num_rating)) %>%
    dplyr::select(-one_of("merge_dummy", "rating_year", "dist_effective")) %>%
    # M ratings (>= and < join - not explictly supported by dplyr)
    dplyr::left_join(y = moodys_historical_ratings(wrds, dl = FALSE) %>%
                       dplyr::select(issue_id, rating_year, moodys_rating, moodys_num_rating),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::mutate(dist_effective = dplyr::case_when(
      is.na(rating_year) ~ 100,
      year < rating_year ~ 100 + (rating_year - year),
      TRUE               ~ year - rating_year)) %>%
    dplyr::group_by(issue_id, year) %>%
    dplyr::filter(dist_effective == min(dist_effective, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(moodys_rating = ifelse(year < rating_year, "NR", moodys_rating),
                  moodys_num_rating = ifelse(year < rating_year, 23, moodys_num_rating)) %>%
    dplyr::select(-one_of("rating_year", "dist_effective")) %>%
    # F ratings (>= and < join - not explictly supported by dplyr)
    dplyr::left_join(y = fitch_historical_ratings(wrds, dl = FALSE) %>%
                       dplyr::select(issue_id, rating_year, fitch_rating, fitch_num_rating),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::mutate(dist_effective = dplyr::case_when(
      is.na(rating_year) ~ 100,
      year < rating_year ~ 100 + (rating_year - year),
      TRUE               ~ year - rating_year)) %>%
    dplyr::group_by(issue_id, year) %>%
    dplyr::filter(dist_effective == min(dist_effective, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(fitch_rating = ifelse(year < rating_year, "NR", fitch_rating),
                  fitch_num_rating = ifelse(year < rating_year, 23, fitch_num_rating)) %>%
    dplyr::select(-one_of("rating_year", "dist_effective"))
  if(dl == TRUE) {
    rating_df %>% dplyr::collect()
  } else {
    rating_df
  }
}


#' Mergent issuer panel summary
#'
#' \code{mergent_issuer_panel} constructs a yearly panel of issuer-level bond summaries.
#'
#' Aggregates issue-level bond data on an annual basis to the issuer level on an annual basis.
#' Summarizes several key features of each issuers bond issuance in each year, including
#' the number of bonds currently outstanding, the distribution of bond sizes, the average
#' bond ratings, the features of maturing bonds, and the features of newly issued bonds
#' within the year.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param begin_year Numeric.
#' @param end_year Numeric.
#' @param corps_only Boolean. Summarize corporate bonds only? Defaults to \code{FALSE}. If true,
#' function uses the filters in \code{mergent_corporates}.
#' @param clean Boolean. Restrict summary to bond issues with clean data? Clean is defined
#' as in the function \code{mergent_issues}.
#' @param vanilla Boolean. Restrict summary to vanilla bond issues only? Vanilla is defined
#' as in the function \code{mergent_issues}.
#' @param min_offering_amt Numeric. Mimum size (in thousands) for bonds to be included in the
#' summary.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' mergent_df = mergent_issuer_panel(wrds = wrds, begin_year = 2000, end_year = 2002,
#' corps_only = TRUE, clean = TRUE, vanilla = TRUE, min_offering_amt = 1000)
mergent_issuer_panel = function(wrds, begin_year, end_year, corps_only = FALSE, clean = FALSE,
                                vanilla = FALSE, min_offering_amt = 1) {
  ao_df = mergent_yearly_ao(wrds = wrds,
                            begin_year = begin_year,
                            end_year = end_year,
                            dl = TRUE) %>%
    dplyr::filter(flag_zero_ao == 0) %>%
    dplyr::mutate(maturity_left = maturity_year - year)
  if(corps_only == TRUE) {
    temp_df = mergent_corporates(wrds = wrds, clean = clean, vanilla = vanilla, dl = TRUE)
  } else {
    temp_df = mergent_issues(wrds = wrds, clean = clean, vanilla = vanilla, dl = TRUE)
  }
  ao_df = ao_df %>%
    dplyr::inner_join(y = temp_df %>%
                          dplyr::select(issue_id, industry_code, industry_group, naics_code,
                                        finance_or_utility_flag, has_options_flag,
                                        maturity_length, offering_yield, treasury_spread,
                                        offering_price, coupon, sp_initial_num_rating,
                                        moodys_initial_num_rating, fitch_initial_num_rating),
                      by = c("issue_id" = "issue_id")) %>%
    dplyr::left_join(y = sp_historical_ratings(wrds = wrds, dl = TRUE),
                     by = c("issue_id" = "issue_id", "year" = "rating_year")) %>%
    dplyr::left_join(y = moodys_historical_ratings(wrds = wrds, dl = TRUE),
                     by = c("issue_id" = "issue_id", "year" = "rating_year")) %>%
    dplyr::left_join(y = fitch_historical_ratings(wrds = wrds, dl = TRUE),
                     by = c("issue_id" = "issue_id", "year" = "rating_year")) %>%
    dplyr::mutate(best_num_rating = pmin(sp_num_rating, moodys_num_rating,
                                         fitch_num_rating, na.rm = TRUE),
                  best_initial_rating = pmin(sp_initial_num_rating, moodys_initial_num_rating,
                                             fitch_initial_num_rating, na.rm = TRUE)) %>%
    dplyr::mutate(best_num_rating = ifelse(is.na(best_num_rating), 23, best_num_rating),
                  best_initial_rating = ifelse(is.na(best_initial_rating), 23, best_initial_rating),
                  sp_initial_num_rating = ifelse(sp_initial_num_rating == 23,
                                                 0, sp_initial_num_rating),
                  moodys_initial_num_rating = ifelse(moodys_initial_num_rating == 23,
                                                     0, moodys_initial_num_rating),
                  fitch_initial_num_rating = ifelse(fitch_initial_num_rating == 23,
                                                    0, fitch_initial_num_rating),
                  sp_num_rating = ifelse(is.na(sp_num_rating) | sp_num_rating == 23,
                                         0, sp_num_rating),
                  moodys_num_rating = ifelse(is.na(moodys_num_rating) | moodys_num_rating == 23,
                                         0, moodys_num_rating),
                  fitch_num_rating = ifelse(is.na(fitch_num_rating) | fitch_num_rating == 23,
                                         0, fitch_num_rating),
                  sp_inv_grade = ifelse(is.na(sp_inv_grade), 0, sp_inv_grade),
                  moodys_inv_grade = ifelse(is.na(moodys_inv_grade), 0, moodys_inv_grade),
                  fitch_inv_grade = ifelse(is.na(fitch_inv_grade), 0, fitch_inv_grade),
                  sp_rated = ifelse(is.na(sp_rating) | sp_rating == "NR", 0, 1),
                  moodys_rated = ifelse(is.na(moodys_rating) | moodys_rating == "NR", 0, 1),
                  fitch_rated = ifelse(is.na(fitch_rating) | fitch_rating == "NR", 0, 1),
                  eao = estimated_amount_outstanding,
                  fni = flag_new_issue_year) %>%
    dplyr::mutate(agencies_rated_by = sp_rated + moodys_rated + fitch_rated,
                  rated_by_any_flag = ifelse(agencies_rated_by > 0, 1, 0),
                  avg_rating = (sp_num_rating + moodys_num_rating + fitch_num_rating) /
                    (agencies_rated_by),
                  avg_rating = ifelse(is.nan(avg_rating), NA, avg_rating),
                  inv_grade_by_any_flag = ifelse(sp_inv_grade + fitch_inv_grade + moodys_inv_grade > 0,
                                                 1, 0),
                  avg_is_inv_grade_flag = ifelse(avg_rating <= 10, 1, 0),
                  eao_above_25mm = ifelse(eao >= 25000, 1, 0),
                  eao_above_50mm = ifelse(eao >= 50000, 1, 0),
                  eao_above_100mm = ifelse(eao >= 100000, 1, 0),
                  eao_above_150mm = ifelse(eao >= 150000, 1, 0),
                  eao_above_200mm = ifelse(eao >= 200000, 1, 0),
                  eao_above_250mm = ifelse(eao >= 250000, 1, 0),
                  eao_above_300mm = ifelse(eao >= 300000, 1, 0),
                  eao_above_500mm = ifelse(eao >= 500000, 1, 0),
                  eao_above_1000mm = ifelse(eao >= 1000000, 1, 0))
  ao_df = ao_df %>%
    dplyr::group_by(issuer_id, industry_code, industry_group,
                    naics_code, finance_or_utility_flag, year) %>%
    dplyr::summarise(num_bonds = dplyr::n(),
                     amount_outstanding = sum(eao, na.rm = TRUE),
                     num_above_25mm = sum(eao_above_25mm, na.rm = TRUE),
                     num_above_50mm = sum(eao_above_50mm, na.rm = TRUE),
                     num_above_100mm = sum(eao_above_100mm, na.rm = TRUE),
                     num_above_150mm = sum(eao_above_150mm, na.rm = TRUE),
                     num_above_200mm = sum(eao_above_200mm, na.rm = TRUE),
                     num_above_250mm = sum(eao_above_250mm, na.rm = TRUE),
                     num_above_300mm = sum(eao_above_300mm, na.rm = TRUE),
                     num_above_500mm = sum(eao_above_500mm, na.rm = TRUE),
                     num_above_1000mm = sum(eao_above_1000mm, na.rm = TRUE),
                     wavg_coupon = weighted.mean(coupon, eao, na.rm = TRUE),
                     wavg_mat_left = weighted.mean(maturity_left, eao, na.rm = TRUE),
                     wavg_has_options_flag = weighted.mean(has_options_flag, eao, na.rm = TRUE),
                     num_rated_bonds = sum(rated_by_any_flag, na.rm = TRUE),
                     num_inv_grade_bonds = sum(inv_grade_by_any_flag, na.rm = TRUE),
                     wavg_best_rating = weighted.mean(best_num_rating, eao, na.rm = TRUE),
                     wavg_avg_rating = weighted.mean(avg_rating, eao, na.rm = TRUE),
                     num_maturing_bonds = sum(flag_maturity_year, na.rm = TRUE),
                     amount_maturing_bonds = sum(flag_maturity_year*eao, na.rm = TRUE),
                     num_new_bonds = sum(flag_new_issue_year, na.rm = TRUE),
                     num_new_above_25mm = sum(fni*eao_above_25mm, na.rm = TRUE),
                     num_new_above_50mm = sum(fni*eao_above_50mm, na.rm = TRUE),
                     num_new_above_100mm = sum(fni*eao_above_100mm, na.rm = TRUE),
                     num_new_above_150mm = sum(fni*eao_above_150mm, na.rm = TRUE),
                     num_new_above_200mm = sum(fni*eao_above_200mm, na.rm = TRUE),
                     num_new_above_250mm = sum(fni*eao_above_250mm, na.rm = TRUE),
                     num_new_above_300mm = sum(fni*eao_above_300mm, na.rm = TRUE),
                     num_new_above_500mm = sum(fni*eao_above_500mm, na.rm = TRUE),
                     num_new_above_1000mm = sum(fni*eao_above_1000mm, na.rm = TRUE),
                     amount_new_bonds = sum(flag_new_issue_year*eao, na.rm = TRUE),
                     wavg_coupon_new_bonds = weighted.mean(coupon*fni, eao*fni, na.rm = TRUE),
                     wavg_yield_new_bonds = weighted.mean(offering_yield*fni, eao*fni, na.rm = TRUE),
                     wavg_price_new_bonds = weighted.mean(offering_price*fni, eao*fni, na.rm = TRUE),
                     wavg_mat_new_bonds = weighted.mean(maturity_length*fni, eao*fni, na.rm = TRUE),
                     pct_rated_new_bonds = weighted.mean(fni*rated_by_any_flag,
                                                         fni, na.rm = TRUE),
                     pct_inv_grade_new_bonds = sum(fni*inv_grade_by_any_flag,
                                                   fni, na.rm = TRUE),
                     wavg_initial_rating_new_bonds = weighted.mean(best_initial_rating*fni,
                                                                   eao*fni, na.rm = TRUE)) %>%
    dplyr::mutate(wavg_coupon_new_bonds = ifelse(is.nan(wavg_coupon_new_bonds),
                                                 NA, wavg_coupon_new_bonds),
                  wavg_yield_new_bonds = ifelse(is.nan(wavg_yield_new_bonds),
                                                 NA, wavg_yield_new_bonds),
                  wavg_price_new_bonds = ifelse(is.nan(wavg_price_new_bonds),
                                                 NA, wavg_price_new_bonds),
                  wavg_mat_new_bonds = ifelse(is.nan(wavg_mat_new_bonds),
                                                 NA, wavg_mat_new_bonds),
                  pct_rated_new_bonds = ifelse(is.nan(pct_rated_new_bonds),
                                                 NA, pct_rated_new_bonds),
                  pct_inv_grade_new_bonds = ifelse(is.nan(pct_inv_grade_new_bonds),
                                                 NA, pct_inv_grade_new_bonds),
                  wavg_initial_rating_new_bonds = ifelse(is.nan(wavg_initial_rating_new_bonds),
                                                 NA, wavg_initial_rating_new_bonds))
  # Merge onto a cartesian of this year with issuer id and keep min and max and hold the gaps or
  #remove completley and mark it
  ao_df
}













