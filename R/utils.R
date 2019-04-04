

# Standard Compustat variables --------------------------------------------------------------------
compustat_annual_standard_variables = function() {

  #Company identifier variables
  id_vars = c("gvkey", "iid", "fyear", "datadate", "fyr", "tic", "conm", "cusip")

  #Assets variables
  asset_vars = c("at", "act", "ch", "che", "ppegt", "ppent", "gdwl", "intan", "invt")

  #Liability variables
  liability_vars = c("lt", "lct", "dltt", "dlc", "dd1", "dd2", "dd3", "dd4", "dd5")

  #Equity variables (some from misc. or cash flow section)
  equity_vars = c("ceq", "seq", "re", "csho", "mkvalt", "dvpsp_f", "prcc_f", "dvc", "dvt")

  #Income and expenses variables (excluding R&D)
  income_vars = c("revt", "cogs", "xsga", "dp", "ebitda", "ebit", "oiadp", "ib", "pi", "ni")

  #Cash flow and investment variables (including R&D)
  cash_vars = c("aqc", "capx", "xrd", "dltis", "dltr")

  #Return variable list
  c(id_vars, asset_vars, liability_vars, equity_vars, income_vars, cash_vars)

}

compustat_names_variables = function() {
  c("gvkey", "cik", "sic", "naics") #year1 year2
}

# Compustat cleaning functions --------------------------------------------------------------------

compustat_annual_remove_dups = function(comp_df) {

  #Count number of NAs for each row in the database
  comp_df$num_nas = apply(comp_df, 1, function(x) sum(is.na(x)))

  # Remove duplicates by keeping gvkey-fyear with fewest NA observations
  comp_df = comp_df %>%
    dplyr::arrange( gvkey, fyear, num_nas ) %>%
    dplyr::distinct( gvkey, fyear, .keep_all = TRUE )

  # Return the data
  return(comp_df)

}

# Standard CRSP variables -------------------------------------------------------------------------


# Standard Mergent variables ----------------------------------------------------------------------

mergent_standard_variables = function() {

  #Issue and issuer identifier variables
  id_vars = c("complete_cusip", "issuer_cusip", "issue_cusip", "isin", "issue_id", "issuer_id", "active_issue")

  #Issue characteristics
  issue_vars = c("bond_type", "security_level", "mtn", "enhancement", "rule_144a", "coupon_type",
                 "coupon", "offering_amt", "day_count_basis", "foreign_currency", "perpetual",
                 "private_placement", "preferred_security", "slob", "exchangeable", "unit_deal",
                 "pay_in_kind", "defeased")

  #Option variables
  option_vars = c("convertible", "asset_backed", "putable")

  #Date variables
  date_vars = c("offering_date", "maturity")

  #Pricing and yield variables
  pricing_vars = c("treasury_spread", "offering_price", "offering_yield")

  #Return variable list
  c(id_vars, issue_vars, option_vars, date_vars, pricing_vars)

}

# Mergent helper databases ------------------------------------------------------------------------

# Mergent callable and sinking fund flag table
mergent_callable = function(wrds, dl = FALSE) {
  callable_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_redemption")) %>%
    dplyr::select(issue_id, callable, sinking_fund) %>%
    # dplyr does not support distinct(, .keep_all = TRUE) for PostgreSQL
    dplyr::group_by(issue_id) %>%
    dplyr::mutate( row_num = dplyr::row_number() ) %>%
    dplyr::ungroup() %>%
    dplyr::filter( row_num == 1 ) %>%
    dplyr::select(-row_num)
  if(dl == TRUE) {
    callable_df %>% dplyr::collect()
  } else {
    callable_df
  }
}

# Mergent agent ID and industry table
mergent_agent = function(wrds, dl = FALSE) {
  agent_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_issuer")) %>%
    dplyr::select(issuer_id, agent_id, industry_group,
                  industry_code, naics_code, country_domicile ) %>%
    dplyr::mutate(industry_group = as.numeric(industry_group),
                  industry_code = as.numeric(industry_code))
  if(dl == TRUE) {
    agent_df %>% dplyr::collect()
  } else {
    agent_df
  }
}

# Mergent stock ticker table
mergent_ticker = function(wrds, dl = FALSE) {
  ticker_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_exchange_listing")) %>%
    dplyr::filter(!is.na(ticker),
                  exchange %in% c("N", "NAS", "A")) %>%
    # dplyr does not support distinct(, .keep_all = TRUE) for PostgreSQL
    dplyr::group_by(issuer_id) %>%
    dplyr::mutate(row_num = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(row_num == 1) %>%
    dplyr::select(-row_num)
  if(dl == TRUE) {
    ticker_df %>% dplyr::collect()
  } else {
    ticker_df
  }
}

# Mergent initial credit ratings for bond issuances
mergent_initial_ratings = function(wrds, dl = FALSE, date_distance = 1) {
  ratings_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_ratings")) %>%
    dplyr::filter(rating_type %in% c("MR", "FR", "SPR"),
                  reason == "IN",
                  !is.na(rating),
                  rating != "NR") %>%
    # dplyr does not support distinct(, .keep_all = TRUE) for PostgreSQL
    dplyr::group_by(issue_id, rating_type) %>%
    dplyr::mutate(row_num = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(row_num == 1) %>%
    dplyr::select(issue_id, rating_type, rating, rating_date) %>%
    dplyr::inner_join(y = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_mergedissue")) %>%
                          dplyr::select(issue_id, offering_date),
                      by = c("issue_id" = "issue_id")) %>%
    dplyr::mutate(dist = abs(rating_date - offering_date)/365.25) %>%
    dplyr::filter(!is.na(dist),
                  dist <= date_distance) %>%
    dplyr::select(issue_id, rating_type, rating, rating_date) %>%
    dplyr::mutate(sp_initial_rating = ifelse(rating_type == "SPR", tolower(rating), NA),
                  moodys_initial_rating = ifelse(rating_type == "MR", tolower(rating), NA),
                  fitch_initial_rating = ifelse(rating_type == "FR", tolower(rating), NA)) %>%
    dplyr::group_by(issue_id) %>%
    dplyr::summarise(sp_initial_rating = max(sp_initial_rating, na.rm = TRUE),
                     moodys_initial_rating = max(moodys_initial_rating, na.rm = TRUE),
                     fitch_initial_rating = max(fitch_initial_rating, na.rm = TRUE),
                     first_rating_date = min(rating_date, na.rm = TRUE))
  if(dl == TRUE) {
    ratings_df %>% dplyr::collect()
  } else {
    ratings_df
  }
}


# Tack on ratings numbers rating_tto a given set of ratings
mergent_add_ratings_no = function(mergent_df, sp_col_name, moodys_col_name, fitch_col_name,
                          sp_name = "sp_rating_num", moodys_name = "moodys_rating_num",
                          fitch_name = "fitch_rating_num", lcase = TRUE) {
  data("rating_table", envir = environment())
  # Convert to lower case if needed and create new merging variable
  if(lcase == TRUE) {
    rating_table = rating_table %>%
      dplyr::mutate(m_rating = tolower(m_rating),
                    s_rating = tolower(s_rating),
                    f_rating = tolower(f_rating))
  }
  rating_table = rating_table %>%
    dplyr::rename(abcd1234_s = s_rating,
                  abcd1234_m = m_rating,
                  abcd1234_f = f_rating)
  # Merge on SP ratings
  if(!missing(sp_col_name)) {
    if(sp_col_name %in% names(mergent_df)) {
      mergent_df[, "abcd1234_s"] = mergent_df[, sp_col_name]
      temp_df = rating_table
      temp_df[, sp_name] = temp_df[, "num_rating"]
      temp_df = temp_df[, c("abcd1234_s", sp_name)]
      mergent_df = mergent_df %>%
        dplyr::left_join(y = temp_df,
                         by = c("abcd1234_s" = "abcd1234_s")) %>%
        dplyr::select(-dplyr::one_of(c("abcd1234_s")))
    } else {
      stop("Error! sp_column_name not found in supplied data.frame.")
    }
  }
  # Merge on moodys ratings
  if(!missing(moodys_col_name)) {
    if(moodys_col_name %in% names(mergent_df)) {
      mergent_df[, "abcd1234_m"] = mergent_df[, moodys_col_name]
      temp_df = rating_table
      temp_df[, moodys_name] = temp_df[, "num_rating"]
      temp_df = temp_df[, c("abcd1234_m", moodys_name)]
      mergent_df = mergent_df %>%
        dplyr::left_join(y = temp_df,
                         by = c("abcd1234_m" = "abcd1234_m")) %>%
        dplyr::select(-dplyr::one_of(c("abcd1234_m")))
    } else {
      stop("Error! moodys_column_name not found in supplied data.frame.")
    }
  }
  # Merge on fitch ratings
  if(!missing(fitch_col_name)) {
    if(fitch_col_name %in% names(mergent_df)) {
      mergent_df[, "abcd1234_f"] = mergent_df[, fitch_col_name]
      temp_df = rating_table
      temp_df[, fitch_name] = temp_df[, "num_rating"]
      temp_df = temp_df[, c("abcd1234_f", fitch_name)]
      mergent_df = mergent_df %>%
        dplyr::left_join(y = temp_df,
                         by = c("abcd1234_f" = "abcd1234_f")) %>%
        dplyr::select(-dplyr::one_of(c("abcd1234_f")))
    } else {
      stop("Error! fitch_column_name not found in supplied data.frame.")
    }
  }
  mergent_df
}

# Tack on industry codes to data set
mergent_add_indu_codes = function(mergent_df, merging_industry_code = "industry_code") {
  if(!(merging_industry_code %in% names(mergent_df))) {
    stop("Error! merging_industry_code must be a column in mergent_df.")
  }
  data("mergent_indu_table", envir = environment())
  mergent_df[, "abcd12345"] = mergent_df[, merging_industry_code]
  mergent_indu_table[, "abcd12345"] = mergent_indu_table[, "indu_code"]
  mergent_df = mergent_df %>%
    dplyr::left_join(y = mergent_indu_table %>%
                       dplyr::select(abcd12345, mergent_indu_name, mergent_indu_group_name),
                     by = c("abcd12345"="abcd12345")) %>%
    dplyr::select(-dplyr::one_of("abcd12345"))
}

# Mergent historical amount outstanding file (starts in 1995 per documentation) at annual frequency
mergent_historical_ao = function(wrds, dl = FALSE) {
  mergent_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_amt_out_hist")) %>%
    dplyr::mutate(effective_year = date_part('year', effective_date)) %>%
    # keep one observation per year, dplyr does not support distinct(, .keep_all = TRUE)
    dplyr::arrange(issue_id, effective_year, effective_date) %>%
    dplyr::group_by(issue_id, effective_year) %>%
    dplyr::mutate(row_num = dplyr::row_number(),
                  num_obs = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(row_num == num_obs) %>%
    dplyr::select(-dplyr::one_of(c("row_num", "num_obs")))
  if(dl == TRUE) {
    mergent_df %>% collect()
  } else {
    mergent_df
  }
}

# Tricking dplyr into pushing a year table up to the WRDS cloud
year_table = function(wrds, begin_year = 1995, end_year = 2019, dl = FALSE) {
  year_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_issuer")) %>%
    dplyr::mutate(year = dplyr::row_number()+(begin_year-1),
                  merge_dummy = 1) %>%
    dplyr::filter(year >= begin_year,
                  year <= end_year) %>%
    dplyr::select(year, merge_dummy)
  if(dl == TRUE) {
    year_df %>% dplyr::collect()
  } else {
    year_df
  }
}

# Mergent year-by-year issue amount outstanding (issue_id == 1 to test)
mergent_yearly_ao = function(wrds, begin_year = 1995, end_year = 2019, dl = FALSE) {
  mergent_df = mergent_issues(wrds = wrds, clean = FALSE, vanilla = FALSE, dl = FALSE) %>%
    dplyr::filter(offering_amt > 0,
                  !is.na(maturity_year),
                  !is.na(offering_year),
                  maturity_year >= begin_year,
                  offering_year <= end_year) %>%
    dplyr::select(issue_id, offering_year, maturity_year, offering_amt) %>%
    dplyr::mutate(merge_dummy = 1) %>%
    dplyr::left_join(y = year_table(wrds = wrds, begin_year = begin_year, end_year = end_year),
                     by = c("merge_dummy" = "merge_dummy")) %>%
    dplyr::select(issue_id, year, offering_year, maturity_year, offering_amt) %>%
    dplyr::filter(year <= maturity_year) %>%
    dplyr::left_join(y = mergent_historical_ao(wrds = wrds, dl = FALSE) %>%
                       dplyr::select(issue_id, effective_year, amount_outstanding),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::filter((is.na(effective_year) | effective_year <= year)) %>%
    dplyr::mutate(dist_effective = year - effective_year) %>%
    dplyr::group_by(issue_id, year) %>%
    dplyr::mutate(min_dist = min(dist_effective, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::filter((is.na(min_dist) | dist_effective == min_dist)) %>%
    dplyr::mutate(estimated_amount_outstanding = dplyr::case_when(
      !is.na(amount_outstanding) ~ amount_outstanding,
      TRUE                       ~ offering_amt)) %>%
    dplyr::select(issue_id, year, offering_year, maturity_year,
                  estimated_amount_outstanding, offering_amt, amount_outstanding) %>%
    dplyr::rename(table_amount_outstanding = amount_outstanding) %>%
    dplyr::mutate(flag_zero_ao = ifelse(estimated_amount_outstanding == 0, 1, 0))
  if(dl == TRUE) {
    mergent_df %>% dplyr::collect()
  } else {
    mergent_df
  }
}



# Mergent high-level issuerid summary table
mergent_issuerid_summary = function(wrds) {
  issuer_df = mergent_agent(wrds = wrds, dl = FALSE)

}





