

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
    dplyr::arrange(issue_id, effective_year, desc(effective_date)) %>%
    dplyr::group_by(issue_id, effective_year) %>%
    dplyr::mutate(row_num = dplyr::row_number(),
                  num_obs = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(row_num == num_obs) %>%
    dplyr::select(-dplyr::one_of(c("row_num", "num_obs"))) %>%
    # begin changes - remove ao updates that are for same value as initial offering amount
    dplyr::left_join(y = mergent_issues(wrds = wrds, clean = FALSE, vanilla = FALSE, dl = FALSE) %>%
                         dplyr::select(issue_id, offering_amt) %>%
                         dplyr::filter(offering_amt > 0),
                     by = c("issue_id" = "issue_id")) %>%
    dplyr::filter(amount_outstanding != offering_amt) %>%
    dplyr::select(-offering_amt) %>%
    # end changes
    dplyr::group_by(issue_id) %>%
    dplyr::mutate(min_effective_year = min(effective_year, na.rm = TRUE)) %>%
    dplyr::ungroup()
  if(dl == TRUE) {
    mergent_df %>% collect()
  } else {
    mergent_df
  }
}

# Mergent historical ratings table
mergent_historical_ratings = function(wrds, dl = FALSE) {
  rating_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_ratings")) %>%
    dplyr::filter(rating_type %in% c("MR", "FR", "SPR"),
                  !(rating %in% c("0", "Aa", "Ba", "Baa", "B+(EXP)", "Caa", "DD",
                                  "DDD", "NAV", "P-1", "SUSP"))) %>%
    dplyr::mutate(year = date_part('year', rating_date),
                  month = date_part('month', rating_date)) %>%
    dplyr::select(issue_id, rating_type, rating_date, year, month, rating) %>%
    dplyr::mutate(num_rating = dplyr::case_when(
      rating %in% c("Aaa", "AAA") ~ 1,
      rating %in% c("Aa1", "AA+") ~ 2,
      rating %in% c("Aa2", "AA") ~ 3,
      rating %in% c("Aa3", "AA-") ~ 4,
      rating %in% c("A1", "A+") ~ 5,
      rating %in% c("A2", "A") ~ 6,
      rating %in% c("A3", "A-") ~ 7,
      rating %in% c("Baa1", "BBB+") ~ 8,
      rating %in% c("Baa2", "BBB") ~ 9,
      rating %in% c("Baa3", "BBB-") ~ 10,
      rating %in% c("Ba1", "BB+") ~ 11,
      rating %in% c("Ba2", "BB") ~ 12,
      rating %in% c("Ba3", "BB-") ~ 13,
      rating %in% c("B1", "B+") ~ 14,
      rating %in% c("B2", "B") ~ 15,
      rating %in% c("B3", "B-") ~ 16,
      rating %in% c("Caa1", "CCC+") ~ 17,
      rating %in% c("Caa2", "CCC") ~ 18,
      rating %in% c("Caa3", "CCC-") ~ 19,
      rating %in% c("Ca", "CC") ~ 20,
      rating %in% c("C") ~ 21,
      rating %in% c("D") ~ 22,
      rating %in% c("NR") ~ 23,
      TRUE ~ NA)) %>%
    dplyr::mutate(inv_grade = ifelse(num_rating <= 10, 1, 0)) %>%
    # dplyr does not allow for distinct(, .keep_all = TRUE) so group_by
    dplyr::arrange(issue_id, rating_type, year, dplyr::desc(rating_date)) %>%
    dplyr::group_by(issue_id, rating_type, year) %>%
    dplyr::mutate(row_num = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(row_num == 1) %>%
    dplyr::select(-dplyr::one_of("row_num")) %>%
    dplyr::rename(rating_year = year)
  if(dl == TRUE) {
    rating_df %>% dplyr::collect()
  } else {
    rating_df
  }
}

# Class by class ratings
sp_historical_ratings = function(wrds, dl = FALSE) {
  sp_df = mergent_historical_ratings(wrds, dl = FALSE) %>%
    dplyr::filter(rating_type == "SPR") %>%
    dplyr::select(issue_id, rating_year, rating, num_rating, inv_grade) %>%
    dplyr::rename(sp_rating = rating,
                  sp_num_rating = num_rating,
                  sp_inv_grade = inv_grade)
  if(dl == TRUE) {
    sp_df %>% dplyr::collect()
  } else {
    sp_df
  }
}
moodys_historical_ratings = function(wrds, dl = FALSE) {
  mr_df = mergent_historical_ratings(wrds, dl = FALSE) %>%
    dplyr::filter(rating_type == "MR") %>%
    dplyr::select(issue_id, rating_year, rating, num_rating, inv_grade) %>%
    dplyr::rename(moodys_rating = rating,
                  moodys_num_rating = num_rating,
                  moodys_inv_grade = inv_grade)
  if(dl == TRUE) {
    mr_df %>% dplyr::collect()
  } else {
    mr_df
  }
}
fitch_historical_ratings = function(wrds, dl = FALSE) {
  fr_df = mergent_historical_ratings(wrds, dl = FALSE) %>%
    dplyr::filter(rating_type == "FR") %>%
    dplyr::select(issue_id, rating_year, rating, num_rating, inv_grade) %>%
    dplyr::rename(fitch_rating = rating,
                  fitch_num_rating = num_rating,
                  fitch_inv_grade = inv_grade)
  if(dl == TRUE) {
    fr_df %>% dplyr::collect()
  } else {
    fr_df
  }
}

# Mergent initial credit ratings for bond issuances
mergent_initial_ratings = function(wrds, dl = FALSE, date_distance = 1) {
  rating_df = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_ratings")) %>%
    dplyr::filter(rating_type %in% c("MR", "FR", "SPR"),
                  reason == "IN",
                  !(rating %in% c("0", "Aa", "Ba", "Baa", "B+(EXP)", "Caa", "DD",
                                  "DDD", "NAV", "P-1", "SUSP"))) %>%
    dplyr::filter(rating != "NR") %>%
    dplyr::group_by(issue_id, rating_type) %>%
    dplyr::mutate(row_num = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(row_num == 1) %>%
    dplyr::select(issue_id, rating_type, rating_date, rating) %>%
    dplyr::mutate(num_rating = dplyr::case_when(
      rating %in% c("Aaa", "AAA") ~ 1,
      rating %in% c("Aa1", "AA+") ~ 2,
      rating %in% c("Aa2", "AA") ~ 3,
      rating %in% c("Aa3", "AA-") ~ 4,
      rating %in% c("A1", "A+") ~ 5,
      rating %in% c("A2", "A") ~ 6,
      rating %in% c("A3", "A-") ~ 7,
      rating %in% c("Baa1", "BBB+") ~ 8,
      rating %in% c("Baa2", "BBB") ~ 9,
      rating %in% c("Baa3", "BBB-") ~ 10,
      rating %in% c("Ba1", "BB+") ~ 11,
      rating %in% c("Ba2", "BB") ~ 12,
      rating %in% c("Ba3", "BB-") ~ 13,
      rating %in% c("B1", "B+") ~ 14,
      rating %in% c("B2", "B") ~ 15,
      rating %in% c("B3", "B-") ~ 16,
      rating %in% c("Caa1", "CCC+") ~ 17,
      rating %in% c("Caa2", "CCC") ~ 18,
      rating %in% c("Caa3", "CCC-") ~ 19,
      rating %in% c("Ca", "CC") ~ 20,
      rating %in% c("C") ~ 21,
      rating %in% c("D") ~ 22,
      rating %in% c("NR") ~ 23,
      TRUE ~ NA)) %>%
    dplyr::mutate(inv_grade = ifelse(num_rating <= 10, 1, 0)) %>%
    dplyr::inner_join(y = dplyr::tbl(wrds, dbplyr::in_schema("fisd", "fisd_mergedissue")) %>%
                        dplyr::select(issue_id, offering_date),
                      by = c("issue_id" = "issue_id")) %>%
    dplyr::mutate(dist = abs(rating_date - offering_date)/365.25) %>%
    dplyr::filter(!is.na(dist),
                  dist <= date_distance)
  if(dl == TRUE) {
    rating_df %>% dplyr::collect()
  } else {
    rating_df
  }
}


# Class by class initial ratings
sp_initial_ratings = function(wrds, dl = FALSE, date_distance = 1) {
  sp_df = mergent_initial_ratings(wrds, dl = FALSE, date_distance = date_distance) %>%
    dplyr::filter(rating_type == "SPR") %>%
    dplyr::select(issue_id, rating_date, rating, num_rating, inv_grade) %>%
    dplyr::rename(sp_initial_rating = rating,
                  sp_initial_num_rating = num_rating,
                  sp_initial_inv_grade = inv_grade)
  if(dl == TRUE) {
    sp_df %>% dplyr::collect()
  } else {
    sp_df
  }
}
moodys_initial_ratings = function(wrds, dl = FALSE, date_distance = 1) {
  mr_df = mergent_initial_ratings(wrds, dl = FALSE, date_distance = date_distance) %>%
    dplyr::filter(rating_type == "MR") %>%
    dplyr::select(issue_id, rating_date, rating, num_rating, inv_grade) %>%
    dplyr::rename(moodys_initial_rating = rating,
                  moodys_initial_num_rating = num_rating,
                  moodys_initial_inv_grade = inv_grade)
  if(dl == TRUE) {
    mr_df %>% dplyr::collect()
  } else {
    mr_df
  }
}
fitch_initial_ratings = function(wrds, dl = FALSE, date_distance = 1) {
  fr_df = mergent_initial_ratings(wrds, dl = FALSE, date_distance = date_distance) %>%
    dplyr::filter(rating_type == "FR") %>%
    dplyr::select(issue_id, rating_date, rating, num_rating, inv_grade) %>%
    dplyr::rename(fitch_initial_rating = rating,
                  fitch_initial_num_rating = num_rating,
                  fitch_initial_inv_grade = inv_grade)
  if(dl == TRUE) {
    fr_df %>% dplyr::collect()
  } else {
    fr_df
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






