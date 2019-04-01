

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
    #dplyr does not support distinct(, .keep_all = TRUE) for PostgreSQL
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
    #dplyr does not support distinct(, .keep_all = TRUE) for PostgreSQL
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
