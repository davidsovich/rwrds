library(tidyverse)
library(devtools)

# Bond credit rating table ------------------------------------------------------------------------

rating_table = data.frame(
  m_rating = c("Aaa", "Aa1", "Aa2", "Aa3", "A1", "A2", "A3", "Baa1",
               "Baa2", "Baa3", "Ba1", "Ba2", "Ba3", "B1", "B2", "B3",
               "Caa1", "Caa2", "Caa3", "Ca", "C", "D", "NR"),
  s_rating = c("AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+",
               "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-",
               "CCC+", "CCC", "CCC-", "CC", "C", "D", "NR"),
  f_rating = c("AAA", "AA+", "AA", "AA-", "A+", "A", "A-", "BBB+",
               "BBB", "BBB-", "BB+", "BB", "BB-", "B+", "B", "B-",
               "CCC+", "CCC", "CCC-", "CC", "C", "D", "NR"),
  num_rating = c(1:23),
  stringsAsFactors = FALSE)

usethis::use_data(rating_table, overwrite = TRUE)

# Mergent industry table --------------------------------------------------------------------------

mergent_indu_table = data.frame(indu_code = c(10,11,12,13,14,15,16,32, 20, 21, 22, 23, 24, 25,
                                    26, 30, 31, 33, 40, 41, 42, 43, 44, 45, 60, 99 ),
                     indu_group_code = c(rep(1,8), rep(2,7), rep(3,3), rep(4,6), rep(5,2)),
                     mergent_indu_name = c("Manufacturing", "Communications", "OilGas",
                                            "Railroad", "Retail", "Leisure", "Transportation",
                                            "Telephone", "Banking", "Credit", "FinancialServices",
                                            "Insurance", "RealEstate", "SavingsLoan", "Leasing",
                                            "Electric", "Gas", "Water", "ForeignAgency",
                                            "Foreign", "Supranational", "Treasury", "Agency",
                                            "TaxableMuni", "Misc", "Unassigned"),
                     mergent_indu_group_name = c(rep("Industrial",8), rep("Finance",7),
                                                  rep("Utility",3), rep("Government",6),
                                                  rep("Misc",2) ),
                     stringsAsFactors = FALSE )

usethis::use_data(mergent_indu_table, overwrite = TRUE)




