



rWRDS.clean_trace_crsp_linking_table = function( wrds, link_df ){

  #Load required packages
  require(tidyverse)
  require(lubridate)
  require(sqldf)
  require(foreign)
  require(zoo)

  #Remove links with with duplicate cusip-permno-startdates by keeping longer lengthed link
  link_df = link_df %>%
    mutate( length_of_link = as.numeric( link_enddt - link_startdt ) ) %>%
    arrange( cusip, permno, link_startdt, -1*length_of_link ) %>%
    distinct( cusip, permno, link_startdt, .keep_all = TRUE )

  #Collapse links with multiple records for a single cusip-permno combination
  if(TRUE){

    #Preliminary: summarize number of number of observations and unique permno matches for each cusip
    temp_df = link_df %>%
      group_by( cusip ) %>%
      mutate( num_links = n(), num_distinct_permno = n_distinct( permno ) ) %>%
      ungroup()

    #Split temporary data.frame() into observations with and without multiple links for single combination
    temp_df_collapse = temp_df %>% filter( num_links > 1, num_distinct_permno == 1 )
    temp_df_no_change = temp_df %>% filter( !(cusip %in% temp_df_collapse$cusip) )

    #Collapse the links in temp_df_collapse into a single link for each cusip-permno
    temp_df_collapse = temp_df_collapse %>%
      group_by( cusip ) %>%
      mutate( link_startdt = min( link_startdt ),
              link_enddt = max( link_enddt ) ) %>%
      ungroup() %>%
      distinct( cusip, .keep_all = TRUE )

    #Re-join the datasets
    link_df = rbind( temp_df_collapse, temp_df_no_change )

  }

  #Adjust beginning link dates for first cusip observation (IMPORTANT: only first cusip observation or get overlaps ) --
  # use offering date (if match with Mergent) or first populated TRACE date otherwise (needs to be done because TRACE starts in 2002 and so links start 2002 )
  if(TRUE){

    #Download the Mergent issue information
    mergent_df = tbl( wrds, in_schema("fisd", "fisd_mergedissue")) %>%
      filter( !is.na(offering_date) ) %>%
      dplyr::select( issue_id, complete_cusip, offering_date, offering_amt, maturity, bond_type ) %>%
      collect() %>%
      distinct( complete_cusip, .keep_all = TRUE )

    #Merge on the Mergent information
    link_df = link_df %>%
      left_join( y = mergent_df,
                 by = c("cusip"="complete_cusip") )

    #Order observations for each CUSIP based on link_startdt and mark first and last observations
    link_df = link_df %>%
      arrange( cusip, link_startdt ) %>%
      group_by( cusip ) %>%
      mutate( row_number_cusip = row_number(),
              number_observations_cusip = n() ) %>%
      ungroup() %>%
      mutate( first_observation_cusip = as.numeric( row_number_cusip == 1 ),
              last_observation_cusip = as.numeric( row_number_cusip == number_observations_cusip ) )

    #Adjust beginning link dates for first observation for each CUSIP
    link_df = link_df %>%
      mutate( revised_link_startdt = as.Date( ifelse( first_observation_cusip == 0,
                                                      link_startdt,
                                                      ifelse( is.na( offering_date ),
                                                              link_startdt,
                                                              ifelse( offering_date < link_startdt,
                                                                      offering_date,
                                                                      link_startdt ) ) ) ) )
  }

  #Adjust link end dates for final observations of each CUSIP (IMPORTANT to only use final CUSIP observation and not cusip-permno observation) --
  #use maturity (if match with Mergent) or final populated TRACE date otherwise
  if(TRUE){

    #Adjust beginning link dates for first observation for each CUSIP
    link_df = link_df %>%
      mutate( revised_link_enddt = as.Date( ifelse( last_observation_cusip == 0,
                                                    link_enddt,
                                                    ifelse( is.na( maturity ),
                                                            link_enddt,
                                                            ifelse( maturity > link_enddt,
                                                                    maturity,
                                                                    link_enddt ) ) ) ) )

  }

  #Collapse overlapping link dates for cusips with multiple permno matches (Work in progress)

  #Keep only subset of variables
  link_df = link_df %>%
    dplyr::select( cusip, permno, permco, issue_id, offering_date, maturity, bond_type,
                   link_startdt, link_enddt, revised_link_startdt, revised_link_enddt )

  #Return the result
  return(link_df)

}


