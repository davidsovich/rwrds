#' Connect to the WRDS cloud
#'
#' \code{wrds_connect} creates a remote connection to the WRDS cloud.
#'
#' Note: WRDS cloud runs PostgreSQL.
#'
#' @export
#'
#' @param username Character. WRDS username.
#' @param password Character. WRDS password.
#' @examples
#' wrds = wrds_connect(username = Sys.getenv("WRDS_NAME"), password = Sys.getenv("WRDS_PASS"))
wrds_connect = function(username, password) {
  wrds = DBI::dbConnect(RPostgres::Postgres(),
                        host = 'wrds-pgdata.wharton.upenn.edu',
                        port = 9737,
                        dbname='wrds',
                        user = username,
                        password = password,
                        sslmode='require')
  return(wrds)
}

#' Disconnect from WRDS cloud
#'
#' \code{wrds_disconnect} closes a remote connection to the WRDS cloud.
#'
#' Note: WRDS cloud runs PostgreSQL.
#'
#' @export
#'
#' @param wrds WRDS connection.
#' @examples
#' wrds = wrds_connect(username = Sys.getenv("WRDS_NAME"), password = Sys.getenv("WRDS_PASS"))
#' wrds_disconnect(wrds = wrds)
wrds_disconnect = function(wrds) {
  RPostgres::dbDisconnect(wrds)
}

#' List available schemas in WRDS
#'
#' \code{wrds_schema_list} downloads a list of the available schemas in the WRDS cloud.
#'
#' Schemas represent different data vendors in WRDS. Schemas can be thought of as separate
#' databases that contain their own set of tables.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' schema_list = wrds_schema_list(wrds = wrds)
wrds_schema_list = function(wrds) {
  DBI::dbGetQuery (wrds, paste("SELECT DISTINCT table_schema",
                               "FROM information_schema.tables",
                               "WHERE table_type = 'VIEW' OR table_type = 'FOREIGN TABLE'",
                               "ORDER BY table_schema"))
}

#' List the tables in a WRDS schema.
#'
#' \code{wrds_table_list} lists the tables in a WRDS schema
#'
#' Each WRDS schema contains its own set of tables. Tables are queried directly in the package.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param schema Character. WRDS schema from \code{wrds_schema_list}.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' table_list = wrds_table_list(wrds = wrds, schema = "compa")
wrds_table_list = function(wrds, schema) {
  DBI::dbGetQuery (wrds, paste("SELECT DISTINCT table_name",
                               "FROM information_schema.columns",
                               "WHERE table_schema = ", paste0("'", schema, "'"),
                               "ORDER BY table_name"))
}

#' List the variables in a WRDS table
#'
#' \code{wrds_variable_list} lists the columns (variables) in a WRDS table.
#'
#' NA.
#'
#' @export
#'
#' @param wrds WRDS connection object from \code{wrds_connect} function.
#' @param schema Character. WRDS schema from \code{wrds_schema_list}.
#' @param table Character. WRDS table from \code{wrds_table_list}.
#' @examples
#' wrds = wrds_connect(username = "testing", password = "123456")
#' variable_list = wrds_variable_list(wrds = wrds, schema = "compa", table = "funda")
wrds_variable_list = function(wrds, schema, table) {
  DBI::dbGetQuery (wrds, paste("SELECT column_name, udt_name",
                               "FROM information_schema.columns",
                               "WHERE table_schema = ", paste0("'", schema, "'"),
                               " AND table_name = ", paste0("'", table, "'"),
                               "ORDER BY column_name"))
}
