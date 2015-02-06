#' Conveniently Create Connection Object to PostgreSQL based timeseriesdb
#' 
#' Create a conection object while getting user information from the R session. 
#' Also standard db parameters like port and driver are set. Yet flexible information like 
#' host or dbname should be added to Sys.setenv environments. 
#' 
#' @param dbuser character username. Defaults to reading username from Sys.info()
#' @param dbname character name of the database, assumes dbname is stored in TIMESERIESDB_NAME.
#' @param dbhost character host address, asssumes dbhost ist stored in TIMESERIESDB_HOST.
#' @param password character password is used. Defaults to NULL so R Studio's more
#' secure .rs.askForPassword is used.
#' @param dbport integer port number defaults to 5432 for postgres
#' @export
createConObj <- function(dbuser = Sys.info()["user"],
                         dbname = Sys.getenv("TIMESERIESDB_NAME"),
                         dbhost = Sys.getenv("TIMESERIESDB_HOST"),
                         passwd = NULL,
                         dbport = 5432){
                           drv = dbDriver("PostgreSQL")
  if(is.null(passwd)){
    con <- DBI::dbConnect(drv, host = dbhost, dbname = dbname, port = dbport,
                   user = dbuser,
                   password = .rs.askForPassword("Enter password for timeseriesdb"))
  } else {
    con <- DBI::dbConnect(drv, host = dbhost, dbname = dbname, port = dbport,
                          user = dbuser,
                          password = passwd)
  }
  
  con
  
  
}


