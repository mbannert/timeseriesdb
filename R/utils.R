#'
#' Helper function to convert time series indices of the form 2005.75
#' to a date representation like 2005-07-01.
#' Does not currently support sub-monthly frequencies.
#'
#' @param x numeric A vector of time series time indices (e.g. from stats::time)
#' @param as.string logical If as.string is TRUE the string representation of the
#' Date is returned, otherwise a Date object.
#'
#' @export
index_to_date <- function (x, as.string = FALSE)
{
  # If called as index_to_date(time(a_ts))
  # x is a ts. Unclass it so we can work with the faster basic operators
  x <- c(x)
  
  years <- floor(x + 1/24)
  months <- floor(12*(x - years + 1/24)) + 1
  # No support for days currently
  # datestr <- paste(years, months, 1, sep = "-")
  datestr <- sprintf("%d-%02d-01", years, months)
  
  if(!as.string) {
    return(as.Date(datestr))
  } else {
    return(datestr)
  }
}

#' Convert date-likes to time index
#'
#' @param x The Date or Y-m-d string to convert
#'
#' @return The numeric representation of the date that can be used with ts
#' @export
date_to_index <- function(x) {
  x <- as.character(x)
  components <- as.numeric(unlist(strsplit(x, "-")))
  components[1] + (components[2] - 1)/12
}

#' @export
`[.tslist` <- function(x, i) {
  x <- unclass(x)
  out <- x[i]
  class(out) <- c("tslist", "list")
  out
}

# recursive function to check depth of list. hat tip flodel
# at stackoverflow: http://stackoverflow.com/questions/13432863/determine-level-of-nesting-in-r
#' Determine depth of a list
#'
#' This function recursively checks the depth of a list and returns an integer value of depth
#'
#' @param this an object of class list
#' @details Hat tip to flodel at stackoverflow for suggesting this light weight way analyze depth of a nested list. Further complexity needs to be added to cover the fact that data.frame are lists, too. A more sophisticated recursive function can be found in the gatveys2 package.
#' @references http://stackoverflow.com/questions/13432863/determine-level-of-nesting-in-r
#' @export
get_list_depth <- function(this) {
  ifelse(
    is.list(this),
    ifelse(
      length(this) > 0,
      1L + max(sapply(this, get_list_depth)),
      1L
    ),
    0L
  )
}


#' Create Database Connection
#' 
#' Connects to the PostgreSQL database backend of timeseriesdb. This function
#' is convenience wrapper around DBI's dbConnect. It's less general than the DBI
#' function and only works for PostgreSQL, but it is a little more convenient 
#' because of its defaults / assumptions.
#' 
#' @param dbname character name of the database.
#' @param user character name of the database user. Defaults to the user of the R session. this is often the user for the database, too so you do not have to specify your username explicitly if that is the case.
#' @param host character denoting the hostname. Defaults to localhost.
#' @param passwd character password. Defaults to NULL triggering an R Studio function that
#' asks for your passwords interactively if you are on R Studio. 
#' @param passwd_from_file boolean if set to TRUE the passwd param is interpreted as a file location for a password file such as .pgpass. Make sure to be very restrictive with file permissions if you store a password to a file. 
#' @param line_no integer specify line number of password file that holds the actual password.
#' @param env_pass_name character name of the environment that holds a password. Defaults to NULL. If set, this way of obtaining the password is preferred over all other ways. Other specification will be ignored if this parameter is set. Storing passwords in environment variables can be very handy when working in a docker environment. 
#' @param connection_description character connection description describing the application that connects to the database. This is mainly helpful for DB admins and shows up in the pg_stat_activity table. Defaults to 'timeseriesdb'. Avoid spaces as this is a psql option. 
#' @param port integer defaults to 5432, the PostgreSQL standard port. 
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect
#' @export
db_create_connection <- function(dbname,
                          user = Sys.info()['user'],
                          host = "localhost",
                          passwd = NULL,
                          passwd_from_file = FALSE,
                          line_no = 1,
                          env_pass_name = NULL,
                          connection_description = "timeseriesdb",
                          port = 5432){
  if(!is.null(env_pass_name)){
    passwd <- Sys.getenv(env_pass_name)
    if(passwd == "") {
      stop(sprintf("Could not find password in %s!", env_pass_name))
    }
  } else {
    
    if(is.null(passwd) & !passwd_from_file & commandArgs()[1] == "RStudio"){
      passwd <- .rs.askForPassword("Please enter your database password: ")
    }
    
    if(passwd_from_file){
      passwd <- readLines(passwd)[line_no]
    }
    
  }

  options <- sprintf("--application_name=%s", connection_description)
  
  dbConnect(drv = Postgres(),
            dbname = dbname,
            user = user,
            host = host,
            password = passwd,
            port = port,
            options = options)
}



#' Helper to construct SQL function calls
#'
#' Calls function `schema`.`fname` with the given `args`, returning
#' the result.
#'
#' @param con RPostgres connection object
#' @param fname character Name of the function to be called
#' @param schema character Name of the timeseries schema
#' @param args list of function arguments. A single, unnested list.
#'
#' @return value of `dbGetQuery(con, "SELECT * FROM schema.fname($args)")$fname`
db_call_function <- function(con,
                             fname,
                             args = NULL,
                             schema = "timeseries") {
  query <- sprintf("SELECT * FROM %s.%s(%s)",
                   dbQuoteIdentifier(con, schema),
                   dbQuoteIdentifier(con, fname),
                   ifelse(length(args) > 0,
                          paste(sprintf("$%d", 1:length(args)), collapse = ", "),
                          ""))
  
  res <- dbGetQuery(con, query, args)
  
  if(fname %in% names(res)) {
    res[[fname]] # query returns value (e.g. JSON) -> unwrap the value
  } else {
    res # query returns table -> just return the DF as it comes
  }
}
