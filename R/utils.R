#'
#' Helper function to convert time series indices of the form 2005.75
#' to a date representation like 2005-07-01.
#' Does not currently support sub-monthly frequencies.
#'
#' @param x numeric A vector of time series time indices (e.g. from stats::time)
#' @param as.string logical If as.string is TRUE the string representation of the
#' Date is returned, otherwise a Date object.
index_to_date <- function (x, as.string = FALSE)
{
  if(inherits(x, "Date")) {
    if(as.string) {
      return(as.character(x))
    } else {
      return(x)
    }
  }

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

#' Test if a list has exactly depth 2
#'
#' @param x The list to check
has_depth_2 <- function(x) {
  xx <- unlist(x, recursive = FALSE)
  xxx <- unlist(xx, recursive = FALSE)

  is.list(x) && is.list(xx) && !is.list(xxx)
}


# Mocking functions from base does not work :shrug:
readPasswordFile <- readLines


#' Create Database Connection
#'
#' Connects to the PostgreSQL database backend of timeseriesdb. This function
#' is convenience wrapper around DBI's dbConnect. It's less general than the DBI
#' function and only works for PostgreSQL, but it is a little more convenient
#' because of its defaults / assumptions.
#'
#' @param dbname character name of the database.
#' @param user character name of the database user. Defaults to the user of the R session.
#'             this is often the user for the database, too so you do not have to specify
#'             your username explicitly if that is the case.
#' @param host character denoting the hostname. Defaults to localhost.
#' @param passwd character password, file or environment name. Defaults to NULL triggering an R Studio function that
#' asks for your passwords interactively if you are on R Studio. Make sure to adapt the boolean params correspondingly.
#' @param passwd_from_file boolean if set to TRUE the passwd param is interpreted as a file
#'                         location for a password file such as .pgpass. Make sure to be very
#'                         restrictive with file permissions if you store a password to a file.
#' @param line_no integer specify line number of password file that holds the actual password.
#' @param connection_description character connection description describing the application
#'                               that connects to the database. This is mainly helpful for
#'                               DB admins and shows up in the pg_stat_activity table.
#'                               Defaults to 'timeseriesdb'. Avoid spaces as this is a psql option.
#' @param port integer defaults to 5432, the PostgreSQL standard port.
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect
#' @export
db_create_connection <- function(dbname,
                          user = Sys.info()[['user']],
                          host = "localhost",
                          passwd = NULL,
                          passwd_from_file = FALSE,
                          line_no = 1,
                          passwd_from_env = FALSE,
                          connection_description = "timeseriesdb",
                          port = 5432){
  if(passwd_from_env){
    env_name <- passwd
    passwd <- Sys.getenv(env_name)
    if(passwd == "") {
      stop(sprintf("Could not find password in %s!", env_name))
    }
  } else if(passwd_from_file) {
    if(!file.exists(passwd)) {
      stop("Password file does not exist.")
    }

    pwdlines <- readPasswordFile(passwd)
    nlines <- length(pwdlines)

    if(nlines < line_no) {
      stop(sprintf("line_no too great (password file only has %d lines)", nlines))
    }

    passwd <- pwdlines[line_no]
  } else if(is.null(passwd)) {
    if(commandArgs()[1] == "RStudio") {
      passwd <- .rs.askForPassword("Please enter your database password: ")
    } else {
      stop("Unable to obtain password. Please use passwd_from_file or pass the password directly via passwd.")
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
#' Args may be named to enable postgres to decide which candidate to choose in case
#' of overloaded functions.
#' If any args are named, all of them must be.
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
  args_names = names(args)
  if(!is.null(args_names) && any(nchar(args_names) == 0)) {
    stop("Either all args must be named or none!")
  }

  args_pattern <- ""
  if(!is.null(args)) {
    # dbGetQuery does not like parameters to be NULL so we substitute NA here
    # which the db will treat as null anyway
    args[sapply(args, is.null)] <- NA

    args_pattern <- sprintf("$%d", 1:length(args))

    if(!is.null(args_names)) {
      args_pattern <- paste(
        sprintf("%s :=", args_names),
        args_pattern
      )
    }

    args_pattern <- paste(args_pattern, collapse = ", ")

  }

  query <- sprintf("SELECT * FROM %s.%s(%s)",
                   dbQuoteIdentifier(con, schema),
                   dbQuoteIdentifier(con, fname),
                   args_pattern)

  res <- tryCatch(
    dbGetQuery(con, query, unname(args)),
    error = function(e) {
      if(grepl("permission denied for function", e)) {
        stop("You do not have sufficient privileges to perform this action.")
      } else {
        stop(e)
      }
    })

  if(fname %in% names(res)) {
    res[[fname]] # query returns value (e.g. JSON) -> unwrap the value
  } else {
    if(class(res) == "data.frame") {
      as.data.table(res) # query returns table -> just return as data.table
    } else {
      res
    }
  }
}


#' GRANT all rights on a (temp) table to schema admin
#'
#' The SECURITY DEFINER functions do not have access to tables that
#' are stored via dbWriteTable. Usage rights on these tables must
#' be granted for them to be usable inside the db functions
#'
#' @param con RPostgres connection
#' @param table which table to grant rights on
#' @param schema name of the timeseries schema being worked with
#'
#' @importFrom DBI dbExecute dbQuoteIdentifier
db_grant_to_admin <- function(con,
                              table,
                              schema = "timeseries") {
  dbExecute(con,
            sprintf("GRANT SELECT, UPDATE, INSERT, DELETE ON %s TO %s",
                    dbQuoteIdentifier(con, table),
                    dbQuoteIdentifier(con, sprintf("%s_admin", schema))))
}
