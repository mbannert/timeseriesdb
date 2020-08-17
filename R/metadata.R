
# meta --------------------------------------------------------------------

#' @export
create_meta <- function(...) {
  UseMethod("create_meta")
}

#' @export
create_meta.list <- function(metadata) {
  if(is.null(names(metadata)) || any(nchar(names(metadata)) == 0)) {
    stop("All fields of metadata objects must be named!")
  }
  class(metadata) <- c("meta", "list")
  metadata
}

#' @export
create_meta.default <- function(...) {
  create_meta.list(list(...))
}

#' @export
as.meta <- function(x) {
  if(is.na(x) || is.null(x)) {
    x
  } else if(!is.list(x)) {
    stop("Only lists can be converted to meta objects!")
  } else {
    create_meta(x)
  }
}

# tsmeta -------------------------------------------------------------


#' @export
create_tsmeta <- function(...) {
  l <- list(...)
  n <- names(l)
  if(is.null(n) | any(nchar(n) == 0)){
    stop("All arguments must be named.")
  }
  as.tsmeta(l)
}

#' @export
as.tsmeta <- function(meta) {
  UseMethod("as.tsmeta")
}

#' @export
as.tsmeta.data.table <- function(meta) {
  if(nrow(meta) > 0) {
    out <- meta[, .(md = list(as.list(.SD))), by = ts_key][, md]
    names(out) <- meta$ts_key
    # Remove NA elements from list
    out <- lapply(out, function(x){x[!is.na(x)]})
    as.tsmeta.list(out)
  } else {
    create_tsmeta()
  }
}

#' @export
as.tsmeta.list <- function(meta) {
  if(get_list_depth(meta) != 2 && length(meta) > 0) {
    stop("A meta list must have exactly depth 2!")
  }
  meta <- lapply(meta, function(x) {
    class(x) <- c("meta", class(x))
    x
  })
  class(meta) <- c("tsmeta", class(meta))
  meta
}

#' @export
as.tsmeta.data.frame <- function(meta) {
  as.tsmeta.list(as.data.table(meta))
}


#' @export
as.tsmeta.tsmeta <- identity


# printers ----------------------------------------------------------------

#' @export
print.meta <- function(x, ...) {
  if(length(x) > 0) {
    atts <- attributes(x)
    cat("Object of class meta\n")
    n <- names(x)
    name_lengths <- sapply(n, nchar)
    max_name_length <- max(name_lengths)
    for(i in n) {
      cat(sprintf("%s%s: %s\n", i, paste(rep(" ", max_name_length - name_lengths[i]), collapse = ""), x[[i]]))
    }
  } else {
    cat("Empty object of class meta\n")
  }
}

#' @export
print.tsmeta <- function(x, ...) {
  if(length(x) > 0) {
    cat("Object of class tsmeta\n")
    print(unclass(x))
  } else {
    cat(sprintf("Empty object of class tsmeta\n"))
  }
}


# functions ---------------------------------------------------------------

# writers -----------------------------------------------------------------

#' Store Time Series Metadata to PostgreSQL
#'
#' to be written: explanation of what is metadata, localized vs. unlocalized
#'
#' @param con RPostgres database connection object.
#' @param metadata object of class tsmeta that contains the metadata to be stored.
#' @param locale character indicating the language of the meta information to be store. We recommend to use ISO country codes to represent languages. Defaults to NULL. When local is set to NULL, metadata are stored without localization. Note that, when localizing meta information by assigning a language, multiple meta information objects can be stored for a single time series.
#' @param on_conflict character allows for either 'update': add new fields and update existing ones or "overwrite": completely replace existing record.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @return status list created from DB status return JSON.
#'
#' @importFrom jsonlite fromJSON toJSON
#' @export
#'
#' @examples
db_meta_store <- function(con,
                                 metadata,
                                 valid_from,
                                 locale = NULL,
                                 on_conflict = "update",
                                 schema = "timeseries") {
  if(!on_conflict %in% c("update", "overwrite")) {
    stop("on_conflict must be one of c(\"update\", \"overwrite\")")
  }

  metadata <- lapply(metadata, toJSON, auto_unbox = TRUE, digits = NA)

  if(!is.null(locale)) {
    md_table <- data.frame(
      ts_key = names(metadata),
      locale = locale,
      metadata = unlist(metadata),
      stringsAsFactors = FALSE
    )


    db_return <- db_with_temp_table(con,
                                    "tmp_md_insert",
                                    md_table,
                                    field.types = c(
                                      ts_key = "text",
                                      locale = "text",
                                      metadata = "jsonb"),
                                      db_call_function(con,
                                                       "md_local_upsert",
                                                       list(as.Date(valid_from), on_conflict),
                                                       schema = schema),
                                     schema = schema)
  } else {
    md_table <- data.frame(
      ts_key = names(metadata),
      metadata = unlist(metadata),
      stringsAsFactors = FALSE
    )


    db_return <- db_with_temp_table(con,
                                   "tmp_md_insert",
                                   md_table,
                                   field.types = c(
                                     ts_key = "text",
                                     metadata = "jsonb"),
                                     db_call_function(con,
                                                      "md_unlocal_upsert",
                                                      list(as.Date(valid_from), on_conflict),
                                                      schema = schema),
                                   schema = schema)
  }

  out <- fromJSON(db_return, simplifyDataFrame = FALSE)

  if(out$status == 'warning') {
    for(w in out$warnings) {
      warning(w$message)
    }
  }

  out
}


# readers -----------------------------------------------------------------

#' Read Time Series Metadata
#'
#' Read meta information given a vector of time series identifiers.
#'
#' @param con RPostgres database connection object.
#' @param ts_keys character vector of time series identifiers  to read metadata for. If regex is TRUE, ts_keys is understood as regular expression pattern as opposed to a vector of keys.
#' @param valid_on Date for which to read the metadata. Defaults to NA reading the most recent version.
#' @param regex boolean should ts_keys allow be interpreted as a regular expression pattern? Defaults to FALSE.
#' @param locale character language identifier of the meta data lookup. If NULL, unlocalized metadata are read.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @return list of tsmeta objects.
#' @importFrom jsonlite fromJSON
#' @export
db_meta_read <- function(con,
                                ts_keys,
                                valid_on = NA,
                                regex = FALSE,
                                locale = NULL,
                                schema = "timeseries") {
  db_return <- db_with_tmp_read(con,
                                ts_keys,
                                regex,
                                {
                                  if(is.null(locale)) {
                                    db_call_function(con,
                                                     "read_metadata_raw",
                                                     list(
                                                       valid_on = as.Date(valid_on)
                                                     ),
                                                     schema = schema)
                                  } else {
                                    db_call_function(con,
                                                     "read_metadata_localized_raw",
                                                     list(
                                                       valid_on = as.Date(valid_on),
                                                       loc = locale
                                                     ),
                                                     schema = schema)
                                  }
                                },
                                schema = schema)

  out <- fromJSON(paste0("[",
                         paste(db_return$metadata, collapse = ","),
                         "]"),
                  simplifyDataFrame = FALSE)
  names(out) <- db_return$ts_key
  out <- as.tsmeta.list(out)

  out
}

#' Title
#'
#' @param con
#' @param collection_name
#' @param owner
#' @param valid_on
#' @param locale
#' @param schema
#'
#' @return
#' @export
#'
#' @examples
db_collection_read_meta <- function(con,
                                    collection_name,
                                    owner,
                                    valid_on = NA,
                                    locale = NULL,
                                    schema = "timeseries") {
  db_return <- if(is.null(locale)) {
    db_call_function(con,
                     "read_collection_metadata_raw",
                     list(
                       p_collection_name = collection_name,
                       p_owner = owner,
                       p_valid_on = as.Date(valid_on)
                     ),
                     schema = schema)
  } else {
    db_call_function(con,
                     "read_collection_metadata_localized_raw",
                     list(
                       p_collection_name = collection_name,
                       p_owner = owner,
                       p_valid_on = as.Date(valid_on),
                       p_loc = locale
                     ),
                     schema = schema)
  }

  out <- fromJSON(paste0("[",
                         paste(db_return$metadata, collapse = ","),
                         "]"),
                  simplifyDataFrame = FALSE)
  names(out) <- db_return$ts_key
  out <- as.tsmeta.list(out)

  out
}



#' Title
#'
#' @param con
#' @param dataset_id
#' @param valid_on
#' @param locale
#' @param schema
#'
#' @return
#' @export
#'
#' @examples
db_dataset_read_meta <- function(con,
                                 dataset_id,
                                 valid_on = NA,
                                 locale = NULL,
                                 schema = "timeseries") {
  db_return <- if(is.null(locale)) {
    db_call_function(con,
                     "read_dataset_metadata_raw",
                     list(
                       p_dataset = dataset_id,
                       p_valid_on = as.Date(valid_on)
                     ),
                     schema = schema)
  } else {
    db_call_function(con,
                     "read_dataset_metadata_localized_raw",
                     list(
                       p_dataset = dataset_id,
                       p_valid_on = as.Date(valid_on),
                       p_loc = locale
                     ),
                     schema = schema)
  }

  out <- fromJSON(paste0("[",
                         paste(db_return$metadata, collapse = ","),
                         "]"),
                  simplifyDataFrame = FALSE)
  names(out) <- db_return$ts_key
  out <- as.tsmeta.list(out)

  out
}


#' Get Latest Validity for Metadata of a Given Time Series
#'
#' Because metadata are only loosely coupled with their respective time series
#' in order to keep metadata records constant over multiple version of
#' time series if the data description does not change, it comes in
#' handy to find out the last time meta information was updated. This function
#' automagickally finds exactly this date.
#'
#' @param con RPostgres connection object.
#' @param ts_keys character vector of time series identifiers.
#' @param regex boolean should ts_keys be interpreted as regular expression patterns? Defaults to FALSE.
#' @param locale character language identifier of the meta data lookup. If NULL, unlocalized metadata are read.
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @return
#' @export
#'
#' @examples
db_meta_get_last_update <- function(con,
                                 ts_keys,
                                 regex = FALSE,
                                 locale = NULL,
                                 schema = "timeseries") {
  out <- db_with_tmp_read(con,
                          ts_keys,
                          regex,
                          {
                            if(is.null(locale)) {
                              out <- db_call_function(con,
                                                      "get_latest_vintages_metadata",
                                                      schema = schema)
                            } else {
                              out <- db_call_function(con,
                                                      "get_latest_vintages_metadata_localized",
                                                      list(locale),
                                                      schema = schema)
                            }
                          },
                          schema = schema)

  as.data.table(out)
}
