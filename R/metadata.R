
# meta --------------------------------------------------------------------

#' @export
meta <- function(...) {
  UseMethod("meta")
}

#' @export
meta.list <- function(metadata) {
  if(is.null(names(metadata)) || any(nchar(names(metadata)) == 0)) {
    stop("All fields of metadata objects must be named!")
  }
  class(metadata) <- c("meta", "list")
  metadata
}

#' @export
meta.default <- function(...) {
  meta.list(list(...))
}

#' @export
as.meta <- function(x) {
  if(is.na(x) || is.null(x)) {
    x
  } else if(!is.list(x)) {
    stop("Only lists can be converted to meta objects!")
  } else {
    meta(x)
  }
}

# tsmeta -------------------------------------------------------------


#' @export
# TODO: check ... (must be named, maybe cover list case)
create_tsmeta <- function(...) {
  as.tsmeta(list(...))
}

#' @export
as.tsmeta <- function(meta) {
  UseMethod("as.tsmeta")
}

#' @export
# TODO: DO these need to be exported or not? still confused...
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
    cat(sprintf("Metadata%s\n", ifelse(!is.null(atts$locale), sprintf(" (%s)", atts$locale), "")))
    n <- names(x)
    name_lengths <- sapply(n, nchar)
    max_name_length <- max(name_lengths)
    for(i in n) {
      cat(sprintf("%s%s: %s\n", i, paste(rep(" ", max_name_length - name_lengths[i]), collapse = ""), x[[i]]))
    }
  } else {
    cat("No metadata\n")
  }
}

#' @export
print.tsmeta <- function(x, ...) {
  atts <- attributes(x)
  if(length(x) > 0) {
    cat(sprintf("A tsmeta object%s\n", ifelse(!is.null(atts$locale), sprintf(" (%s)", atts$locale), "")))
    print(unclass(x))
  } else {
    cat(sprintf("An empty tsmeta object\n"))
  }
}


# functions ---------------------------------------------------------------

# writers -----------------------------------------------------------------

#' Store timeseries metadata
#'
#' to be written: explanation of what is metadata, localized vs. unlocalized
#'
#' @param con RPostgres database connection
#' @param metadata tsmeta The metadata to be stored
#' @param locale character What language to store the data as. If locale is NULL (default)
#' the metadata is stored without associated language information
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @return
#'
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom RPostgres dbWriteTable
#' @export
#'
#' @examples
db_store_ts_metadata <- function(con,
                                 metadata,
                                 valid_from,
                                 locale = NULL,
                                 schema = "timeseries") {
  metadata <- lapply(metadata, toJSON, auto_unbox = TRUE, digits = NA)

  if(!is.null(locale)) {
    md_table <- data.frame(
      ts_key = names(metadata),
      locale = locale,
      metadata = unlist(metadata),
      stringsAsFactors = FALSE
    )

    dbWriteTable(con,
                 "tmp_md_insert",
                 md_table,
                 temporary = TRUE,
                 overwrite = TRUE,
                 field.types = c(
                   ts_key = "text",
                   locale = "text",
                   metadata = "jsonb"))

    db_return <- db_call_function(con,
                                  "md_local_upsert",
                                  list(as.Date(valid_from)),
                                  schema = schema)
  } else {
    md_table <- data.frame(
      ts_key = names(metadata),
      metadata = unlist(metadata),
      stringsAsFactors = FALSE
    )

    dbWriteTable(con,
                 "tmp_md_insert",
                 md_table,
                 temporary = TRUE,
                 overwrite = TRUE,
                 field.types = c(
                   ts_key = "text",
                   metadata = "jsonb"))

    db_return <- db_call_function(con,
                                  "md_unlocal_upsert",
                                  list(as.Date(valid_from)),
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

#' Read time series metadata
#'
#'
#'
#' @param con RPostgres database connection
#' @param ts_keys Character vector of ts keys to read metadata for. If regex is TRUE, ts_keys is used as a pattern.
#' @param valid_on Date for which to read the metadata. If NA the most recent version is read.
#' @param regex Automatically find time series with keys matching the pattern in ts_keys
#' @param locale What language to read metadata for. If NULL, unlocalized metadata is read.
#' @param as.dt Should a tsmeta.dt be returned? By default db_read_ts_metadata return a tsmeta.list
#' @param schema character name of the schema. Defaults to 'timeseries'.
#'
#' @importFrom jsonlite fromJSON
#'
#' @export
db_read_ts_metadata <- function(con,
                                ts_keys,
                                valid_on = NA,
                                regex = FALSE,
                                locale = NULL,
                                schema = "timeseries") {
  db_tmp_read(
    con,
    ts_keys,
    regex,
    schema
  )

  # TODO: should missing ts have NA values or just be missing?
  # TODO: chunking?

  if(is.null(locale)) {
    db_return <- db_call_function(con,
                                  "read_metadata_raw",
                                  list(as.Date(valid_on)),
                                  schema = schema)
  } else {
    db_return <- db_call_function(con,
                                  "read_metadata_localized_raw",
                                  list(as.Date(valid_on), locale),
                                  schema = schema)
  }

  out <- fromJSON(paste0("[",
                         paste(db_return$metadata, collapse = ","),
                         "]"),
                  simplifyDataFrame = FALSE)
  names(out) <- db_return$ts_key
  out <- as.tsmeta.list(out)

  if(!is.null(locale)) {
    attributes(out) <- c(attributes(out), list(locale = locale))

    for(i in seq_along(out)) {
      attributes(out[[i]]) <- c(attributes(out[[i]]), list(locale = locale))
    }
  }

  out
}
