
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

# tsmeta.dt ---------------------------------------------------------------

#' @export
tsmeta.dt <- function(...) {
  as.tsmeta.dt(data.table(...))
}

#' @export
as.tsmeta.dt <- function(meta) {
  UseMethod("as.tsmeta.dt")
}

#' @export
as.tsmeta.dt.tsmeta.list <- function(meta_list) {
  if(length(meta_list) > 0) {
    meta_lengths <- sapply(meta_list, length)
    empty_metas <- meta_lengths == 0

    out <- rbindlist(meta_list, fill = TRUE, idcol = TRUE)[.(names(meta_list)), on = .(.id)]
    minlength <- min(meta_lengths[!empty_metas])
    if(ncol(out) != (minlength + 1)) {
      warning("Fill-in occurred, not all fields were present in all meta data items!")
    }
    setnames(out, ".id", "ts_key")
    class(out) <- c("tsmeta.dt", class(out))
    out
  } else {
    tsmeta.dt()
  }
}

#' @export
as.tsmeta.dt.list <- function(meta) {
  as.tsmeta.dt(as.tsmeta.list(meta))
}

#' @export
as.tsmeta.dt.data.frame <- function(meta) {
  meta <- as.data.table(meta)
  if(nrow(meta)) {
    setcolorder(meta,"ts_key")
  }
  class(meta) <- c("tsmeta.dt", class(meta))
  meta
}

#' @export
as.tsmeta.dt.tsmeta.dt <- identity



# tsmeta.list -------------------------------------------------------------


#' @export
tsmeta.list <- function(...) {
  as.tsmeta.list(list(...))
}

#' @export
as.tsmeta.list <- function(meta) {
  UseMethod("as.tsmeta.list")
}

#' @export
as.tsmeta.list.tsmeta.dt <- function(meta) {
  if(nrow(meta) > 0) {
    out <- meta[, .(md = list(as.list(.SD))), by = ts_key][, md]
    names(out) <- meta$ts_key
    # Remove NA elements from list
    out <- lapply(out, function(x){x[!is.na(x)]})
    as.tsmeta.list.list(out)
  } else {
    tsmeta.list()
  }
}

#' @export
as.tsmeta.list.list <- function(meta) {
  if(get_list_depth(meta) != 2 && length(meta) > 0) {
    stop("A meta list must have exactly depth 2!")
  }
  meta <- lapply(meta, function(x) {
    class(x) <- c("meta", class(x))
    x
  })
  class(meta) <- c("tsmeta.list", class(meta))
  meta
}

#' @export
as.tsmeta.list.data.frame <- function(meta) {
  as.tsmeta.list(as.tsmeta.dt(meta))
}


#' @export
as.tsmeta.list.tsmeta.list <- identity


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
print.tsmeta.dt <- function(x, ...) {
  atts <- attributes(x)
  if(nrow(x) > 0) {
    cat(sprintf("A tsmeta.dt object%s\n", ifelse(!is.null(atts$locale), sprintf(" (%s)", atts$locale), "")))
    print(as.data.table(x))
  } else {
    cat(sprintf("An empty tsmeta.dt object\n"))
  }
}

#' @export
print.tsmeta.list <- function(x, ...) {
  atts <- attributes(x)
  if(length(x) > 0) {
    cat(sprintf("A tsmeta.list object%s\n", ifelse(!is.null(atts$locale), sprintf(" (%s)", atts$locale), "")))
    print(unclass(x))
  } else {
    cat(sprintf("An empty tsmeta.list object\n"))
  }
}


# functions ---------------------------------------------------------------

# writers -----------------------------------------------------------------

db_store_ts_metadata <- function(con,
                                 metadata,
                                 valid_from = NULL,
                                 locale = NULL,
                                 schema = "timeseries") {
  UseMethod("db_store_ts_metadata", metadata)
}

#' Title
#'
#' @param con
#' @param metadata
#' @param locale
#' @param schema
#'
#' @return
#'
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom RPostgres dbWriteTable
#' @export
#'
#' @examples
db_store_ts_metadata.tsmeta.list <- function(con,
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

db_store_ts_metadata.tsmeta.dt <- function(con,
                                           metadata,
                                           valid_from = NULL,
                                           locale = NULL,
                                           schema = "timeseries") {
  db_store_ts_metadata.tsmeta.list(con,
                                   as.tsmeta.list(metadata),
                                   valid_from,
                                   locale,
                                   schema)
}


# readers -----------------------------------------------------------------

#' Title
#'
#' @param con
#' @param ts_keys
#' @param valid_on
#' @param regex
#' @param locale
#' @param as.dt
#' @param schema
#'
#' @return
#'
#' @importFrom jsonlite fromJSON
#'
#' @export
#'
#' @examples
db_read_ts_metadata <- function(con,
                                ts_keys,
                                valid_on = NA,
                                regex = FALSE,
                                locale = NULL,
                                as.dt = FALSE,
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

  if(as.dt) {
    out <- fromJSON(paste0("[",
                           paste(db_return$metadata, collapse = ","),
                           "]"),
                    simplifyDataFrame = TRUE)
    out <- cbind(data.table(ts_key = db_return$ts_key),
                 out)
    out <- as.tsmeta.dt(out)

    # if is.null(locale) this will not chante the attrs
    attributes(out) <- c(attributes(out), list(locale = locale))
  } else {
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
  }

  out
}
