connect_to_test_db <- function() {
  dbConnect(Postgres(), "postgres", "localhost", 1111, "", "dev_admin", bigint = "integer")
}

# TODO: see ?dbCanConnect
is_test_db_reachable <- function(){
  tryCatch({
    con <- connect_to_test_db()
    dbDisconnect(con)
    TRUE
  },
  error = function(e) { FALSE }
  )
}

reset_db <- function(con) {
  dbExecute(con, "DELETE FROM timeseries.collect_catalog")
  dbExecute(con, "DELETE FROM timeseries.collections")
  dbExecute(con, "DELETE FROM timeseries.metadata")
  dbExecute(con, "DELETE FROM timeseries.metadata_localized")
  dbExecute(con, "DELETE FROM timeseries.timeseries_main")
  dbExecute(con, "DELETE FROM timeseries.catalog")
  dbExecute(con, "DELETE FROM timeseries.datasets")
}

prepare_db <- function(con,
                       init_datasets = FALSE,
                       init_catalog = FALSE) {

  datasets <- data.frame(
    set_id = c(
      "set1",
      "set2"
    ),
    set_description = c(
      "test set 1",
      "test set 2"
    ),
    set_md = c(
      '{"testno": 1}',
      '{"testno": 2}'
    )
  )

  catalog <- data.frame(
    ts_key = c(
      "ts1",
      "ts2",
      "ts3",
      "ts4",
      "ts5",
      "vts1",
      "vts2"
    ),
    set_id = c(
      "set1",
      "set1",
      "set2",
      "set2",
      "default",
      "default",
      "default"
    )
  )

  vintages <- data.frame(
    id = c(
      "f6aa69c8-41ae-11ea-b77f-2e728ce88125",
      "f6aa6c70-41ae-11ea-b77f-2e728ce88125",
      "f6aa6dba-41ae-11ea-b77f-2e728ce88125",
      "f6aa6ee6-41ae-11ea-b77f-2e728ce88125"
    ),
    ts_key = c(
      "vts1",
      "vts1",
      "vts2",
      "vts2"
    ),
    validity = c(
      "2020-01-01",
      "2020-02-01",
      "2020-01-01",
      "2020-02-01"
    ),
    coverage = c(
      "['2020-01-01', '2020-01-01')",
      "['2020-01-01', '2020-02-01')",
      "['2020-01-01', '2020-01-01')",
      "['2020-01-01', '2020-02-01')"
    ),
    release_date = c(
      "2020-01-01 00:00:00",
      "2020-02-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-02-01 00:00:00"
    ),
    created_by = c(
      "test",
      "test",
      "test",
      "test"
    ),
    created_at = c(
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00"
    ),
    ts_data = c(
      '{"frequency": 12, "time": ["2020-01-01"], "value": [1]}',
      '{"frequency": 12, "time": ["2020-01-01", "2020-02-01"], "value": [1, 2]}',
      '{"frequency": 12, "time": ["2020-01-01"], "value": [1]}',
      '{"frequency": 12, "time": ["2020-01-01", "2020-02-01"], "value": [1, 2]}'
    ),
    access = c(
      "public",
      "public",
      "main",
      "main"
    )
  )

  mdul <- data.frame(
    id = c(
      "1b6277fe-4378-11ea-b77f-2e728ce88125",
      # One journey to Iceland lateur...
      "079eaf0e-4c00-11ea-b77f-2e728ce88125",
      "079eb3aa-4c00-11ea-b77f-2e728ce88125",
      "1b627a92-4378-11ea-b77f-2e728ce88125",
      "1b627bdc-4378-11ea-b77f-2e728ce88125",
      "1b627d12-4378-11ea-b77f-2e728ce88125"
    ),
    ts_key = c(
      "vts1",
      "vts1",
      "vts1",
      "vts2",
      "vts2",
      "vts2"
    ),
    validity = c(
      Sys.Date() - 1,
      Sys.Date(),
      Sys.Date() + 1,
      Sys.Date() - 1,
      Sys.Date(),
      Sys.Date() + 1
    ),
    created_by = c(
      "test",
      "test",
      "test",
      "test",
      "test",
      "test"
    ),
    created_at = c(
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00"
    ),
    metadata = c(
      '{"field": "old value"}',
      '{"field": "value"}',
      '{"field": "new value"}',
      '{"field": "value", "other_field": -3}',
      '{"field": "value", "other_field": 3}',
      '{"field": "value", "other_field": 27}'
    )
  )

  mdl <- data.frame(
    id = c(
      "1b627e48-4378-11ea-b77f-2e728ce88125",
      "1b628370-4378-11ea-b77f-2e728ce88125",
      "1b628578-4378-11ea-b77f-2e728ce88125",
      "1b6286cc-4378-11ea-b77f-2e728ce88125",
      "1b628802-4378-11ea-b77f-2e728ce88125",
      "1b62892e-4378-11ea-b77f-2e728ce88125"
    ),
    ts_key = c(
      "vts1",
      "vts2",
      "vts1",
      "vts2",
      "vts1",
      "vts2"
    ),
    locale = c(
      "en",
      "en",
      "de",
      "de",
      "de",
      "de"
    ),
    validity = c(
      Sys.Date() - 1,
      Sys.Date() - 1,
      Sys.Date() - 1,
      Sys.Date() - 1,
      Sys.Date(),
      Sys.Date()
    ),
    created_by = c(
      "test",
      "test",
      "test",
      "test",
      "test",
      "test"
    ),
    created_at = c(
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00",
      "2020-01-01 00:00:00"
    ),
    metadata = c(
      '{"label": "vintage time series 1"}',
      '{"label": "vintage time series 2"}',
      '{"label": "versionierte zeitreihe 1"}',
      '{"label": "versionierte zeitreihe 2"}',
      '{"label": "versionierte zeitreihe 1, version 2"}',
      '{"label": "versionierte zeitreihe 2, version 2"}'
    )
  )

  collections <- data.table(
    id = c(
      "bc4ad148-516a-11ea-8d77-2e728ce88125",
      "bc4ad40e-516a-11ea-8d77-2e728ce88125",
      "bc4ad558-516a-11ea-8d77-2e728ce88125"
    ),
    name = c(
      "tests first",
      "tests second",
      "some random one"
    ),
    owner = c(
      "test",
      "test",
      "johnny_public"
    ),
    description = c(
      "the first collection ever",
      NA,
      "yo check out these awesome public time series!"
    )
  )

  collect_catalog <- data.table(
    id = c(
      "bc4ad148-516a-11ea-8d77-2e728ce88125",
      "bc4ad148-516a-11ea-8d77-2e728ce88125",
      "bc4ad148-516a-11ea-8d77-2e728ce88125",

      "bc4ad40e-516a-11ea-8d77-2e728ce88125",
      "bc4ad40e-516a-11ea-8d77-2e728ce88125",

      "bc4ad558-516a-11ea-8d77-2e728ce88125",
      "bc4ad558-516a-11ea-8d77-2e728ce88125",
      "bc4ad558-516a-11ea-8d77-2e728ce88125"
    ),
    ts_key = c(
      "ts1",
      "ts2",
      "ts3",

      "ts4",
      "ts5",

      "ts1",
      "ts4",
      "vts1"
    )
  )

  reset_db(con)
  if(init_datasets) {
    dbWriteTable(con,
                 DBI::Id(schema = "timeseries", table = "datasets"),
                 datasets,
                 append = TRUE)

    # TODO: probably rename init_catalog
    if(init_catalog) {
      dbWriteTable(con,
                   DBI::Id(schema = "timeseries", table = "catalog"),
                   catalog,
                   append = TRUE)

      dbWriteTable(con,
                   DBI::Id(schema = "timeseries", table = "timeseries_main"),
                   vintages,
                   append = TRUE)

      dbWriteTable(con,
                   DBI::Id(schema = "timeseries", table  = "metadata"),
                   mdul,
                   append = TRUE)

      dbWriteTable(con,
                   DBI::Id(schema = "timeseries", table = "metadata_localized"),
                   mdl,
                   append = TRUE)

      dbWriteTable(con,
                   DBI::Id(schema = "timeseries", table = "collections"),
                   collections,
                   append = TRUE)

      dbWriteTable(con,
                   DBI::Id(schema = "timeseries", table = "collect_catalog"),
                   collect_catalog,
                   append = TRUE)
    }
  }
}

test_with_fresh_db <- function(con, description, code, hard_reset = FALSE) {
  skip_on_cran()
  skip_if_not(is_test_db_reachable())

  prepare_db(con, !hard_reset, !hard_reset)

  test_that(description, code)
}
