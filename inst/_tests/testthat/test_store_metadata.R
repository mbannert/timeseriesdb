if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
  con_reader <- connect_to_test_db("dev_reader_public")
}

meta_fixture_df <- function(ts_key,
                            validity,
                            metadata,
                            locale = NULL) {

  if(is.null(locale)) {
    out <- data.frame(
      ts_key = ts_key,
      validity = validity,
      metadata = metadata,
      stringsAsFactors = FALSE
    )
  } else {
    out <- data.frame(
      ts_key = ts_key,
      validity = validity,
      locale = locale,
      metadata = metadata,
      stringsAsFactors = FALSE
    )
  }

  out$validity <- as.Date(out$validity)
  class(out$metadata) <- "pq_jsonb"

  out
}

test_that("db_store_metadata does not accept invalid on_conflict", {
  expect_error(db_metadata_store("con",
                                    "stuff",
                                    on_conflict = "call the police"),
               "one of")
})

# test storing md localized -----------------------------------------------
context("localized metadata")


# params for db_call_function ---------------------------------------------

test_that("is passes correct args to db_call_function unlocalized", {
  fake_db_call_function = mock()

  with_mock(
    db_with_temp_table = function(con, name, content, field.types, code, schema){eval(code)},
    toJSON = mock("json"),
    fromJSON = mock(list(status = "ok")),
    dbExecute = mock(),
    db_call_function = fake_db_call_function,
    {
      db_metadata_store("con",
                           as.tsmeta.list(
                             list(
                               ts1 = list(
                                 field = "value"
                               )
                             )
                           ),
                           valid_from = "2020-01-01",
                           schema = "schema",
                           on_conflict = "overwrite")

      expect_args(fake_db_call_function,
                  1,
                  "con",
                  "metadata_unlocalized_upsert",
                  list(as.Date("2020-01-01"), "overwrite"),
                  "schema")
    }
  )
})

# return values -----------------------------------------------------------
test_with_fresh_db(con_admin, "reader may not store metadata", {
  expect_error(db_metadata_store(con_reader,
                                    create_tsmeta(ts1 = list(field = "value")),
                                    valid_from = "2020-06-10",
                                    locale = "de",
                                    schema = "tsdb_test"),
               "sufficient privileges")
})

test_with_fresh_db(con_admin, "db_metadata_store localized returns 'ok'", {
  result <- db_metadata_store(con_writer,
                                 create_tsmeta(ts1 = list(field = "value")),
                                 valid_from = "2020-01-01",
                                 locale = "de",
                                 schema = "tsdb_test")

  expect_equal(
    result,
    list(status = "ok")
  )
})

test_with_fresh_db(con_admin, "db_metadata_store localized warns on missing keys", {
  expect_warning(
    db_metadata_store(con_writer,
                         create_tsmeta(tsx = list(field = "value")),
                         valid_from = "2020-01-01",
                         locale = "de",
                         schema = "tsdb_test"),
    "catalog")
})

test_with_fresh_db(con_admin, "db_metadata_store localized missing key warning contents", {
  result <- suppressWarnings(
    db_metadata_store(con_writer,
                         create_tsmeta(tsx = list(field = "value")),
                         valid_from = "2020-01-01",
                         locale = "de",
                         schema = "tsdb_test"))

  expect_equal(
    result,
    list(
      status = "warning",
      warnings = list(
        list(
          message = "Some keys were not found in the catalog",
          offending_keys = "tsx"
        )
      )
    )
  )
})

test_with_fresh_db(con_admin, "storing older vintages is a nono", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-02-01",
                       "de",
                       schema = "tsdb_test")
  expect_warning(db_metadata_store(con_writer,
                                      create_tsmeta(ts1 = list(field = "value")),
                                      "2020-01-01",
                                      "de",
                                      schema = "tsdb_test"),
                 "vintage")
})

test_with_fresh_db(con_admin, "storing older vintages warning contents", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-02-01",
                       locale = "de",
                       schema = "tsdb_test")
  result <- suppressWarnings(db_metadata_store(con_writer,
                                                  create_tsmeta(ts1 = list(field = "value")),
                                                  "2020-01-01",
                                                  locale = "de",
                                                  schema = "tsdb_test"))

  expect_equal(
    result,
    list(
      status = "warning",
      warnings = list(
        list(
          message = "Some keys already have a later vintage",
          offending_keys = "ts1"
        )
      )
    )
  )
})

test_with_fresh_db(con_admin, "invalid keys and invalid vintages", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-02-01",
                       locale = "de",
                       schema = "tsdb_test")
  warnings <- capture_warnings(db_metadata_store(con_writer,
                                                    create_tsmeta(ts1 = list(field = "value"),
                                                                  tsx = list(field = "value")),
                                                    "2020-01-01",
                                                    locale = "de",
                                                    schema = "tsdb_test"))

  expect_length(warnings, 2)
  expect_match(warnings, "vintage", all = FALSE)
  expect_match(warnings, "catalog", all = FALSE)
})

# db state ----------------------------------------------------------------


test_with_fresh_db(con_admin, "db_metadata_store localized stores metadata", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, locale, metadata
                       FROM tsdb_test.metadata_localized
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value"}', "de")
  )
})

test_with_fresh_db(con_admin, "db_metadata_store localized can add fields", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field2 = 3)),
                       valid_from = "2020-01-01",
                       locale = "de",
                       on_conflict = "update",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, locale, metadata
                       FROM tsdb_test.metadata_localized
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value", "field2": 3}', "de")
  )
})

test_with_fresh_db(con_admin, "db_metadata_store localized can override fields", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "new_value")),
                       valid_from = "2020-01-01",
                       locale = "de",
                       on_conflict = "update",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, locale, metadata
                       FROM tsdb_test.metadata_localized
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "new_value"}', "de")
  )
})

test_with_fresh_db(con_admin, "db_metadata_store localized can overwrite records", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(other_field = 23)),
                       valid_from = "2020-01-01",
                       locale = "de",
                       on_conflict = "overwrite",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, locale, metadata
                       FROM tsdb_test.metadata_localized
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"other_field": 23}', "de")
  )
})

test_with_fresh_db(con_admin, "db_metadata_store localized creates vintages", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "vallue")),
                       valid_from = "2020-02-01",
                       locale = "de",
                       schema = "tsdb_test")

  result <-  dbGetQuery(con_admin, "SELECT ts_key, validity, locale, metadata
                        FROM tsdb_test.metadata_localized
                        WHERE ts_key = 'ts1'
                        ORDER BY tsdb_test.metadata_localized.validity")
  expect_equal(
    result,
    meta_fixture_df(c("ts1", "ts1"),
                    c("2020-01-01", "2020-02-01"),
                    c('{"field": "value"}', '{"field": "vallue"}'),
                    c("de", "de"))
  )
})

test_with_fresh_db(con_admin, "db_metadata_store localized can hold different languages for the same key", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       locale = "de",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "valeur")),
                       valid_from = "2020-01-01",
                       locale = "fr",
                       schema = "tsdb_test")

  result <-  dbGetQuery(con_admin, "SELECT ts_key, validity, locale, metadata
                        FROM tsdb_test.metadata_localized
                        WHERE ts_key = 'ts1'
                        ORDER BY locale")
  expect_equal(
    result,
    meta_fixture_df(c("ts1", "ts1"),
                    c("2020-01-01", "2020-01-01"),
                    c('{"field": "value"}', '{"field": "valeur"}'),
                    c("de", "fr"))
  )
})

# test storing md unlocalized, unversioned --------------------------------
context("unlocalized metadata")

# params for db_call_function ---------------------------------------------

test_that("is passes correct args to db_call_function localized", {
  fake_db_call_function = mock()

  with_mock(
    db_with_temp_table = function(con, name, content, field.types, code, schema){eval(code)},
    toJSON = mock("json"),
    fromJSON = mock(list(status = "ok")),
    dbExecute = mock(),
    db_call_function = fake_db_call_function,
    {
      db_metadata_store("con",
                           as.tsmeta.list(
                             list(
                               ts1 = list(
                                 field = "value"
                               )
                             )
                           ),
                           valid_from = "2020-01-01",
                           schema = "schema",
                           locale = "de",
                           on_conflict = "overwrite")

      expect_args(fake_db_call_function,
                  1,
                  "con",
                  "metadata_localized_upsert",
                  list(as.Date("2020-01-01"), "overwrite"),
                  "schema")
    }
  )
})

# returns -----------------------------------------------------------------

test_with_fresh_db(con_admin, "reader may not store unlocaloized metadata", {
  expect_error(db_metadata_store(con_reader,
                                    create_tsmeta(ts1 = list(field = "value")),
                                    valid_from = "2020-06-10",
                                    schema = "tsdb_test"),
               "sufficient privileges")
})

test_with_fresh_db(con_admin, "db_metadata_store unlocalized returns ok", {
  result <- db_metadata_store(con_writer,
                                 create_tsmeta(ts1 = list(field = "value")),
                                 "2020-01-01",
                                 schema = "tsdb_test")

  expect_equal(
    result,
    list(
      status = "ok"))
})

test_with_fresh_db(con_admin, "db_metadata_store unlocalized warns on missing keys", {
  expect_warning(
    db_metadata_store(con_writer,
                         create_tsmeta(tsx = list(field = "value")),
                         "2020-01-01",
                         schema = "tsdb_test"),
    "catalog")
})

# These may be overkill
# testing at least for the structure is valid though as it is public interface
test_with_fresh_db(con_admin, "db_metadata_store unlocalized missing key warning contents", {
  result <- suppressWarnings(
    db_metadata_store(con_writer,
                         create_tsmeta(tsx = list(field = "value")),
                         "2020-01-01",
                         schema = "tsdb_test"))

  expect_equal(
    result,
    list(
      status = "warning",
      warnings = list(
        list(
          message = "Some keys were not found in the catalog",
          offending_keys = "tsx"
        )
      )
    )
  )
})

test_with_fresh_db(con_admin, "storing older vintages is a nono", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-02-01",
                       schema = "tsdb_test")
  expect_warning(db_metadata_store(con_writer,
                                      create_tsmeta(ts1 = list(field = "value")),
                                      "2020-01-01",
                                      schema = "tsdb_test"),
                 "vintage")
})

test_with_fresh_db(con_admin, "storing older vintages warning contents", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-02-01",
                       schema = "tsdb_test")
  result <- suppressWarnings(db_metadata_store(con_writer,
                                                  create_tsmeta(ts1 = list(field = "value")),
                                                  "2020-01-01",
                                                  schema = "tsdb_test"))

  expect_equal(
    result,
    list(
      status = "warning",
      warnings = list(
        list(
          message = "Some keys already have a later vintage",
          offending_keys = "ts1"
        )
      )
    )
  )
})

test_with_fresh_db(con_admin, "invalid keys and invalid vintages", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-02-01",
                       schema = "tsdb_test")
  warnings <- capture_warnings(db_metadata_store(con_writer,
                                                    create_tsmeta(ts1 = list(field = "value"),
                                                                  tsx = list(field = "value")),
                                                    "2020-01-01",
                                                    schema = "tsdb_test"))

  expect_equal(length(warnings), 2)
  expect_match(warnings, "vintage", all = FALSE)
  expect_match(warnings, "catalog", all = FALSE)
})

# db state ----------------------------------------------------------------


test_with_fresh_db(con_admin, "db_metadata_store unlocalized stores metadata", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-01-01",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, metadata
                       FROM tsdb_test.metadata
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value"}')
  )
})

test_with_fresh_db(con_admin, "db_metadata_store unlocalized can add fields", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-01-01",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field2 = 3)),
                       "2020-01-01",
                       on_conflict = "update",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, metadata
                       FROM tsdb_test.metadata
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "value", "field2": 3}')
  )
})

test_with_fresh_db(con_admin, "db_metadata_store unlocalized can override fields", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-01-01",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "new_value")),
                       "2020-01-01",
                       on_conflict = "update",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, metadata
                       FROM tsdb_test.metadata
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"field": "new_value"}')
  )
})

test_with_fresh_db(con_admin, "db_metadata_store unlocalized can override fields", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       "2020-01-01",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(itger_vield = 23)), # It's an inside joke
                       "2020-01-01",
                       on_conflict = "overwrite",
                       schema = "tsdb_test")

  result <- dbGetQuery(con_admin, "SELECT ts_key, validity, metadata
                       FROM tsdb_test.metadata
                       WHERE ts_key = 'ts1'")
  expect_equal(
    result,
    meta_fixture_df("ts1", "2020-01-01", '{"itger_vield": 23}')
  )
})

test_with_fresh_db(con_admin, "db_metadata_store creates vintages", {
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "value")),
                       valid_from = "2020-01-01",
                       schema = "tsdb_test")
  db_metadata_store(con_writer,
                       create_tsmeta(ts1 = list(field = "vallue")),
                       valid_from = "2020-02-01",
                       schema = "tsdb_test")

  result <-  dbGetQuery(con_admin, "SELECT ts_key, validity, metadata
                        FROM tsdb_test.metadata
                        WHERE ts_key = 'ts1'
                        ORDER BY tsdb_test.metadata.validity")
  expect_equal(
    result,
    meta_fixture_df(c("ts1", "ts1"),
                    c("2020-01-01", "2020-02-01"),
                    c('{"field": "value"}', '{"field": "vallue"}'))
  )
})
