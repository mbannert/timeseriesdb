context("release calendar")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader <- connect_to_test_db("dev_reader_public")
  con_writer <- connect_to_test_db("dev_writer")
}


# creating releases -------------------------------------------------------

test_that("defaults", {
  fake_db_call_function = mock('{"status": "ok"}')

  fake_db_with_tmp_table <- function(con,
                                     name,
                                     content,
                                     field.types,
                                     code,
                                     schema){force(code)}

  with_mock(
    db_call_function = fake_db_call_function,
    dbWriteTable = mock(),
    db_with_temp_table = fake_db_with_tmp_table,
    {
      db_release_create("con",
                        "a_release",
                        "Super Data",
                        as.Date("2020-06-16"),
                        c("set1"),
                        schema = "tsdb_test")

      expect_args(fake_db_call_function,
                  1,
                  "con",
                  "create_release",
                  list(
                    "a_release",
                    "Super Data",
                    NULL,
                    as.Date("2020-06-16"),
                    2020,
                    6,
                    12
                  ),
                  "tsdb_test")
    }
  )
})

test_with_fresh_db(con_admin, "writer may not create releases", {
  expect_error(
    db_release_create(con_writer,
                      "icanhaz",
                      "probably not",
                      Sys.time(),
                      c("set1"),
                      schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "create_release returns 'ok'", {
  out <- db_release_create(con_admin,
                           "another",
                           "Thor reference",
                           Sys.time(),
                           c("default"),
                           schema = "tsdb_test")

  expect_equal(out, list(status = "ok"))
})

test_with_fresh_db(con_admin, "creating releases with duplicate id is an error", {
  expect_error(
    db_release_create(con_admin,
                      "ancient_release",
                      "",
                      Sys.time(),
                      c("set1"),
                      schema = "tsdb_test"),
    "already exists"
  )
})

test_with_fresh_db(con_admin, "creating with nonexistent dataset", {
  expect_error(
    db_release_create(con_admin,
                    "new_release",
                    "",
                    Sys.time(),
                    c("notaset"),
                    schema = "tsdb_test"),
    "notaset")
})

test_with_fresh_db(con_admin, "create_release db state", {
  db_release_create(con_admin,
                    "new_release",
                    "Best Data Ever",
                    as.POSIXct("2020-06-15 13:09"),
                    c("set1"),
                    schema = "tsdb_test")

  state <- dbGetQuery(con_admin, "select * from tsdb_test.release_calendar WHERE id = 'new_release'")

  expect_equal(
    state,
    data.frame(
      id = "new_release",
      title = "Best Data Ever",
      note = NA_character_,
      release_date = as.POSIXct("2020-06-15 13:09"),
      target_year = 2020,
      target_period = 6,
      target_frequency = 12,
      stringsAsFactors = FALSE
    )
  )
})

# updating releases -------------------------------------------------------

test_with_fresh_db(con_admin, "update release returns ok", {
  out <- db_release_update(con_admin,
                           "future_release",
                           "some other title",
                           schema = "tsdb_test")
  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "updating release with nonexistent set", {
  expect_error(db_release_update(con_admin,
                           "future_release",
                           datasets = c("notaset"),
                           schema = "tsdb_test"),
               "notaset")
})

test_with_fresh_db(con_admin, "updating a release", {
  db_release_update(con_admin,
                    id = "future_release",
                    title = "A new title",
                    note = "Note",
                    release_date = as.Date("2021-01-01"),
                    datasets = c("set2"),
                    target_year = 2020,
                    target_period = 1,
                    target_frequency = 6,
                    schema = "tsdb_test")

  state <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.release_calendar WHERE id = 'future_release'")

  expect_equal(
    state,
    data.frame(
      id = "future_release",
      title = "A new title",
      note = "Note",
      release_date = as.Date("2021-01-01"),
      target_year = 2020,
      target_period = 1,
      target_frequency = 6,
      stringsAsFactors = FALSE
    )
  )

  state_sets <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.release_dataset WHERE release_id = 'future_release'")
  expect_equal(
    state_sets,
    data.frame(
      release_id = "future_release",
      set_id = "set2",
      stringsAsFactors = FALSE
    )
  )
})

test_with_fresh_db(con_admin, "partially updating a release", {
  db_release_update(con_admin,
                    id = "future_release",
                    title = "A new title",
                    release_date = as.Date("2031-04-01"),
                    target_period = 3,
                    schema = "tsdb_test")

  state <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.release_calendar WHERE id = 'future_release'")

  expect_equal(
    state,
    data.frame(
      id = "future_release",
      title = "A new title",
      note = "Apophis is coming",
      release_date = as.Date("2031-04-01"),
      target_year = 2020,
      target_period = 3,
      target_frequency = 12,
      stringsAsFactors = FALSE
    )
  )

  state_sets <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.release_dataset WHERE release_id = 'future_release'")
  expect_equal(
    state_sets,
    data.frame(
      release_id = "future_release",
      set_id = "set1",
      stringsAsFactors = FALSE
    )
  )
})

test_with_fresh_db(con_admin, "updating a nonexistent release", {
  expect_error(
    db_release_update(con_admin,
                      id = "phishing_blindly",
                      title = "A new title",
                      release_date = as.Date("2031-04-01"),
                      target_period = 3,
                      schema = "tsdb_test"),
    "phishing_blindly does not exist"
  )
})


# cancel releases ---------------------------------------------------------

test_with_fresh_db(con_admin, "writer may not cancel releases", {
  expect_error(
    db_release_cancel(con_writer, "future_release", schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "release cancel returns status", {
  out <- db_release_cancel(con_admin, "future_release", schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "release cancel db state", {
  db_release_cancel(con_admin, "future_release", schema = "tsdb_test")

  state_calendar <- dbGetQuery(con_admin, "SELECT id
                                           FROM tsdb_test.release_calendar
                                           ORDER BY id")
  expect_equal(
    state_calendar$id,
    c("ancient_release", "combo_release", "last_release")
  )

  state_calendar_sets <- dbGetQuery(con_admin, "SELECT *
                                                FROM tsdb_test.release_dataset
                                                WHERE release_id = 'future_release'")
  expect_equal(nrow(state_calendar_sets), 0)
})

test_with_fresh_db(con_admin, "cancelling a nonexistent release is a-OK", {
  out <- db_release_cancel(con_admin, "life_the_universe_and_everything", schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "cancelling a past release is against the auditors' wishes", {
  expect_error(
    db_release_cancel(con_admin, "ancient_release", schema = "tsdb_test"),
    "has already passed"
  )
})

# list_releases -----------------------------------------------------------

test_with_fresh_db(con_admin, "db_release_list return shape", {
  out <- db_release_list(con_reader, schema = "tsdb_test")

  expect_is(out, "data.frame")
  expect_equal(
    names(out),
    c(
      "id", "title", "note", "release_date", "target_year", "target_period",
      "target_frequency"
    )
  )
})

test_with_fresh_db(con_admin, "db_release_list return value (approx)", {
  out <- db_release_list(con_reader, schema = "tsdb_test")

  expect_equal(
    out$id,
    c("future_release", "combo_release")
  )
})

test_with_fresh_db(con_admin, "db_release_list with past return value (approx)", {
  out <- db_release_list(con_reader, include_past = TRUE, schema = "tsdb_test")

  expect_equal(
    out$id,
    c(
      "ancient_release",
      "last_release",
      "future_release",
      "combo_release"
    )
  )
})


# get next release --------------------------------------------------------

test_with_fresh_db(con_admin, "db_dataset_next_release return shape", {
  out <- db_dataset_next_release(con_reader, "set1", schema = "tsdb_test")

  expect_is(out, "data.frame")
  expect_equal(names(out), c("set_id", "release_id", "release_date"))
})

test_with_fresh_db(con_admin, "db_dataset_next_release", {
  out <- db_dataset_next_release(con_reader, c("set1", "set2"), schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      set_id = c("set1", "set2"),
      release_id = c("future_release", "combo_release"),
      release_date = c(as.POSIXct(Sys.Date() + 1), as.POSIXct(Sys.Date() + 4))
    )
  )
})

test_with_fresh_db(con_admin, "db_dataset_next_release with missing set", {
  out <- db_dataset_next_release(con_reader, c("set1", "bananas"), schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      set_id = c("bananas", "set1"),
      release_id = c(NA, "future_release"),
      release_date = c(as.POSIXct(NA), as.POSIXct(Sys.Date() + 1, origin = "1970-01-01"))
    )
  )
})

# get latest release ------------------------------------------------------

test_with_fresh_db(con_admin, "db_dataset_latest_release return shape", {
  out <- db_dataset_latest_release(con_reader, "set1", schema = "tsdb_test")

  expect_is(out, "data.frame")
  expect_equal(names(out), c("set_id", "release_id", "release_date"))
})

test_with_fresh_db(con_admin, "db_dataset_latest_release return", {
  out <- db_dataset_latest_release(con_reader, "set1", schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      set_id = "set1",
      release_id = "last_release",
      release_date = as.POSIXct(Sys.Date() - 1)
    )
  )
})

test_with_fresh_db(con_admin, "db_get_latest_release with missing set", {
  out <- db_dataset_latest_release(con_reader, c("set1", "bananas"), schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      set_id = c("bananas", "set1"),
      release_id = c(NA, "last_release"),
      release_date = c(as.POSIXct(NA), as.POSIXct(Sys.Date() - 1, origin = "1970-01-01"))
    )
  )
})
