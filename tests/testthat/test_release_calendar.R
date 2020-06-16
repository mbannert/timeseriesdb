if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader <- connect_to_test_db("dev_reader_public")
  con_writer <- connect_to_test_db("dev_writer")
}


# creating releases -------------------------------------------------------

test_that("defaults", {
  fake_db_call_function = mock('{"hello": "there"}')

  with_mock(
    db_call_function = fake_db_call_function,
    dbWriteTable = mock(),
    {
      db_create_release("con",
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
                    NA,
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
    db_create_release(con_writer,
                      "icanhaz",
                      "probably not",
                      Sys.time(),
                      c("set1"),
                      schema = "tsdb_test"),
    "Only timeseries admin"
  )
})

test_with_fresh_db(con_admin, "create_release returns 'ok'", {
  out <- db_create_release(con_admin,
                           "another",
                           "Thor reference",
                           Sys.time(),
                           c("default"),
                           schema = "tsdb_test")

  expect_equal(out, list(status = "ok"))
})

test_with_fresh_db(con_admin, "creating releases with duplicate id is an error", {
  expect_error(
    db_create_release(con_admin,
                      "ancient_release",
                      "",
                      Sys.time(),
                      c("set1"),
                      schema = "tsdb_test"),
    "already exists"
  )
})

test_with_fresh_db(con_admin, "creating with nonexistent dataset", {
  out <- db_create_release(con_admin,
                    "new_release",
                    "",
                    Sys.time(),
                    c("notaset"),
                    schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "failure",
      reason = "Some datasets do not exist.",
      missing_datasets = "notaset"
    )
  )
})

test_with_fresh_db(con_admin, "create_release db state", {
  db_create_release(con_admin,
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
      # TODO: see #155
      release_date = as.POSIXct("2020-06-15 15:09"),
      reference_year = 2020,
      reference_period = 6,
      reference_frequency = 12,
      stringsAsFactors = FALSE
    )
  )
})

# updating releases -------------------------------------------------------

test_with_fresh_db(con_admin, "update release returns ok", {
  out <- db_update_release(con_admin,
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
  out <- db_update_release(con_admin,
                           "future_release",
                           datasets = c("notaset"),
                           schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "failure",
      reason = "Some datasets do not exist.",
      missing_datasets = "notaset"
    )
  )
})

test_with_fresh_db(con_admin, "updating a release", {
  db_update_release(con_admin,
                    id = "future_release",
                    title = "A new title",
                    note = "Note",
                    release_date = as.Date("2021-01-01"),
                    datasets = c("set2"),
                    reference_year = 2020,
                    reference_period = 1,
                    reference_frequency = 6,
                    schema = "tsdb_test")

  state <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.release_calendar WHERE id = 'future_release'")

  expect_equal(
    state,
    data.frame(
      id = "future_release",
      title = "A new title",
      note = "Note",
      release_date = as.Date("2021-01-01"),
      reference_year = 2020,
      reference_period = 1,
      reference_frequency = 6,
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
  db_update_release(con_admin,
                    id = "future_release",
                    title = "A new title",
                    release_date = as.Date("2031-04-01"),
                    reference_period = 3,
                    schema = "tsdb_test")

  state <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.release_calendar WHERE id = 'future_release'")

  print(state)

  expect_equal(
    state,
    data.frame(
      id = "future_release",
      title = "A new title",
      note = "Apophis is coming",
      release_date = as.Date("2031-04-01"),
      reference_year = 2020,
      reference_period = 3,
      reference_frequency = 12,
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
