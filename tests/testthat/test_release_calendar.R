if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader <- connect_to_test_db("dev_reader_public")
  con_writer <- connect_to_test_db("dev_writer")
}

test_that("defaults", {
  fake_db_call_function = mock()

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

test_with_fresh_db(con_admin, "create_release returns the id", {
  out <- db_create_release(con_admin,
                           "another",
                           "Thor reference",
                           Sys.time(),
                           c("default"),
                           schema = "tsdb_test")

  expect_equal(out, "another")
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

# TODO: Give this one a json return w/ info
test_with_fresh_db(con_admin, "creating with nonexistent dataset", {
  db_create_release(con_admin,
                    "new_relese",
                    "",
                    Sys.time(),
                    c("notaset"),
                    schema = "tsdb_test")
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
