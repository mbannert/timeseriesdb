# And now for something completely tricky... *liberty bell*

context("db_ts_read")

tsl_state_0 <- list(
  rts1 = ts(rep(1.9, 10), 2019, frequency = 4)
)
class(tsl_state_0) <- c("tslist", "list")

tsl_state_1 <- list(
  rts1 = ts(rep(2, 10), 2019, frequency = 4)
)
class(tsl_state_1) <- c("tslist", "list")

tsl_state_2 <- list(
  rts1 = ts(rep(2.1, 10), 2019, frequency = 4)
)
class(tsl_state_2) <- c("tslist", "list")

tsl_state_2_v2 <- list(
  rts1 = ts(rep(2.1415926, 10), 2019, frequency = 4)
)
class(tsl_state_2_v2) <- c("tslist", "list")

tsl_pblc <- list(
  rtsp = ts(rep(3, 10), 2019, frequency = 4)
)
class(tsl_pblc) <- c("tslist", "list")

tslx <- list(
  rtsx = xts(seq(4), order.by = seq(as.Date("2020-01-01"), length.out = 4, by = "1 days"))
)
class(tslx) <- c("tslist", "list")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader_public <- connect_to_test_db("dev_reader_public")
  con_reader_main <- connect_to_test_db("dev_reader_main")
}

# read ts -----------------------------------------------------------------

test_with_fresh_db(con_admin, "public reader may not read main series", {
  tsl_read <- db_ts_read(con_reader_public, "rts1", schema = "tsdb_test")
  expect_length(tsl_read, 0)
})

test_with_fresh_db(con_admin, "series with no access get skipped", {
  tsl_read <- db_ts_read(con_reader_public, c("rts1", "rtsp"), schema = "tsdb_test")
  expect_equal(tsl_read, tsl_pblc)
})

test_with_fresh_db(con_admin, "by default it reads the most recent valid vintage", {
  tsl_read <- db_ts_read(con_reader_main, "rts1", schema = "tsdb_test")
  expect_equal(tsl_read, tsl_state_2)
})

test_with_fresh_db(con_admin, "by default it reads the most recent valid vintage but with respecting rls date", {
  tsl_read <- db_ts_read(con_reader_main,
                         "rts1",
                         respect_release_date = TRUE,
                         schema = "tsdb_test")
  expect_equal(tsl_read, tsl_state_1)
})

test_with_fresh_db(con_admin, "reading desired vintages works", {
  tsl_read_1 <- db_ts_read(con_reader_main,
                           "rts1",
                           valid_on = Sys.Date() - 4,
                           schema = "tsdb_test")
  expect_equal(tsl_read_1, tsl_state_0)

  tsl_read_2 <- db_ts_read(con_reader_main,
                           "rts1",
                           valid_on = Sys.Date() - 2,
                           schema = "tsdb_test")
  expect_equal(tsl_read_2, tsl_state_1)
})

test_with_fresh_db(con_admin, "reading vintages, respecting release date", {
  tsl_read <- db_ts_read(con_reader_main,
                         "rts1",
                         valid_on = Sys.Date() - 2,
                         respect_release_date = TRUE,
                         schema = "tsdb_test")
  expect_equal(tsl_read, tsl_state_1)
})

test_with_fresh_db(con_admin, "reading via regex works", {
  tsl_read <- db_ts_read(con_reader_main,
                         "^rts",
                         regex = TRUE,
                         schema = "tsdb_test")
  expect_setequal(names(tsl_read), c("rts1", "rtsp", "rtsx"))
})

test_with_fresh_db(con_admin, "reading an xts", {
  tsl_read <- db_ts_read(con_reader_main,
                         "rtsx",
                         schema = "tsdb_test")
  expect_equal(tsl_read, tslx)
})

test_with_fresh_db(con_admin, "reading with duplicate keys warns", {
  expect_warning(db_ts_read(con_reader_main, c("rts1", "rts1"), schema = "tsdb_test"))
})

test_with_fresh_db(con_admin, "reading with duplicate keys returns proper ts list", {
  tsl_read <- suppressWarnings(
    db_ts_read(con_reader_main,
                         c("rts1", "rts1"),
                         schema = "tsdb_test"))

  expect_equal(tsl_read, tsl_state_2)
})

# yeh yeh we said we weren't going to test pure sql stuff...
test_with_fresh_db(con_admin, "SQL-only test for array version", {
  out <- dbGetQuery(con_reader_main, "SELECT * FROM tsdb_test.ts_read_raw('{rts1,rtsp}'::TEXT[])")

  expect_equal(
    out$ts_key,
    c("rts1", "rtsp")
  )

  # lazy test is lazy
  expect_match(
    out$ts_data[[1]],
    "2.1"
  )

  expect_match(
    out$ts_data[[2]],
    "3"
  )
})

# reading datasets --------------------------------------------------------
context("reading datasets")

test_with_fresh_db(con_admin, "reading a whole dataset works", {
  tsl_read <- db_dataset_read_ts(con_reader_main,
                                 "set_read",
                                 schema = "tsdb_test")

  exp <- structure(c(tsl_state_2, tsl_pblc), class = c("tslist", "list"))

  expect_equal(tsl_read, exp[names(tsl_read)])
})

test_with_fresh_db(con_admin, "reading whole dataset, reapecting release date",  {
  tsl_read <- db_dataset_read_ts(con_reader_main,
                                 "set_read",
                                 respect_release_date = TRUE,
                                 schema = "tsdb_test")

  exp <- structure(c(tsl_state_1, tsl_pblc), class = c("tslist", "list"))

  expect_equal(tsl_read, exp[names(tsl_read)]) # Order does not matter (at the moment)
})

test_with_fresh_db(con_admin, "reading whole dataset, leaving out prohibited series", {
  tsl_read <- db_dataset_read_ts(con_reader_public,
                                 "set_read",
                                 schema = "tsdb_test")

  expect_equal(tsl_read, tsl_pblc)
})

test_with_fresh_db(con_admin, "reading older vintages of dataset", {
  tsl_read <- db_dataset_read_ts(con_reader_main,
                                 "set_read",
                                 valid_on = Sys.Date() - 4,
                                 schema = "tsdb_test")

  expect_equal(tsl_read, tsl_state_0)
})

test_with_fresh_db(con_admin, "reading nonexistend set", {
  tsl_read <- db_dataset_read_ts(con_reader_main,
                                 "notaset",
                                 schema = "tsdb_test")

  expect_is(tsl_read, "tslist")
  expect_length(tsl_read, 0)
})

test_with_fresh_db(con_admin, "reading multiple sets", {
  tsl_read <- db_dataset_read_ts(con_reader_main,
                                 c("set_read", "default"),
                                 schema = "tsdb_test")
  expect_setequal(names(tsl_read), c("rts1", "rtsp", "rtsx", "vts1", "vts2"))
})

test_with_fresh_db(con_admin, "SQL-only test for array version of read dataset", {
  out <- dbGetQuery(con_reader_main, "SELECT * FROM tsdb_test.ts_read_dataset_raw('{set_read}'::TEXT[]) order by ts_key")

  expect_equal(
    out$ts_key,
    c(
      "rts1",
      "rtsp"
    )
  )

  expect_match(
    out$ts_data[[1]],
    "2.1"
  )

  expect_match(
    out$ts_data[[2]],
    "3"
  )
})



test_with_fresh_db(con_admin, "SQL-only test for long format ts read", {
  out <- dbGetQuery(con_reader_main, "SELECT * FROM tsdb_test.ts_read_long('{rts1,rts2}'::TEXT[])")

  expect_equal(
    out,
    structure(list(ts_key = c("rts1", "rts1", "rts1", "rts1", "rts1",
                              "rts1", "rts1", "rts1", "rts1", "rts1"),
                   date = c("\"2019-01-01\"",                                                                                "\"2019-04-01\"", "\"2019-07-01\"", "\"2019-10-01\"", "\"2020-01-01\"",
                            "\"2020-04-01\"", "\"2020-07-01\"", "\"2020-10-01\"", "\"2021-01-01\"",
                            "\"2021-04-01\""), value = c(2.1, 2.1, 2.1, 2.1, 2.1, 2.1, 2.1,
                                                         2.1, 2.1, 2.1)),
              class = "data.frame", row.names = c(NA, -10L
                                                         ))
  )


})





# reading collections --------------------------------------------------------
context("reading collections")

test_with_fresh_db(con_admin, "reading a collection works", {
  tsl_read <- db_collection_read_ts(con_reader_main,
                                    "readtest",
                                    "test",
                                    schema = "tsdb_test")

  exp <- structure(c(tsl_state_2, tsl_pblc), class = c("tslist", "list"))

  expect_equal(tsl_read, exp)
})

test_with_fresh_db(con_admin, "reading collection, respecting release date",  {
  tsl_read <- db_collection_read_ts(con_reader_main,
                                    "readtest",
                                    "test",
                                    respect_release_date = TRUE,
                                    schema = "tsdb_test")

  exp <- structure(c(tsl_state_1, tsl_pblc), class = c("tslist", "list"))

  expect_equal(tsl_read, exp)
})

test_with_fresh_db(con_admin, "reading collection, leaving out prohibited series", {
  tsl_read <- db_collection_read_ts(con_reader_public,
                                    "readtest",
                                    "test",
                                    schema = "tsdb_test")

  expect_equal(tsl_read, tsl_pblc)
})

test_with_fresh_db(con_admin, "reading older vintages of collection", {
  tsl_read <- db_collection_read_ts(con_reader_main,
                                    "readtest",
                                    "test",
                                    valid_on = Sys.Date() - 4,
                                    schema = "tsdb_test")

  expect_equal(tsl_read, tsl_state_0)
})

test_with_fresh_db(con_admin, "reading nonexistend collection", {
  tsl_read <- db_collection_read_ts(con_reader_main,
                                    "readtest",
                                    "mineallmine",
                                    schema = "tsdb_test")

  expect_is(tsl_read, "tslist")
  expect_length(tsl_read, 0)
})



# get latest update -------------------------------------------------------

test_with_fresh_db(con_admin, "getting latest update of ts", {
  out <- db_ts_get_last_update(con_reader_public, c("vts1", "vts2"), schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      ts_key = c("vts1", "vts2"),
      updated = c(
        as.POSIXct("2020-01-02 01:00:00"),
        as.POSIXct("2020-01-02 01:00:00")
      )
    )
  )
})


# test db_ts_find_keys --------------------------------------------------
test_with_fresh_db(con_admin, "db_ts_find_keys returns data frame with correct ts_keys", {
  out <- db_ts_find_keys(con_reader_public, "rts" , schema = "tsdb_test")

  expected <- data.table(
    ts_key = c("rts1",
               "rtsp",
               "rtsx"
    )
  )

  expect_equal(out, expected)
})

