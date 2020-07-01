context("access levels")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader <- connect_to_test_db("dev_reader_main")
  con_writer <- connect_to_test_db("dev_writer")
}

# updating access levels --------------------------------------------------

test_with_fresh_db(con_admin, "reader may not change access levels", {
  expect_error(
    db_change_access_level(con_reader, "ts1", "does not matter", schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "setting access level to unregistered one", {
  expect_error(
    db_change_access_level(con_writer, "vts1", "fort_knox", schema = "tsdb_test"),
    "fort_knox is not a valid"
  )
})

test_with_fresh_db(con_admin, "setting access level return status", {
  out <- db_change_access_level(con_writer, "vts1", "tsdb_test_access_public", schema = "tsdb_test")

  expect_equal(out, list(status = "ok"))
})

test_with_fresh_db(con_admin, "setting access level for all vintages", {
  db_change_access_level(con_writer, c("vts1", "vts2"), "tsdb_test_access_restricted", schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main WHERE ts_key ~ 'vts' ORDER BY ts_key, validity")

  expect_equal(
    res$access,
    rep("tsdb_test_access_restricted", 4)
  )
})

test_with_fresh_db(con_admin, "setting access level for specific vintages", {
  db_change_access_level(con_writer,
                         c("vts1", "vts2"),
                         "tsdb_test_access_restricted",
                         validity = "2020-01-01",
                         schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main WHERE ts_key ~ 'vts' ORDER BY ts_key, validity")

  expect_equal(
    res$access,
    c(
      "tsdb_test_access_restricted",
      "tsdb_test_access_public",
      "tsdb_test_access_restricted",
      "tsdb_test_access_main"
    )
  )
})

# test db_list_access_levels --------------------------------------------------
test_with_fresh_db(con_admin, "db_list_access_levels returns data frame with correct names", {
  out <- db_list_access_levels(con_reader, schema = "tsdb_test")

  expected <- data.frame(
    role = c("tsdb_test_access_public",
               "tsdb_test_access_main",
               "tsdb_test_access_restricted"
    ),
    description = c("Publicly available time series",
                    "Non-public time series without license restrictions",
                    "License restricted time series"
    ),
    is_default = c(NA,
                   TRUE,
                   NA
    ),
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})
