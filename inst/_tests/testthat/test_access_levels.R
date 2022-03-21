if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader <- connect_to_test_db("dev_reader_main")
  con_writer <- connect_to_test_db("dev_writer")
}

# updating access levels --------------------------------------------------

test_with_fresh_db(con_admin, "reader may not change access levels", {
  expect_error(
    db_ts_change_access(con_reader, "ts1", "does not matter", schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "setting access level to unregistered one", {
  expect_error(
    db_ts_change_access(con_writer, "vts1", "fort_knox", schema = "tsdb_test"),
    "fort_knox is not a valid"
  )
})

test_with_fresh_db(con_admin, "setting access level return status", {
  out <- db_ts_change_access(con_writer, "vts1", "tsdb_test_access_public", schema = "tsdb_test")

  expect_equal(out, list(status = "ok"))
})

test_with_fresh_db(con_admin, "setting access level for all vintages", {
  db_ts_change_access(con_writer, c("vts1", "vts2"), "tsdb_test_access_restricted", schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main WHERE ts_key ~ 'vts' ORDER BY ts_key, validity")

  expect_equal(
    res$access,
    rep("tsdb_test_access_restricted", 4)
  )
})

test_with_fresh_db(con_admin, "setting access level for specific vintages", {
  db_ts_change_access(con_writer,
                         c("vts1", "vts2"),
                         "tsdb_test_access_restricted",
                         valid_from = "2020-01-01",
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


# updating access level on dataset -----------------------------------------

test_with_fresh_db(con_admin, "reader may not set access level for dataset", {
  expect_error(
    db_dataset_change_access(con_reader, "ts1", "does not matter", schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "setting dataset access level to unregistered one", {
  expect_error(
    db_dataset_change_access(con_writer, "set_read", "fort_knox", schema = "tsdb_test"),
    "fort_knox is not a valid"
  )
})

test_with_fresh_db(con_admin, "setting dataset access level with nonexisting set", {
  expect_warning(
    db_dataset_change_access(con_writer, "set_readd", "fort_knox", schema = "tsdb_test"),
    "Dataset set_readd"
  )
})

test_with_fresh_db(con_admin, "setting dataset access level return status", {
  out <- db_dataset_change_access(con_writer, "set_read", "tsdb_test_access_public", schema = "tsdb_test")

  expect_equal(out, list(status = "ok"))
})

test_with_fresh_db(con_admin, "setting dataset access level for all vintages", {
  db_dataset_change_access(con_writer, "set_read", "tsdb_test_access_restricted", schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main WHERE ts_key = 'rts1' ORDER BY ts_key, validity")

  expect_equal(
    res$access,
    rep("tsdb_test_access_restricted", 4)
  )
})

test_with_fresh_db(con_admin, "setting dataset access level for specific vintages", {
  db_dataset_change_access(con_writer,
                         "set_read",
                         "tsdb_test_access_restricted",
                         valid_from = Sys.Date() - 1,
                         schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main WHERE ts_key = 'rts1' ORDER BY ts_key, validity")

  expect_equal(
    res$access,
    c(
      "tsdb_test_access_main",
      "tsdb_test_access_main",
      "tsdb_test_access_restricted",
      "tsdb_test_access_main"
    )
  )
})

# test db_access_level_list --------------------------------------------------
test_with_fresh_db(con_admin, "db_access_level_list returns data frame with correct names", {
  out <- db_access_level_list(con_reader, schema = "tsdb_test")

  expected <- data.table(
    role = c("tsdb_test_access_main",
             "tsdb_test_access_public",
             "tsdb_test_access_restricted"
    ),
    description = c("Non-public time series without license restrictions",
                    "Publicly available time series",
                    "License restricted time series"
    ),
    is_default = c(TRUE,
                   NA,
                   NA
    )
  )

  expect_equal(out, expected)
})


# test db_access_level_delete --------------------------------------------------
test_with_fresh_db(con_admin, "deleting access level in use", {
  expect_error(
    db_access_level_delete(con_admin, "tsdb_test_access_public", "tsdb_test"),
    "is still in use in timeseries_main"
  )
})

test_with_fresh_db(con_admin, "deleting default access_level", hard_reset = TRUE, {
  expect_error(
    db_access_level_delete(con_admin, "tsdb_test_access_main", "tsdb_test"),
    "is the default access level"
  )
})

test_with_fresh_db(con_admin, "deleting not existing access level", {
  expect_warning(
    db_access_level_delete(con_admin, "tsdb_test_access_restricted2", "tsdb_test"),
    "access level does not exist"
  )
})

test_with_fresh_db(con_admin, "db deleting access level works returns ok", {
  out <- db_access_level_delete(con_admin,
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test")
  expected <- list(status ="ok")

  expect_equal(out, expected)
})


test_with_fresh_db(con_admin, "db deleting access level works", {
  db_access_level_delete(con_admin,
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test")

  out <- dbGetQuery(con_admin, 'select * from tsdb_test.access_levels
                    order by role')

  expected <- data.frame(
    role = c("tsdb_test_access_main",
             "tsdb_test_access_public"

    ),
    description = c("Non-public time series without license restrictions",
                    "Publicly available time series"
    ),
    is_default = c(TRUE,
                   NA
    ),
    stringsAsFactors = FALSE
  )

    expect_equal(out, expected)
})

test_with_fresh_db(con_admin, "db rights to delete access level", {
  expect_error(db_access_level_delete(con_writer,
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test"),
               "not have sufficient privileges")
})

# test db_access_level_create --------------------------------------------------
test_with_fresh_db(con_admin, "db inserting access level that doesn't exist", {
  expect_error(
    db_access_level_create(con_admin,
                            "tsdb_test_access_public2",
                            schema = "tsdb_test"),
    "it can not be an access level"
  )
})

test_with_fresh_db(con_admin, "db inserting access level that already exists", {
  expect_warning(
    db_access_level_create(con_admin,
                            "tsdb_test_access_main",
                            schema = "tsdb_test"),
    "already exists"
  )
})

test_with_fresh_db(con_admin, "db inserting access level works default NA", {
  db_access_level_create(con_admin,
                          "tsdb_test_admin",
                          access_level_description = "admin description",
                          schema = "tsdb_test")

  out <- dbGetQuery(con_admin, 'select * from tsdb_test.access_levels
                    order by role')

  expected <- data.frame(
    role = c("tsdb_test_access_main",
             "tsdb_test_access_public",
             "tsdb_test_access_restricted",
             "tsdb_test_admin"

    ),
    description = c("Non-public time series without license restrictions",
                    "Publicly available time series",
                    "License restricted time series",
                    "admin description"
    ),
    is_default = c(TRUE,
                   NA,
                   NA,
                   NA
    ),
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})

test_with_fresh_db(con_admin, "db inserting access level works stastus ok default NA", {
  out <- db_access_level_create(con_admin,
                          "tsdb_test_admin",
                          access_level_description = "admin description",
                          schema = "tsdb_test")

  expected <- list(status ="ok")

  expect_equal(out, expected)
})


test_with_fresh_db(con_admin, "db inserting access level works default TRUE", {
  db_access_level_create(con_admin,
                          "tsdb_test_admin",
                          access_level_description = "admin description",
                          access_level_default = TRUE,
                          schema = "tsdb_test")

  out <- dbGetQuery(con_admin, 'select * from tsdb_test.access_levels
                    order by role')

  expected <- data.frame(
    role = c("tsdb_test_access_main",
             "tsdb_test_access_public",
             "tsdb_test_access_restricted",
             "tsdb_test_admin"

    ),
    description = c("Non-public time series without license restrictions",
                    "Publicly available time series",
                    "License restricted time series",
                    "admin description"
    ),
    is_default = c(NA,
                   NA,
                   NA,
                   TRUE
    ),
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)

})

test_with_fresh_db(con_admin, "db inserting access level works stastus ok default TRUE", {
  out <- db_access_level_create(con_admin,
                                 "tsdb_test_admin",
                                 access_level_description = "admin description",
                                 access_level_default = TRUE,
                                 schema = "tsdb_test")

  expected <- list(status ="ok")

  expect_equal(out, expected)
})

# db_access_level_set_default ---------------------------------------------

test_with_fresh_db(con_admin, "writer may not change default level", {
  expect_error(
    db_access_level_set_default(con_writer,
                                "tsdb_test_access_public",
                                schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "db_access_level_set_default returns status", {
  out <- db_access_level_set_default(con_admin,
                                     "tsdb_test_access_public",
                                     schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "db_access_level_set_default with nonexisting level", {
  expect_error(
    db_access_level_set_default(con_admin,
                                "hank_the_hacker",
                                schema = "tsdb_test"),
    "level hank_the_hacker"
  )
})

test_with_fresh_db(con_admin, "db_access_level_set_default sets the default", {
  db_access_level_set_default(con_admin, "tsdb_test_access_public", schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.access_levels WHERE is_default")

  expect_equal(
    res$role,
    "tsdb_test_access_public"
  )
})


# getting access level of ts ----------------------------------------------

test_with_fresh_db(con_admin, "db_ts_get_access_level", {
  out <- db_ts_get_access_level(con_reader, c("vts2", "vts1"), schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      ts_key = c("vts2", "vts1"),
      access_level = c("tsdb_test_access_main", "tsdb_test_access_public")
    )
  )
})

test_with_fresh_db(con_admin, "db_ts_get_access_level with targetted vintage", {
  out <- db_ts_get_access_level(con_reader,
                                c("vts2", "vts1"),
                                valid_on = "2020-01-01",
                                schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      ts_key = c("vts2", "vts1"),
      access_level = c("tsdb_test_access_main", "tsdb_test_access_main")
    )
  )
})

test_with_fresh_db(con_admin, "db_ts_access_level with missing key", {
  out <- db_ts_get_access_level(con_reader, "notakey", schema = "tsdb_test")

  expect_equal(
    out,
    data.table(
      ts_key = "notakey",
      access_level = NA_character_
    )
  )
})

