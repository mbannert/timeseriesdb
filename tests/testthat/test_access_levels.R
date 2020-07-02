context("access_levels")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
  con_reader <- connect_to_test_db("dev_reader_public")
}


# test db_list_access_levels --------------------------------------------------
test_with_fresh_db(con_admin, "db_list_access_levels returns data frame with correct names", {
  out <- db_list_access_levels(con_reader, schema = "tsdb_test")

  expected <- data.frame(
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
    ),
    stringsAsFactors = FALSE
  )

  expect_equal(out, expected)
})


# test db_delete_access_levels --------------------------------------------------
test_with_fresh_db(con_admin, "deleting access level in use", {
  expect_error(
    db_delete_access_levels(con_admin, "tsdb_test_access_public", "tsdb_test"),
    "is still in use in timeseries_main"
  )
})

test_with_fresh_db(con_admin, "deleting default access_level", hard_reset = TRUE, {
  expect_error(
    db_delete_access_levels(con_admin, "tsdb_test_access_main", "tsdb_test"),
    "is the default access level"
  )
})

test_with_fresh_db(con_admin, "deleting not existing access level", {
  expect_warning(
    db_delete_access_levels(con_admin, "tsdb_test_access_restricted2", "tsdb_test"),
    "access level does not exist"
  )
})

test_with_fresh_db(con_admin, "db deleting access level works", {
  out <- db_delete_access_levels(con_admin,
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test")
  expected <- list(status ="ok")

  expect_equal(out, expected)
})

test_with_fresh_db(con_admin, "db rights to delete access level", {
  expect_error(db_delete_access_levels(con_writer,
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test"),
               "not have sufficient privileges")
})



# db_set_default_access_level ---------------------------------------------

test_with_fresh_db(con_admin, "writer may not change default level", {
  expect_error(
    db_set_default_access_level(con_writer,
                                "tsdb_test_access_public",
                                schema = "tsdb_test"),
    "sufficient privileges"
  )
})

test_with_fresh_db(con_admin, "db_set_default_access_level returns status", {
  out <- db_set_default_access_level(con_admin,
                                     "tsdb_test_access_public",
                                     schema = "tsdb_test")

  expect_equal(
    out,
    list(
      status = "ok"
    )
  )
})

test_with_fresh_db(con_admin, "db_set_default_access_level with nonexisting level", {
  expect_error(
    db_set_default_access_level(con_admin,
                                "hank_the_hacker",
                                schema = "tsdb_test"),
    "level hank_the_hacker"
  )
})

test_with_fresh_db(con_admin, "db_set_default_access_level sets the default", {
  db_set_default_access_level(con_admin, "tsdb_test_access_public", schema = "tsdb_test")

  res <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.access_levels WHERE is_default")

  expect_equal(
    res$role,
    "tsdb_test_access_public"
  )
})
