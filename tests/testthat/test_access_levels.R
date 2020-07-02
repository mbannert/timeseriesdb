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


# test db_delete_access_levels --------------------------------------------------
test_with_fresh_db(con_admin, "deleting access level in use", {
  expect_error(
    db_delete_access_levels(prueba_con, "tsdb_test_access_public", "tsdb_test"),
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

test_with_fresh_db(con_admin, "db deleting access level works", {
  expect_error(db_delete_access_levels(con_writer, 
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test"))
})

