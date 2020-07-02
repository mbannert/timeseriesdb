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

test_with_fresh_db(con_admin, "db deleting access level works returns ok", {
  out <- db_delete_access_levels(con_admin, 
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test")
  expected <- list(status ="ok")
  
  expect_equal(out, expected)
})


test_with_fresh_db(con_admin, "db deleting access level works", {
  db_delete_access_levels(con_admin, 
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
  expect_error(db_delete_access_levels(con_writer, 
                                 "tsdb_test_access_restricted",
                                 schema = "tsdb_test"),
               "not have sufficient privileges")
})


# test db_insert_access_levels --------------------------------------------------
test_with_fresh_db(con_admin, "db inserting access level that doesn't exist", {
  expect_error(
    db_insert_access_levels(con_admin, 
                            "tsdb_test_access_public2", 
                            schema = "tsdb_test"),
    "it can not be an access level"
  )
})

test_with_fresh_db(con_admin, "db inserting access level that already exists", {
  expect_warning(
    db_insert_access_levels(con_admin, 
                            "tsdb_test_access_main", 
                            schema = "tsdb_test"),
    "already exists"
  )
})

test_with_fresh_db(con_admin, "db inserting access level works default NA", {
  db_insert_access_levels(con_admin,
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
  out <- db_insert_access_levels(con_admin,
                          "tsdb_test_admin",
                          access_level_description = "admin description",
                          schema = "tsdb_test")
  
  expected <- list(status ="ok")
  
  expect_equal(out, expected)
})


test_with_fresh_db(con_admin, "db inserting access level works default TRUE", {
  db_insert_access_levels(con_admin,
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
  out <- db_insert_access_levels(con_admin,
                                 "tsdb_test_admin",
                                 access_level_description = "admin description",
                                 access_level_default = TRUE,
                                 schema = "tsdb_test")
  
  expected <- list(status ="ok")
  
  expect_equal(out, expected)
})

