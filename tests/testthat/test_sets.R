context("ts sets")

con <- NULL

# same check as skip_on_cran()
on_cran <- !identical(Sys.getenv("NOT_CRAN"), "true")

# could also define fixture first, then convert to query (for less hardcoding of names)
# Talk about a rabbit hole...
fixture <- data.table(
  setname = c(
    "set1",
    "set2",
    "set2",
    "inactiveset"
  ),
  username = c(
    "testus_maximus",
    "testus_maximus",
    "not_testus",
    "somebody_else"
  ),
  key_set = list(
    c("key1", "key2", "key3"),
    c("key1", "key4", "key5"),
    c("keyx", "keyy", "keyz"),
    c("a", "b", "d")
  ),
  set_description = c(
    "testus first set",
    "testus second set",
    "not testus set with same name",
    "nothing to see here, move long"
  ),
  active = c(
    TRUE,
    TRUE,
    TRUE,
    FALSE
  )
)

new_set <- data.table(
  setname = "a_new_set",
  username = "test",
  key_set = list(c("ts_key1", "ts_key2")),
  set_description = "description", 
  active = TRUE
)

explode_fixture <- function(row) {
  row[, .(ts_key = unlist(key_set)),
      .(setname, username, set_description, active)][, 
      .(setname, username, ts_key, set_description, active)]
}
  
fixture_query <- paste(
  sprintf("INSERT INTO timeseriesdb_unit_tests.timeseries_sets(%s) VALUES ", paste(names(fixture), collapse = ",")),
  fixture[, 
          .(values = 
              sprintf("('%s', '%s', ARRAY['%s'], '%s', %s)",
                      setname,
                      username,
                      paste(unlist(key_set), collapse = "','"),
                      set_description,
                      ifelse(active, "true", "false")
              )),
          by = 1:nrow(fixture)][, paste(values, collapse = ",")]
)

if (!on_cran) {
  con <- createConObj(dbhost = "localhost",
                      dbname = "sandbox",
                      passwd = "")
  
  # Cleanest of clean slates
  dbGetQuery(con, "DROP SCHEMA timeseriesdb_unit_tests CASCADE")
  dbGetQuery(con, "CREATE SCHEMA timeseriesdb_unit_tests")
  
  # Could also write a test for that
  runCreateTables(con, "timeseriesdb_unit_tests")
}

# Wrapper for clean tests (the man himself said so... https://github.com/r-lib/testthat/issues/544)
test_set <- function(name, code) {
  dbGetQuery(con, "DELETE FROM timeseriesdb_unit_tests.timeseries_sets")
  dbGetQuery(con, fixture_query)
  test_that(name, code)
}

test_set("storeTsSet.character", {
  skip_on_cran()
  out <- storeTsSet(con, 
                    new_set$setname,
                    unlist(new_set$key_set),
                    new_set$username,
                    new_set$set_description,
                    new_set$active,
                    schema = "timeseriesdb_unit_tests")
  expect_equal(attributes(out)$query_status, "OK")
  
  set_read <- as.data.table(dbGetQuery(con, sprintf("SELECT setname, username, unnest(key_set) as ts_key, set_description, active
                         FROM timeseriesdb_unit_tests.timeseries_sets WHERE setname = '%s';", new_set$setname)))
  
  expect_equal(set_read, explode_fixture(new_set[1, ]))
})

test_set("storeTsSet.list", {
  skip_on_cran()
  keys <- unlist(new_set$key_set)
  ley_kist <- as.list(rep("ts_key", length(keys)))
  names(ley_kist) <- keys
  out <- suppressWarnings(storeTsSet(con, 
                    new_set$setname,
                    ley_kist,
                    new_set$username,
                    new_set$set_description,
                    new_set$active,
                    schema = "timeseriesdb_unit_tests"))
  expect_equal(attributes(out)$query_status, "OK")
  
  set_read <- as.data.table(dbGetQuery(con, sprintf("SELECT setname, username, unnest(key_set) as ts_key, set_description, active
                                                    FROM timeseriesdb_unit_tests.timeseries_sets WHERE setname = '%s';", new_set$setname)))
  
  expect_equal(set_read, explode_fixture(new_set[1, ]))
})

test_set("storeTsSet.list throws a deprecation warning", {
  skip_on_cran()
  expect_warning(storeTsSet(
    con, "a list set", list(ts_key1 = "ts_key", ts_key2 = "ts_key"), schema = "timeseriesdb_unit_tests"), "deprecated")
})

test_set("storing sets with same name but different username works", {
  skip("unimplemented")
})

test_set("joinTsSets", {
  skip("unimplemented")
})

test_set("listTsSets", {
  skip("unimplemented")
})

test_set("loadTsSet", {
  skip_on_cran()
})

test_set("deactivateTsSet deactivates an active set", {
  skip_on_cran()
  out <- deactivateTsSet(con,
                         fixture[1, setname],
                         fixture[1, username],
                         schema = "timeseriesdb_unit_tests"
                         )
  
  
  expect_equal(attributes(out)$query_status, "OK")
  
  setstate <- runDbQuery(con, sprintf("SELECT active
                          FROM timeseriesdb_unit_tests.timeseries_sets
                          WHERE setname = '%s' AND username = '%s';",
                          fixture[1, setname],
                          fixture[1, username])
             )
  
  expect_false(setstate$active)
})

test_set("deactivateTsSet leaves an inactive set untouched", {
  skip_on_cran()
  i <- fixture[, min(which(!active))]
  out <- deactivateTsSet(con,
                         fixture[i, setname],
                         fixture[i, username],
                         schema = "timeseriesdb_unit_tests"
  )
  
  
  expect_equal(attributes(out)$query_status, "OK")
  
  setstate <- runDbQuery(con, sprintf("SELECT active
                                      FROM timeseriesdb_unit_tests.timeseries_sets
                                      WHERE setname = '%s' AND username = '%s';",
                                      fixture[i, setname],
                                      fixture[i, username])
  )
  
  expect_false(setstate$active)
})

test_set("activateTsSet activates an inactive set", {
  skip_on_cran()
  i <- fixture[, min(which(!active))]
  out <- activateTsSet(con,
                         fixture[i, setname],
                         fixture[i, username],
                         schema = "timeseriesdb_unit_tests"
  )
  
  
  expect_equal(attributes(out)$query_status, "OK")
  
  setstate <- runDbQuery(con, sprintf("SELECT active
                                      FROM timeseriesdb_unit_tests.timeseries_sets
                                      WHERE setname = '%s' AND username = '%s';",
                                      fixture[i, setname],
                                      fixture[i, username])
  )
  
  expect_true(setstate$active)
})

test_set("activateTsSet leaves an active set unaffected", {
  skip_on_cran()
  i <- fixture[, min(which(active))]
  out <- activateTsSet(con,
                         fixture[i, setname],
                         fixture[i, username],
                         schema = "timeseriesdb_unit_tests"
  )
  
  
  expect_equal(attributes(out)$query_status, "OK")
  
  setstate <- runDbQuery(con, sprintf("SELECT active
                                      FROM timeseriesdb_unit_tests.timeseries_sets
                                      WHERE setname = '%s' AND username = '%s';",
                                      fixture[i, setname],
                                      fixture[i, username])
  )
  
  expect_true(setstate$active)
})

test_set("overwriteTsSet", {
  skip_in_cran()
  
  out <- overwriteTsSet(con,
                        fixture[1, setname],
                        unlist(fixture[2, key_set]),
                        fixture[1, username],
                        fixture[1, set_description],
                        fixture[1, active],
                        schema = "timeseriesdb_unit_tests")
  expect_equal(attributes(out)$query_status, "OK")
  
  set_read <- as.data.table(dbGetQuery(con,
                                    sprintf("SELECT setname, username, unnest(key_set) as ts_key, set_description, active
                                        FROM timeseriesdb_unit_tests.timeseries_sets
                                        WHERE setname = '%s' AND username = '%s';", 
                                            fixture[1, setname], fixture[1, username])))
  
  expect_equal(set_read, explode_fixture(fixture[1, ])[, ts_key := fixture[2, key_set]])
})

test_set("overwriteTsSet returns the delete query status on error", {
  # that's really more of a unit test tho
  skip("unimplemented")
})

test_set("addKeysToTsSet", {
  skip("unimplemented")
})

test_set("removeKeysFromTsSet", {
  skip("unimplemented")
})

test_set("changeTsSetOwner", {
  skip("unimplemented")
})

test_set("deleteTsSet", {
  skip("unimplemented")
})

if(!on_cran) {
  dbDisconnect(con)
}