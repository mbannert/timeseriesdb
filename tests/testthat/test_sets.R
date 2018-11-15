context("ts sets")

con <- NULL

# same check as skip_on_cran()
on_cran <- !identical(Sys.getenv("NOT_CRAN"), "true")

# could also define fixture first, then convert to query (for less hardcoding of names)
# Talk about a rabbit hole...
fixture <- data.table(
  setname = c("set1", "set2", "set2", "inactiveset"),
  username = c("testus_maximus", "testus_maximus", "not_testus", "somebody_else"),
  key_set = list(list("key1", "key2", "key3"), list("key4", "key5", "key1"))
)

set_fixture <- "
INSERT INTO timeseriesdb_unit_tests.timeseries_sets(setname, username, key_set, set_description, active) VALUES
('set1', 'testus_maximus', ARRAY['key1', 'key2', 'key3'], 'testus first set', true),
('set2', 'testus_maximus', ARRAY['key4', 'key5', 'key6'], 'testus second set', true),
('set2', 'not_testus', ARRAY['keyx', 'keyy', 'keyz'], 'not testus set with same name', true),
('inactiveset', 'somebody', ARRAY['a', 'b', 'c'], 'nothing to see here', false)
"

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
  dbGetQuery(con, set_fixture)
  test_that(name, code)
}

test_set("insert a set", {
  skip_on_cran()
  out <- storeTsSet(con, "a_new_set", c("ts_key1", "ts_key2"), "test", "description", schema = "timeseriesdb_unit_tests")
  expect_equal(attributes(out)$query_status, "OK")
  set_read <- dbGetQuery(con, "SELECT setname, username, unnest(key_set) as key_set, set_description, active
                         FROM timeseriesdb_unit_tests.timeseries_sets WHERE setname = 'a_new_set';")
  exp <- data.frame(
    setname = "a_new_set",
    username = "test",
    key_set = c("ts_key1", "ts_key2"),
    set_description = "description", 
    active = TRUE, 
    stringsAsFactors = FALSE)
  expect_equal(set_read, exp)
})

test_set("storeTsSet.list throws a deprecation warning", {
  skip_on_cran()
  expect_warning(storeTsSet(
    con, "a list set", list(ts_key1 = "ts_key", ts_key2 = "ts_key"), schema = "timeseriesdb_unit_tests"), "deprecated")
})

test_set("joinTsSets", {
  skip()
})

test_set("listTsSets", {
  skip()
})

test_set("loadTsSet", {
  skip_on_cran()
})

test_set("deactivateTsSet", {
  skip()
})

test_set("acrivateTsSet", {
  skip()
})

test_set("overwriteTsSet", {
  skip()
})

test_set("addKeysToTsSet", {
  skip()
})

test_set("removeKeysFromTsSet", {
  skip()
})

test_set("changeTsSetOwner", {
  skip()
})

test_set("deleteTsSet", {
  skip()
})

if(!on_cran) {
  dbDisconnect(con)
}