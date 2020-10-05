context("get_version")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_reader <- connect_to_test_db("dev_reader_public")
}

test_with_fresh_db(con_admin, "get_version returns the version", {
  out <- db_get_installed_version(con_reader, schema = "tsdb_test")

  expect_equal(
    out,
    as.character(packageVersion("timeseriesdb"))
  )
})
