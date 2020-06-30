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
