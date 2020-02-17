context("collections - insert")

con <- NULL
if(is_test_db_reachable()) {
  con <- connect_to_test_db()
}

# TODO: Tests for db_call_function args

test_with_fresh_db(con, "db_collection_add returns OK", {
  result <- db_collection_add(con,
                              "tests first",
                              "ts4")

  expect_equal(result,
               list(status = "ok",
                    message = "All keys have been successfully added to the collection."))
})
