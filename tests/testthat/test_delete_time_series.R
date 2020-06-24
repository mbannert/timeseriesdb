context("deleting ts")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
}

test_that("it aborts if the user does not type yes", {
  fake_readline <- mock("nope")
  with_mock(
    readline = fake_readline,
    expect_error(db_delete_time_series("con", "vts2", "tsdb_test"), "nope")
  )
})

test_with_fresh_db(con_admin, "writer may not delete whole ts", {
  fake_readline <- mock("yes")
  with_mock(
    readline = fake_readline,
    {
      expect_error(
        db_delete_time_series(con_writer, "vts1", schema = "tsdb_test"),
        "Only timeseries admins"
      )
    }
  )
})

test_with_fresh_db(con_admin, "db_delete_time_series returns a status", {
  fake_readline <- mock("yes")

  with_mock(
    readline = fake_readline,
    {
      out <- db_delete_time_series(con_admin, "vts1", schema = "tsdb_test")
      expect_equal(out, list(status = "ok"))
    }
  )
})

test_with_fresh_db(con_admin, "deleting ts cleans house", {
  fake_readline <- mock("yes")
  with_mock(
    readline = fake_readline,
    {
      db_delete_time_series(con_admin, c("vts1", "vts2"), schema = "tsdb_test")

      vintages <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main WHERE ts_key = ANY('{vts1, vts2}'::TEXT[])")
      expect_equal(nrow(vintages), 0)

      mdl <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.metadata_localized WHERE ts_key = ANY('{vts1, vts2}')")
      expect_equal(nrow(mdl), 0)

      mdul <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.metadata WHERE ts_key = ANY('{vts1, vts2}')")
      expect_equal(nrow(mdul), 0)

      catalog <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog where ts_key = ANY('{vts1, vts2}')")
      expect_equal(nrow(catalog), 0)

      collects <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.collect_catalog WHERE ts_key = ANY('{vts1, vts2}')")
      expect_equal(nrow(collects), 0)
    }
  )
})

test_with_fresh_db(con_admin, "deleting all ts in a set removes said set", {
  skip("need to figure out if we actually want this behavior")
})
