context("db_ts_store general")

test_that("returns an empty list if there are no ts matching criteria", {
  skip("currently borked")
  # with_mock(
  #   "timeseriesdb:::db_populate_ts_read" = mock(0, cycle = TRUE),
  #   {
  #     expect_equal(
  #       suppressWarnings(db_ts_read("con",
  #                        "bla")),
  #       list()
  #     )
  #
  #     expect_equal(
  #       suppressWarnings(db_ts_read("con",
  #                        "bla",
  #                        regex = TRUE)),
  #       list()
  #     )
  #   }
  # )
})
