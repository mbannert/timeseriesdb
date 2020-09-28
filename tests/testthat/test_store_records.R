context("store_records")

if(is_test_db_reachable()) {
  con_admin <- connect_to_test_db()
  con_writer <- connect_to_test_db("dev_writer")
  con_reader <- connect_to_test_db("dev_reader_main")
}

tsl <- list(
  ts1 = ts(1:4, 2019, frequency = 12),
  ts2 = ts(1:5, 2019, frequency = 4)
)
class(tsl) <- c("tslist", "list")

tsl_update <- list(
  ts1 = ts(rep(5, 4), 2019, frequency = 12)
)
class(tsl_update) <- c("tslist", "list")

ts_single <- ts(100:110, start = 2000, frequency = 4)

xts_single <- xts(1000:1010, order.by = seq(as.Date("2020-01-01"), length.out = 11, by = "1 days"))

tsl_na <- list(
  tsna = ts(c(1, 2, NA, 56), 2020, frequency = 12)
)
class(tsl_na) <- c("tslist", "list")

main_names <- c("id", "ts_key", "validity", "coverage", "release_date", "created_by",
                "created_at", "ts_data", "access")
names_to_test <- setdiff(main_names, c("id", "created_by", "created_at"))

# Test data generated with following code:
# con_admin <- connect_to_test_db()
# con_writer <- connect_to_test_db("dev_writer")
#
# dbExecute(con_admin, "DELETE FROM tsdb_test.timeseries_main")
# dbExecute(con_admin, "DELETE FROM tsdb_test.catalog")
#
# db_ts_store(con_writer,
#                   tsl,
#                   "tsdb_test_access_public",
#                   valid_from = "2019-01-01",
#                   release_date = "2019-01-02",
#                   schema = "tsdb_test")
#
# catalog_after_insert_1 <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key")
# main_after_insert_1 <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")
#
# db_ts_store(con_writer,
#                   tsl,
#                   "tsdb_test_access_public",
#                   valid_from = "2019-02-01",
#                   release_date = "2019-02-02",
#                   schema = "tsdb_test")
#
# catalog_after_insert_2 <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key")
# main_after_insert_2 <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")
#
# db_ts_store(con_writer,
#                   tsl,
#                   "tsdb_test_access_public",
#                   valid_from = "2019-03-01",
#                   release_date = "2019-03-02",
#                   schema = "tsdb_test")
#
# catalog_after_insert_3 <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key")
# main_after_insert_3 <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")
#
# db_ts_store(con_writer,
#                   ts_single,
#                   "tsdb_test_access_public",
#                   valid_from = "2020-01-01",
#                   release_date = "2020-04-01",
#                   schema = "tsdb_test")
#
# catalog_after_insert_single <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key")
# main_after_insert_single <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")
#
# db_ts_store(con_writer,
#                   xts_single,
#                   "tsdb_test_access_public",
#                   valid_from = "2020-01-01",
#                   release_date = "2020-04-01",
#                   schema = "tsdb_test")
#
# catalog_after_insert_single_xts <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key")
# main_after_insert_single_xts <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")
#
# db_ts_store(con_writer,
#             tsl_na,
#             "tsdb_test_access_main",
#             valid_from = "2020-01-01",
#             release_date = "2020-01-02",
#             schema = "tsdb_test")
#
# main_after_na <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")
#
# db_ts_store(con_writer,
#                   tsl_update,
#                   "tsdb_test_access_public",
#                   valid_from = "2019-03-01",
#                   release_date = "2019-03-02",
#                   schema = "tsdb_test")
#
# main_after_update <- dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")
#
#
# save(
#   catalog_after_insert_1,
#   main_after_insert_1,
#   catalog_after_insert_2,
#   main_after_insert_2,
#   catalog_after_insert_3,
#   main_after_insert_3,
#   catalog_after_insert_single,
#   main_after_insert_single,
#   main_after_insert_single_xts,
#   catalog_after_insert_single_xts,
#   main_after_update,
#   main_after_na,
#   file = "tests/testdata/store_records_data.RData"
# )


load("../testdata/store_records_data.RData")

test_with_fresh_db(con_admin, "reader may not store", {
  expect_error(db_ts_store(con_reader,
                                 tsl,
                                 "tsdb_test_access_public",
                                 valid_from = "2019-01-01",
                                 release_date = "2019-01-02",
                                 schema = "tsdb_test"),
               "sufficient privileges")
})

test_with_fresh_db(con_admin, "It returns a status json", {
  out <- db_ts_store(con_writer,
                           tsl,
                           "tsdb_test_access_public",
                           valid_from = "2019-01-01",
                           release_date = "2019-01-02",
                           schema = "tsdb_test")
  expect_is(out, "list")
  expect_equal(out$status, "ok")
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "Inserts produce valid state", {
  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-01-01",
                    release_date = "2019-01-02",
                    schema = "tsdb_test")
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key"),
    catalog_after_insert_1
  )
  main_names <- c("id", "ts_key", "validity", "coverage", "release_date", "created_by",
                  "created_at", "ts_data", "access")
  names_to_test <- setdiff(main_names, c("id", "created_by", "created_at"))
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_insert_1[, names_to_test]
  )

  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-02-01",
                    release_date = "2019-02-02",
                    schema = "tsdb_test")
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key"),
    catalog_after_insert_2
  )
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_insert_2[, names_to_test]
  )

  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-03-01",
                    release_date = "2019-03-02",
                    schema = "tsdb_test")
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key"),
    catalog_after_insert_3
  )
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_insert_3[, names_to_test]
  )

  db_ts_store(con_writer,
                    ts_single,
                    "tsdb_test_access_public",
                    valid_from = "2020-01-01",
                    release_date = "2020-04-01",
                    schema = "tsdb_test")

  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_insert_single[, names_to_test]
  )

  db_ts_store(con_writer,
                    xts_single,
                    "tsdb_test_access_public",
                    valid_from = "2020-01-01",
                    release_date = "2020-04-01",
                    schema = "tsdb_test")

  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_insert_single_xts[, names_to_test]
  )

  db_ts_store(con_writer,
              tsl_na,
              valid_from = "2020-01-01",
              release_date = "2020-01-02",
              schema = "tsdb_test")

  expect_equal(dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
               main_after_na[, names_to_test])
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "Inserts with plain list", {
  l <- unclass(tsl)

  db_ts_store(con_writer,
                    l,
                    "tsdb_test_access_public",
                    valid_from = "2019-01-01",
                    release_date = "2019-01-02",
                    schema = "tsdb_test")
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.catalog ORDER BY ts_key"),
    catalog_after_insert_1
  )
  main_names <- c("id", "ts_key", "validity", "coverage", "release_date", "created_by",
                  "created_at", "ts_data", "access")
  names_to_test <- setdiff(main_names, c("id", "created_by", "created_at"))
  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_insert_1[, names_to_test]
  )
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "storing series with invalid vintages is an error", {
  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-01-01",
                    release_date = "2019-01-02",
                    schema = "tsdb_test")
  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-02-01",
                    release_date = "2019-02-02",
                    schema = "tsdb_test")
  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-03-01",
                    release_date = "2019-03-02",
                    schema = "tsdb_test")
  failed <- db_ts_store(con_writer,
                              tsl[1],
                              "public",
                              valid_from = "2019-02-01",
                              release_date = "2019-03-02",
                              schema = "tsdb_test")
  expect_equal(names(failed), c("status", "message", "offending_keys"))
  expect_equal(failed$status, "warning")
  expect_equal(failed$offending_keys, "ts1")
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "storing with edge vintage causes update", {
  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-01-01",
                    release_date = "2019-01-02",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-02-01",
                    release_date = "2019-02-02",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-03-01",
                    release_date = "2019-03-02",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    tsl_update,
                    "tsdb_test_access_public",
                    valid_from = "2019-03-01",
                    release_date = "2019-03-02",
                    schema = "tsdb_test")

  # TODO: disentangle these tests
  db_ts_store(con_writer,
              tsl_na,
              valid_from = "2020-01-01",
              release_date = "2020-01-02",
              schema = "tsdb_test")

  db_ts_store(con_writer,
                    ts_single,
                    "tsdb_test_access_public",
                    valid_from = "2020-01-01",
                    release_date = "2020-04-01",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    xts_single,
                    "tsdb_test_access_public",
                    valid_from = "2020-01-01",
                    release_date = "2020-04-01",
                    schema = "tsdb_test")

  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_update[, names_to_test]
  )
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "storing with edge vintage causes update of access", {
  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-03-01",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    tsl_update,
                    "tsdb_test_access_main",
                    valid_from = "2019-03-01",
                    schema = "tsdb_test")


  expect_equal(
    dbGetQuery(con_admin, "SELECT access FROM tsdb_test.timeseries_main WHERE ts_key = 'ts1'")$access,
    "tsdb_test_access_main"
  )
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "overwriting older vintage is not possible", {
  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-01-01",
                    release_date = "2019-01-02",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-02-01",
                    release_date = "2019-02-02",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    tsl,
                    "tsdb_test_access_public",
                    valid_from = "2019-03-01",
                    release_date = "2019-03-02",
                    schema = "tsdb_test")

  db_ts_store(con_writer,
                    tsl_update,
                    "tsdb_test_access_public",
                    valid_from = "2019-02-01",
                    release_date = "2019-03-02",
                    schema = "tsdb_test")

  expect_equal(
    dbGetQuery(con_admin, "SELECT * FROM tsdb_test.timeseries_main ORDER BY ts_key, validity")[, names_to_test],
    main_after_insert_3[, names_to_test]
  )
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "db_ts_store uses the default access level", {
  db_ts_store(con_writer, tsl[1], schema = "tsdb_test")

  acl <- dbGetQuery(con_admin, "SELECT access FROM tsdb_test.timeseries_main")$access
  dflt <- dbGetQuery(con_admin, "SELECT role FROM tsdb_test.access_levels WHERE is_default")$role
  expect_equal(acl, dflt)
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "db_ts_store without default access level set", {
  dbExecute(con_admin, "UPDATE tsdb_test.access_levels SET is_default = NULL")

  expect_error(
    db_ts_store(con_writer, tsl[1], schema = "tsdb_test"),
    "access level supplied"
  )
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "db_ts_store complains about invalid access level", {
  expect_error(db_ts_store(con_writer,
                                 tsl,
                                 "my_precious",
                                 schema = "tsdb_test"),
               "a valid access level")
})

test_with_fresh_db(con_admin, hard_reset = TRUE, "db_ts_store with an xts", {
  xtsl <- list(
    rtsx = xts(seq(4), order.by = seq(as.Date("2020-01-01"), length.out = 4, by = "1 days"))
  )

  db_ts_store(con_writer,
                    xtsl,
                    schema = "tsdb_test")

  mn <- dbGetQuery(con_admin, "SELECT ts_data FROM tsdb_test.timeseries_main WHERE ts_key = 'rtsx'")

  expect_match(mn$ts_data, '"frequency":null')
  expect_match(mn$ts_data, '"time":\\["2020-01-01","2020-01-02","2020-01-03","2020-01-04"\\]')
  expect_match(mn$ts_data, '"value":\\[1,2,3,4\\]')
})
