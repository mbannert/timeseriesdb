context("db connection")

test_that("defaults", {
  fake_dbConnect = mock()

  with_mock(
    Postgres = mock("driver"),
    Sys.info = mock(list(user = "bobby"), cycle = TRUE),
    dbConnect = fake_dbConnect,
    {
      db_create_connection("mydb", passwd = "password")

      expect_args(fake_dbConnect,
                  1,
                  "driver",
                  "mydb",
                  "bobby",
                  "localhost",
                  "password",
                  5432,
                  "--application_name=timeseriesdb"
      )
    }
  )
})

test_that("param pass thru", {
  fake_dbConnect = mock()

  with_mock(
    Postgres = mock("driver"),
    Sys.info = mock(list(user = "bobby")),
    dbConnect = fake_dbConnect,
    {
      db_create_connection("mydb",
                           passwd = "password",
                           user = "Jane",
                           host = "remotehost",
                           connection_description = "nonchabusiness",
                           port = 1121)

      expect_args(fake_dbConnect,
                  1,
                  "driver",
                  "mydb",
                  "Jane",
                  "remotehost",
                  "password",
                  1121,
                  "--application_name=nonchabusiness"
      )
    }
  )
})

test_that("password from env", {
  fake_dbConnect = mock()
  fake_sys.getenv = mock("secret")

  with_mock(
    Postgres = mock("driver"),
    Sys.info = mock(list(user = "bobby")),
    dbConnect = fake_dbConnect,
    Sys.getenv = fake_sys.getenv,
    {
      db_create_connection("mydb",
                           passwd = "PG_TEST_PASSWORD",
                           passwd_from_env = TRUE)

      expect_equal(
        mock_args(fake_dbConnect)[[1]]$password,
        "secret"
      )

      expect_args(fake_sys.getenv,
                  1,
                  "PG_TEST_PASSWORD")
    }
  )
})

test_that("password from env, missing", {
  with_mock(
    Sys.getenv = mock(""),
    {
      expect_error(
        db_create_connection("mydb",
                             passwd = "PG_TEST_PASSWORD",
                             passwd_from_env = TRUE),
        "Could not find password"
      )
    }
  )
})

test_that("asking for password", {
  fake_dbConnect = mock()

  with_mock(
    Postgres = mock("driver"),
    Sys.info = mock(list(user = "bobby")),
    dbConnect = fake_dbConnect,
    .rs.askForPassword = mock("isleofyou"),
    commandArgs = mock("RStudio"),
    {
      db_create_connection("mydb")

      expect_equal(
        mock_args(fake_dbConnect)[[1]]$password,
        "isleofyou"
      )
    }
  )
})

test_that("getting password from file", {
  fake_dbConnect = mock()
  fake_readlines = mock(c("letmein", "password123", "iloveyou"))

  with_mock(
    Postgres = mock("driver"),
    Sys.info = mock(list(user = "bobby")),
    file.exists = mock(TRUE),
    dbConnect = fake_dbConnect,
    readPasswordFile = fake_readlines,
    {
      db_create_connection("mydb",
                           passwd = "my/password/file",
                           passwd_from_file = TRUE,
                           line_no = 2)

      expect_equal(
        mock_args(fake_dbConnect)[[1]]$password,
        "password123"
      )

      expect_args(fake_readlines,
                  1,
                  "my/password/file")
    }
  )
})

test_that("password from file, line no too big", {
  fake_readlines = mock(c("letmein", "password123", "iloveyou"))

  with_mock(
    file.exists = mock(TRUE),
    readPasswordFile = fake_readlines,
    {
      expect_error(
        db_create_connection("mydb",
                              passwd = "pwdfile",
                              passwd_from_file = TRUE,
                              line_no = 10),
        "too great"
      )
    }
  )
})

test_that("password from file, nonexistent file", {
  with_mock(
    file.exists = mock(FALSE),
    {
      expect_error(
        db_create_connection("mydb",
                             passwd = "bla",
                             passwd_from_file = TRUE),
        "exist"
      )
    }
  )
})
