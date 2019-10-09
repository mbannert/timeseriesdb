### GENERAL (release date independent)

query_create_ts_updates <- function(schema) {
  sprintf("
    CREATE TEMPORARY TABLE ts_updates (
      LIKE %s.timeseries_main
    ) ON COMMIT DROP",
    schema
  )
}

query_insert_ts_updates <- function() {
  "
    INSERT INTO ts_updates VALUES
    ($1, daterange($2, null, '[)'), $3, $4, $5)
  "
}

query_close_main <- function(schema, tbl) {
  sprintf("
      UPDATE %s.%s
      SET ts_validity = daterange(lower(%s.ts_validity), lower(ts_updates.ts_validity))
      FROM ts_updates
      WHERE ts_updates.ts_key = %s.ts_key
      AND upper_inf(%s.ts_validity);
    ",
    schema,
    tbl,
    tbl,
    tbl,
    tbl
  )
}

query_insert_main <- function(schema, tbl) {
  sprintf("
      INSERT INTO %s.%s
      SELECT * FROM ts_updates
    ",
    schema,
    tbl
  )
}

query_delete_empty_validity_main <- function(schema, tbl) {
  sprintf("
      DELETE FROM %s.%s
      WHERE release = (SELECT release FROM ts_updates LIMIT 1)
      AND isempty(ts_validity)
    ",
    schema,
    tbl
  )
}

query_delete_empty_validity_releases <- function(schema) {
  sprintf("
      DELETE FROM %s.releases
      WHERE release = (SELECT release FROM ts_updates LIMIT 1)
      AND isempty(ts_validity)
    ",
    schema
  )
}

### CASE 1: Vintages, no release dates


query_close_releases <- function(schema, valid_from, release_date) {
  if (!is.null(valid_from) && is.null(release_date)) {
    sprintf(
      "
      UPDATE %s.releases
      SET ts_validity = daterange(lower(ts_validity), $1, '[)'),
      release_validity = tstzrange(lower(release_validity), $2, '[)')
      WHERE release = $3
      AND upper_inf(ts_validity);
      ",
      schema
    )
  } else {
    stop("not yet implemented")
  }
}

query_insert_releases <- function(schema, valid_from, release_date) {
  if (!is.null(valid_from) && is.null(release_date)) {
    sprintf("
      INSERT INTO %s.releases VALUES
      ($1, $2, tstzrange((SELECT max(upper(release_validity)) FROM %s.releases), null, '[)'), $3)",
      schema,
      schema
    )
  } else {
    stop("not implemented")
  }
}