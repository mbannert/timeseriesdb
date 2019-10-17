### GENERAL (release date independent)

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
      WHERE isempty(ts_validity)
    ",
    schema,
    tbl
  )
}

query_delete_empty_validity_releases <- function(schema) {
  sprintf("
      DELETE FROM %s.releases
      WHERE isempty(ts_validity)
    ",
    schema
  )
}

query_create_release <- function(schema) {
  sprintf("INSERT INTO %s.releases(release, release_description) VALUES ($1, $2) RETURNING id",
          schema)
}

query_close_ranges_main <- function(schema, tbl) {
  
  sprintf("
      UPDATE %s.%s
      SET ts_validity = daterange(lower(%s.ts_validity), lower(ts_updates.ts_validity)),
      release_validity = tstzrange(lower(%s.release_validity), lower(ts_updates.release_validity))
      FROM ts_updates
      WHERE ts_updates.ts_key = %s.ts_key
      AND upper_inf(%s.ts_validity)
      AND upper_inf(%s.release_validity);
    ",
          schema,
          tbl,
          tbl,
          tbl,
          tbl,
          tbl,
          tbl
  )
}

### Use case specific queries
