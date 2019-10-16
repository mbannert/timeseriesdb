### GENERAL (release date independent)

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

### Use case specific queries

query_close_releases <- function(schema,
                                 valid_from,
                                 release_date) {
  uc <- get_use_case(valid_from, release_date)
  
  if (uc == 1 || uc == 2) {
    sprintf("
      UPDATE %s.releases
      SET ts_validity = daterange(lower(ts_validity), $1, '[)'),
      release_validity = tstzrange(lower(release_validity), $2, '[)')
      WHERE release = $3
      AND upper_inf(ts_validity)",
      schema
    )
  } else {
    stop("not yet implemented")
  }
}

query_insert_releases <- function(schema,
                                  valid_from,
                                  release_date) {
  use_case <- get_use_case(valid_from, release_date)
  
  if (use_case == 1) {
    sprintf("
      INSERT INTO %s.releases VALUES
      ($1, $2, tstzrange((SELECT max(upper(release_validity)) FROM %s.releases), null, '[)'), $3)",
      schema,
      schema
    )
  } else if (use_case == 2) {
    sprintf("
            INSERT INTO %s.releases VALUES
            ($1, $2, $3, $4)",
            schema) 
  } else {
    stop("not implemented")
  }
}
