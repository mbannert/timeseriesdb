# On the future of time series metadata

All metadata are (is?) stored in a single JSON object:
```
{
  resource_generated_by: "text",
  resource_last_update: timestamp,
  coverage_temp: "text?",
  md_unlocalized: {
    key: value,
    key2: value2
  },
  md_localized: {
    locale1: {
      key: value,
      ...
    },
    locale2: {
      ...
    },
    ...
  }
}
```

this way no separate logic is needed for meta data: ts- and meta-data are just versioned "records".
All meta data can live in a single table.
Problem:
what is now known as md unlocalized is updated for each release, localized need not be?

## In fact
Why not just cram ts data in there as well and build us a monolithic table?
At the cost of possible metadata optimization