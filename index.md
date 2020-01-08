# timeseriesdb 

The {timeseriesdb} R package maps R time series representations to PostgreSQL relations. Hence {timeseriesdb} is not only a package for the R Language for Statistical
Computing but also ships with a proven structure for the open source world's most advanced relational database. 

## Why Use {timeseriesdb}? 

- R sessions don't live forever: If a time series should persist beyond its in- -memory representation, we need a way to store time series to disk. 
{timeseriesdb} offers a convenient way to manage time series in an **enterprise level database** as opposed to solely writing time series to files.

- {timeseriesdb} is designed with the idiosyncratic needs of **economic and official statistics** in mind. It ships with extensive support for **multi-lingual meta information** as well as **versioning of time series (vintages)** to handle **data revisions**.

- thanks to PostgreSQL's row level security policies, you can use {timeseriesdb}
to manage access to your time series. 

## Demo - Basic Usage

- read_ts
- store_ts




## Key Features

- map ts, xts and zoo objects to PostgreSQL relations

- Handle data revisions: keep different versions of a single time series (vintages)

- structure to handle comprehensive multi-lingual data descriptions
   - time series level meta information
   - vintage level meta information
   - dataset level meta information
   
- granular time series level access management thanks to PostgreSQL Row Level Security (RLS)

- easy to install through R Install Routines

- DBI compliant backend thanks to {DBI} and {RPostgres}






