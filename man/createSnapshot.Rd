% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/createSnapshot.R
\name{createSnapshot}
\alias{createSnapshot}
\title{Create a Snapshot of Selected Time Series}
\usage{
createSnapshot(con, series, valid_from = Sys.Date(), schema,
  vintage_schema = NULL)
}
\arguments{
\item{con}{PostgreSQL connection object}

\item{series}{character vector containing time series to be snapshot.
Set to NULL if you want a snapshot of all series in a schema.}

\item{valid_from}{date or character formatted date denotes the start of the 
validity of the new time series version.}

\item{schema}{character name of the schema that contains the series to be backed up. 
This is also the target schema if vintage_schema is not set.}

\item{vintage_schema}{character name of the schema snapshots should be stored to. 
Defaults to NULL, using the schema parameter.}
}
\description{
Creating snapshots of entire time series datasets
is useful when a set of time series is composed in advance of 
a forecast. The state of a dataset is archived in order to 
benchmark forecasting methods and make forecasts reproducible.
}
