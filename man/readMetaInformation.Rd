% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/readMetaInformation.R
\name{readMetaInformation}
\alias{readMetaInformation}
\title{Read Meta Information from a Time Series Database}
\usage{
readMetaInformation(con, series, locale = "de",
  tbl_localized = "meta_data_localized",
  tbl_unlocalized = "meta_data_unlocalized", schema = "timeseries",
  as_list = TRUE, regex = FALSE)
}
\arguments{
\item{con}{PostgreSQL connection object}

\item{series}{character name of a time series object.}

\item{locale}{character denoting the locale of the meta information that is queried.
defaults to 'de' for German. At the KOF Swiss Economic Institute meta information should be available
als in English 'en', French 'fr' and Italian 'it'. Set the locale to NULL to query unlocalized meta information.}

\item{tbl_localized}{character name of the table that contains localized meta information. Defaults to 'meta_data_localized'.}

\item{tbl_unlocalized}{character name of the table that contains general meta information. Defaults to 'meta_data_unlocalized'.}

\item{schema}{SQL schema name. Defaults to timeseries.}

\item{as_list}{Should the result be returned as a tsmeta.list instead of a tsmeta.dt? Default TRUE}

\item{regex}{If set to TRUE, series will be interpreted as a regular exporession, so that metadata for all time series whose keys match the pattern will be returned.}
}
\description{
This function reads meta information from a timeseriesdb package PostgreSQL
database and puts into a meta information environment.
}
