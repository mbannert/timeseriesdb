#' KOF indicators
#'
#' @source KOF Swiss Economic Institute - KOF indicators.
#' \url{https://kof.ethz.ch/en/forecasts-and-indicators/indicators.html}
#' @format A list with four time series objects:
#' \describe{
#'  \item{ch.kof.barometer}{Indicator for the Swiss Business Cycle.}
#'  \item{baro}{Vintages (versions) of the KOF Barometer Indicator.}
#'  \item{ch.kof.ie.retro.ch_total.ind.d11}{KOF Employment Indicator for Switzerland}
#' }
#' @examples
#' \dontrun{
#'  kof_ts
#' }
"kof_ts"


#' Zurich Airport Departures and Arrivals
#' 
#' Time series of daily departures and arrivals at Zurich airport. 
#'
#' @source Zurich Airport, processed KOF High Frequency Dashboard
#' \url{https://kofdata.netlify.app/#/}
#' @format A list with two time series objects:
#' \describe{
#'  \item{ch.zrh_airport.departure.total}{Total Daily Departures ZRH Airport}
#'  \item{ch.zrh_airport.arrival.total}{Total Daily Arrivals ZRH Airport}
#' }
#' @examples
#' \dontrun{
#'  zrh_airport
#' }
"zrh_airport"


#' Meta DAta Zurich Airport Departures and Arrivals 
#' 
#' Meta Data of time series of daily departures and arrivals at Zurich airport. 
#'
#' @source Zurich Airport, processed KOF High Frequency Dashboard
#' \url{https://kofdata.netlify.app/#/}
#' @format A list with several meta description items
#' \describe{
#'  \item{title}{Title}
#'  \item{source_name}{Name of the Source}
#'  \item{source_url}{URL of the dataset}
#'  \item{units}{Units}
#'  \item{aggreagte}{aggregate}
#'  \item{dim.order}{Sequence of dimensions}
#'  \item{hierarchy}{hierarchy}
#'  \item{labels}{labels}
#'  \item{details}{details}
#'  \item{utc.updated}{UTC updated time}
#' }
#' @examples
#' \dontrun{
#'  zrh_airport_md
#' }
"zrh_airport_md"


