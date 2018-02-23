# timeseriesdb - Manage Official Statistics' Time Series Data with R and PostgreSQL

*timeseriesdb* maps R time series object to PostgreSQL relations for permanent storage.  
Instead of writing time series to spreadsheet files or .RData on disk, **timeseriesdb**
uses a PostgreSQL schema which allows to store data alongside extensive, context aware, multi-lingual meta information. **timeseriesdb** aims at time series from official statistics which are typically published on a monthly, quarterly or yearly basis. Thus **timeseriesdb** is optimized to handle updates of large parts of a time series caused by data revisions such as GDP revisions. 


## Getting Started

This quick start guide assumes you've PostgreSQL installed or have access to a remote PostgreSQL database including the necessary access rights to create new schemas and tables. Also, let's assume you installed R. 

If that's not case please refer to: 

- [PostgreSQL Download](https://www.postgresql.org/download/)
- [R Language for Statistical Computing](https://www.r-project.org/)


### Before you install the R Package
**timeseriesdb** depends on **RPostgreSQL** to connect to the **PostgreSQL** database, the user needs to make sure that the PostgreSQL's own library and header files are present and can be found by RPostgreSQL. For Windows, the library called **libpq** is attached to the **RPostgreSQL** package and will thus be installed with the R package. Hence Windows users are unlikely to experience further troubles.

For OS X and Linux the installation is a bit different when **libpq** is not present. For some Linux distributions the corresponding library can be obtained with the **postgresql-devel** package. Similarly, on OS X, the user needs to make sure that **libpq** is present and can be found by **RpostgreSQL**. I recommend to use the **homebrew** package manager and run `brew install postgresql`. OS X and Linux users should note that previously installed versions may not contain the libraries provided by **postgresql-devel** package. 


### Install the R package (stable version)

Yay! **timeseriesdb** is on CRAN now. Installing the stable version (right choice for most users) is a matter of running on your R console 

```{r, eval = FALSE}
install.packages("timeseriesdb")
```

or using R (Studio's) **G**raphical **U**ser **I**nterface. 

### Install the R packages (developer version)

The latest version of the **timeseriesdb** is available from [my github account](https://github.com/mbannert/timeseriesdb). The **devtools** package is an easy way to directly install packages directly from github. 


```{r, eval = FALSE}
library(devtools)
install_github('mbannert/timeseriesdb')
```


### Set up PostgreSQL

The basic idea behind **timeseriesdb** storage concept is to use of the PostgreSQL extension *hstore* to store time series in a key value pair store. Thus you'll have install the hstore extension before you can create the relations needed to operate timeseriesdb. Just run: 

```
CREATE EXTENSION hstore;
```

If set up **timeseriesdb** from scratch, create a new postgres schema: 

```
CREATE SCHEMA timeseries;
```

Note that 'timeseries' is the default schema name. That is, the defaults of the R functions use that name. Of course, you can have multiple schemas and manipulate those, by explicitly passing their name as parameter. In the standard case setting up all relations on localhost sandbox database is just a matter of:

```
library(timeseriesdb)
con <- createConObj(dbhost = "localhost",
                    dbname = "sandbox",
                    passwd = "")

runCreateTables(con)

```

### Update PostgreSQL (if you used older version of timeseriesdb)

In case you've used older version of **timeseriesdb** before, you can run 
the following in order to update your tables to add the new release date feature. 

```
library(timeseriesdb)
con <- createConObj(dbhost = "localhost",
                    dbname = "sandbox",
                    passwd = "")

runUpgradeTables(con)

```


## Basic Usage

As stated before, **timeseriesdb** maps R time series objects into their PostgreSQL counterparts for permanent storage. The core functionality comes from the 
`readTimeSeries` and storeTimeSeries functions. 


### Simple CREATE, UPDATE, DELETE (CRUD)

Let's create a few random time series and store them and read them into R again. 
Note that both functions can handle vectors of time series identifiers. The result of read operation will always be a list -- even if you only read a single series. That is because we want to return the same type always. 

```
library(timeseriesdb)
con <- createConObj(dbhost = "localhost",
                    dbname = "sandbox",
                    passwd = "")


tsl <- list(
	ts1 = ts(rnorm(100),start = c(1990,1),freq = 4),
	ts2 = ts(rnorm(100),start = c(1985,1),freq = 12),
	ts3 = ts(rnorm(50),start = c(2000,1),freq = 4))

storeTimeSeries(names(tsl), con, li)

read_it <- readTimeSeries(c("ts1","ts2"),con)


``` 

Deleting one or more series is just a matter of:


```
deleteTimeSeries("ts3",con)
```

I suggest to prevent users from deleting series using PostgreSQL's sophisticated rights management. Thanks to PostgreSQL's row low level security (RLS) you could even set rights on a per record (time series) basis. 


### Storing Time Series Vintages (Real Time Reproducibility)

Sometime it is important to store different versions of the same time series. Revision of official statistics are common example of why you want to store so-called vintages. Assume you use GDP figures published by your national statistical office in your research. At the time you compiled your computations, the official bureau did their best to publish the GDP, but had to revise their estimations later on anyway. As a researcher who cares about reproducibility you may not want to rely on a source being online forever or to store different versions of their own publications for you. 
That's why you can add a *valid_from* and *valid_to* to a *storeTimeSeries*. 
Note that vintages are not allowed to overlap and need to succeed each other without gaps. This will be enforced by **timeseriesdb**.

```

tsl <- list(ts1 = ts(rnorm(100),start = c(1990,1),freq = 4))

storeTimeseries(names(tsl),con, tsl, valid_from = '2018-01-01',
				valid_to = '2018-01-31')

storeTimeseries(names(tsl),con, tsl, valid_from = '2018-02-01',
				valid_to = '2018-02-28')

```

Note how series with the same time series key can co-exist because of a composite primary key which consists of the identifier and validity.


### The release date feature - sharing data

One big advantage of a database over file based storage is the leverage when sharing data. Instead of storing data in *your_personal_folder/in_a_file_with_a_wierd_name/some_where_on_your_hard_disk/* a SQL database is an industry standard which can be accessed through a ton of programming languages including the ones that are popular in web development. So by storing data with *timeseriesdb* you facilitate sharing and publishing it. 

Yet you may not want to share the full information with everyone immediately. Maybe a supervisor needs to do her work before you publish the data or there's a press conference and everybody should get news at the same time? Therefore we've added a release date to **timeseriesdb**. If you don't set it, its default will be 1900. Unless 
you pull of a Marty McFly that's way in the past. But if you set future release date, a web developer can easily use it to cut off the last observation before sharing it through an API.


```
storeTimeseries(names(tsl),con, tsl, release_date = '2018-07-01')

``` 


### Localized and unlocalized meta information

*timeseriesdb* was developen in Switzerland -- a country that speaks four official languages (German, French, Italian, Rumantsch). The latter is only spoken by a small minority and does not really play a role in describing official statistics (though it would be a dream come true to also have data description in Rumantsch). However, the point is, **timeseriesdb** allows you to store meta information in several languages and associate it with the same series!  


```
  m_e <- new.env()
  m_en <- list("wording" = "let's have a word.",
               "check" = "it's english man.!! SELECTION DELETE123")
  addMetaInformation("ts1",m_en,m_e)
  
  # DE
  m_d <- new.env()
  m_de <- list("wording" = "Wir müssen uns mal unterhalten......",
               "check" = "Das ist deutsch. wirklich")
  addMetaInformation("ts1",m_de,m_d)
  
  updateMetaInformation(m_e,
                      con,
                      locale = "en",
                      tbl = "meta_data_localized")

  updateMetaInformation(m_d,
              con,
              locale = "de",
              tbl = "meta_data_localized")


```

In addition the localized meta information, **timeseriesdb** stores a few things that do not need to be translated such as *username* or *last updated* etc.


## Packages that work well with timeseriesdb

**timeseriesdb** was the first of several time series related packages built at KOF. 
In the meantime there a few more to address some other concerns. The package are designed to work well with each other and form a little suite of helpers to make every day life with time series in official statistics a little easier. 

- Mostly plotting and export of time series [tstools](http://whatsgood.io/post/on-cran-now-tstools-a-time-series-toolbox-for-official-statistics/)
- An API wrapper to get time series data through our timeseriesdb backed API [kofdata](http://whatsgood.io/post/introducing-the-kofdata-rstats-CRAN-package/) 
- @christopsax's Best in class wrapper for X13-ARIMA-SEATS seasonal adjustment [seasonal](http://www.seasonal.website/)
















