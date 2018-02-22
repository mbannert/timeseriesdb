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
`readTimeSeries` and writeTimeSeries functions. 


### Simple CREATE, UPDATE, DELETE (CRUD)




### Storing Time Series Vintages (Real Time Reproducibility)



### The release date feature



## Packages that work well with timeseriesdb

Nice toolbox for official statistics. 

- tstools (list of time series)
- kofdata
- seasonal


open source for official statistics... 













