# {timeseriesdb}: Manage Time Series Data with R and PostgreSQL

{timeseriesdb} maps R time series objects to PostgreSQL database relations for permanent storage. Instead of writing time series to spreadsheet files or .RData files on disk, {timeseriesdb} uses a set of PostgreSQL relations which allows to store data alongside extensive, context aware, multi-lingual meta information. {timeseriesdb} was originally designed with official statistics in mind which are typically published on a monthly, quarterly or yearly basis. But the package can also handle time series of irregular frequency. As opposed to many IoT based approaches, {timeseriesdb} is not optimized to append as fast as possible but to handle data revisions such as GDP revisions. 

## Why {timeseriesdb} ?

{timeseriesdb}  ... 

- is lite weight but powerful. 
- built entirely based on license cost free open source components, mainly the R Language for Statistical Computing and PostgreSQL
- tailored to the needs of Official and Economic Statistics, it manages 
  - different versions (vintages) of a time series. 
  - comprehensive, multi-lingual data description (meta data)
  - access rights as granular as on time series level (Row Level Security (RLS))
  - multiple administration roles and access levels in such a way that roles and access can easily be extended 
- API ready. That is {timeseriesdb} can easily be extendend to expose a REST API to allow for language agnostic access to the data
- easy to set up, well documented and developer friendly to extend. Being a CRAN R package, installation is a one liner. Once the R package is installed it helps to set up the database part. 
- ships with a toolbox to set up a sandbox DB using a standard docker PostgreSQL image.
- users can compose user-specific collections of time series, 
similar to playlist in a music player. 


## What Does {timeseriesdb} NOT DO ?  

{timeseriesdb} is not built to incrementally append new observations as fast as possible. {timeseriesdb} does not try to compete with the amazing speed of InfluxDB or timescale. 


## Example Use (Basic Usage)

The following examples illustrate basic use in a nutshell. 
The learn more about the use of {timeseriesdb}, visit its {pkgdown} documentation page and read the vignette articles.

### Store a List of R Time Series Objects to the Database

```
# Create DB connection. 
# In this case to a local db running on port 111
# w/o pw -- strongly discouraged for production. 
con <- dbConnect(Postgres(),
                "postgres", "localhost",
                 1111, "",
                "postgres")
tsl <- list(ts1 = ts(rnorm(100), frequency = 12,
                     start = 2002),
            ts2 = ts(rnorm(100), frequency = 12,
                     start = 2001))
db_store_ts(connection, tsl)
dbDisconnect(con)
```

### Read Data into a list of R time Series object

```
con <- dbConnect(Postgres(),
                "postgres", "localhost",
                 1111, "",
                "postgres")

tsl <- db_read_ts(connection, c("some_ts_id","another_ts_id"))
dbDisconnect(con)
```


## Installation 

### The R Package

First install the {timeseriesdb} R package which is as straight forward as for any other R package

from CRAN:

```
install.packages("timeseriesdb")
```

from github (latest developer version):

```
library(devtools)
install_github("mbannert/timeseriesdb")

```

### The Database Backend

PostgreSQL is a license costs free, open source Relational Database Management System (DBMS). [Download PostgreSQL](https://www.postgresql.org/download/) from the official website and follow installation instructions for your Operating System. 

Choose one of the following options to run your database backend.

1. Install an instance of PostgreSQL locally on your machine.

2. Just install a PostgreSQL client and connect to a remote PostgreSQL instance, running on, e.g., your IT department's infrastructure, AWS, GPC or any other cloud. 

3. Just install a PostgreSQL client and connect to a local docker container based on a basic PostgreSQL (>= PG 11) image.

Options 1 & 2 are valid production options while option 3 is for testing and development purposes. If you need help getting one of the first two options to go, please check the installation guides refererred to above. 

We will describe option 3 briefly in the following paragraphs. 
Note that you will have a slight advantages running this option when you just clone the git repository from mbannert/timeseriesdb as opposed to just installing the R package. Start the cloned repo as an R Studio project to benefit from the setup scripts inst/sql without looking for them in the installation path. 

Download Docker Desktop and make sure it is running. Switch 
to your command line (terminal, powershell etc.) and pull
a standard PostgreSQL image from Docker Hub.

```
docker pull postgresql-latest
```

Now run 

```
docker stop timeseriesdb_dev
docker run --rm -d -p 1111:5432 --name timeseriesdb_dev  postgres:11
sleep 2
```

This will stop a container with the name *timeseriesdb_dev* if one was running. Then a container running PostgreSQL 11 will be started. This container will be automatically removed once stopped. We map your local port 1111 to the containers 
port 5432 which is the standard port for running a PostgreSQL service. 

Now let's run some SQL scripts on that docker container.
Make sure to you are in the inst/ folder when running these. 

```
psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_extensions.sql
psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_tables.sql
psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_functions.sql
psql -p 1111 -h 'localhost' -d postgres -U postgres -f sql/create_triggers.sql
```

This will install the PostgreSQL extensions *uuid-ossp* and 
*btree_gist* that are necessary to run timeseriesdb. The next scripts create several tables and functions which are the heart of {timeseriesdb}. Finally some triggers are created. Check the developer documentation if you want to learn more about the inner workings of {timeseriesdb}.

## Details

For a detailed documentation, check this pkgdown documentation.
The documentation contains a get-started-guide, a function reference and developer documentation. 












