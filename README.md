timeseriesdb
============

Store and organize a large amount of low frequency time series data. The package was designed to manage a large catalog of official statistics which are typically published on monthly, quarterly or yearly basis. Thus timeseriesdb is optimized to handle a large number of lower frequency time series as opposed to a smaller amount of high frequency time series such as real time data from measuring devices. Hence timeseriesdb provides the opportunity to store extensive multi-lingual meta information. The package also provides a web GUI to explore the underlying PostgreSQL database interactively.

## Installation Notes

### R stable version
The stable version of the \pkg{timeseriesdb} \proglang{R} package itself can be downloaded and installed from CRAN (\proglang{R}'s official repository). The package source as well as binaries for Windows an OS X are available from CRAN. The package can be installed following \proglang{R}'s standard procedure to install packages eithe by running:

```{r, eval = FALSE}
install.packages("timeseriesdb")
```

or using \proglang{R}'s GUI. 

### R developer version
The developer version of \pkg{timeseriesdb} can be obtained from github.com/mbannert/timeseriesdb. The most convenient way to install the latest developer version from inside an \proglang{R} session is to use the \pkg{devtools} package [@devtools]:

```{r, eval = FALSE}
library(devtools)
install_github('mbannert/timeseriesb')
```

### PostgreSQL
However, because \pkg{timeseriesdb} depends on \pkg{RPostgreSQL} to connect to the \proglang{PostgreSQL} database, the user needs to make sure that the \proglang{PostgreSQL}'s own library and header files are present and can be found by \pkg{RPostgreSQL}. For Windows, this library called libpq is attached to the \pkg{RPostgreSQL} package and will thus be installed with the \proglang{R} package. Hence Windows should make sure \pkg{RPostgreSQL} and should not experience further troubles.

For OS X and Linux the installation is a bit different when libpq is not present. For some Linux distributions the corresponding library can be obtained with the postgresql-devel package. Similarly on OS X, the user needs to make sure that libpq is present and can be found by \pkg{RpostgreSQL}. It is recommend to use the \pkg{homebrew} package manager running `brew install postgresql`. OS X and Linux users should note that previously installed versions may not contain the libraries provided by postgresql-devel package. 


## Database

### Database setup
If you do not have a PostgreSQL database that contains a timeseries schema that suits timeseriesdb, 
create a schema called timeseries and run setup.sql which is located in inst/sql of your package folder. 
Start a psql client console from the inst/sql directory and run: 

```
\i setup.sql
```
If a you are not familiar with running a PostgreSQL console, copy and paste the content of that file to the SQL window of your favorite GUI tool, e.g. PGadmin and run it. 

#### Hstore Extension
hstore is an extension to PostgreSQL that needs to be loaded once before you can start creating tables that contain hstore data type. 
Simply run
```
CREATE EXTENSION hstore;
```
to do so. Also make sure to add hstore to your users search path. 

### CASCADE ON DELETE
By default PostgreSQL tables don't cascade on delete. That means if you delete a row in a main table you could not do so unless the corresponding row in a table referenced by foreign key is delete before. This happens for good reasons. But in the case of unlocalized meta information you want to delete unlocalized meta information when your original series is deleted. That's why the latest version of the createTable.sql uses cascade on DELETE.You can drop this constraint by dropping the foreign key and adding a new one. However, adding cascade on delete after creating the table works like: 

```
alter table meta_data_unlocalized add constraint meta_data_unlocalized_fkey foreign key(fid) references timeseries_main (ts_key) on delete cascade
```





