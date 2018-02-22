timeseriesdb - Manage Official Statistics' Time Series Data with R and PostgreSQL
============

*timeseriesdb* is PostgreSQL based database schema to store time series and a corresponding R mapper package. The package was designed to map R time series objects to records in PostgreSQL relations. **timeseriesdb** aims at time series from official statistics which are typically published on a monthly, quarterly or yearly basis. Thus **timeseriesdb** is optimized to handle updates caused by data revisions. Further **timeseriesdb**'s schema allows to store extensive, context aware, multi-lingual meta-information.  


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

The latest version of the **timeseriesdb** is available from [my github account](https://github.com/mbannert/timeseriesdb). The **devtools** package is a easy way to directly install packages directly from github. 


```{r, eval = FALSE}
library(devtools)
install_github('mbannert/timeseriesdb')
```


### Set up PostgreSQL








### PostgreSQL


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





