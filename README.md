# easyparquet
A rust learning project for database reading, parallelism, and concurrency. Backs up mysql tables to local parquet files.

## Usage
This workspace consists of two crates: easyparquet for the database reading and parquet backup, as well as the Rustbank crate, which simulates a backup service for bank accounts (which depends on easyparquet.) 

### Rustbank
To run the rustbank crate, execute the rustbank binary (via cargo run or the binary itself.) It requires the following environment variables:

* database_uri: The exact uri for the specific mysql database. This takes the form of mysql://user:password@host:port/database_name
* backup_directory: The directory to save the .parquet files to

A dataset has been provided in the data subdirectory of rustbank. It consists of two csvs (one for the users table and one for accounts), each with 1000 datapoints. The rustbank tool can upload data to the database, back up the database to local .parquet files, and also generate datasets (with a specified number of rows.)

#### Commands
Execute the binary for exact command syntax.

* generate: Generates sample users and accounts tables using the rust fake crate for a specific directory and number of entries. These are saved as .csv files to the specified directory. The directory must already exist.
* upload: Uploads a given data directory to the database specified by the database_uri environment variable. This reads from the csvs (users.csv and accounts.csv) in the specified directory. Note that this does not clear the existing database tables; this is to facilitate uploading of non-sample data.
* backup: Backs up the users and accounts table from the specified database_uri and saves them to backup_directory as users.parquet and accounts.parquet respectively. The backup_directory must already exist.

### Easyparquet
Easyparquet is a crate used to programatically back up mysql tables. This is intended to be used as a library, but a binary is provided for either manual testing or quick backups. For the binary, a database and list of tables can be specified with environment variables to back up tables from any database without requiring rust code (so long as the column types are supported.) The following environment variables control the binary, execute the binary to perform a quick backup.

* database_uri: The exact uri of the database. Takes the form mysql://user:password@host:port/database_name
* backup_directory: The directory where backed up .parquet files will be stored. This must exist before the binary is run or it will result in errors.
* database_tables: a semicolon-separated list of strings where each is the name of a table. For example, table_names = "users;accounts"

## Notes

* Not all mysql datatypes are currently supported. The supported types are Varchar, Int, Bigint, Decimal(19, 2), Float, Date, and DateTime.
* An example dataset is provided for the rustbank application. This data was generated using the rust fake crate.