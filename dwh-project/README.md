# Sparkify Data Warehouse


## Contents
* [About](#about)
* [File Contents](#file-contents)
  * [File Descriptions](#file-descriptions)
  * [Running the Project](#running-the-project)
* [Database Structure](#database-structure)

## About
This project contains code to load data from event log files and song info files
into a data warehouse hosted on [AWS Redshift](https://aws.amazon.com/redshift/).
The objective is to perform analytics queries to study songplay data.

The data is to be loaded from JSON files stored in S3 into staging tables on Redshift
and then structured and loaded into the final tables, which will be organized in a star
schema.

## File Contents

### File Descriptions
- The `sql_queries.py` file contains the queries necessary to create the tables in the
  database, load the data into these tables, and drop the tables when done.
  
- The `create_tables.py` script will drop the tables and then re-create them using the 
  queries defined in `sql_queries.py` when run.
  
- The `etl.py` script's purpose is to extract the data from S3 and load it into the 
  appropriate tables. This must be run **after** the tables have been created.
  
- The `make_cluster.py` script is a utility for quickly starting and deleting a Redshift
  cluster. To use it, first fill the DB name, user and password in the `dwh.cfg` file, then
  make an appropriate security group in your AWS `us-west-2` VPC and paste the id in the
  `dwh.cfg` file. Finally, run the script with
  ```bash
  python make_cluster.py <action>
  ```
  replacing `<action>` with `create` or `delete`. After creating the cluster, the 
  endpoint and the IAM role ARN are stored in a `cluster_meta.json` file.
  
### Running the Project
In order to run the scripts and create the data warehouse, there are first some preliminary
steps you must follow: First, copy the `dwh.cfg.example` file to a new `dwh.cfg` file and
fill in the missing variables with your specific values. This requires you to prepare an
AWS security group ahead of time (this could be done more cleanly within the scripts but
this will work for now). 

Then you must prepare your redshift cluster either by launching it yourself or using the
`make_cluster.py` script. Now you are ready to create and populate the database.

For this last step run the `create_tables.py` script to prepare the tables and then run
the `etl.py` script to extract the data from S3 and loat it into the tables.

## Database Structure

The database is set up as a star schema for analytics. Since the songplays are the main
object the analytics team wants to study, they are set as the main fact table
(`songplays`). The songplays in turn can be studied along the following dimension tables:

- `users` contains information about the user that played the song.
  
- `songs` contains information about the song itself, such as the year it was made and 
  its duration.
  
- `artists` contains information on the artist that made the song, including their name
  and location.
  
- `time` has details related to the time the song was played.

Additionally, there are two staging tables: `staging_events` and `staging_songs`. These
tables are used as an intermediate step between extracting the JSON data and loading it
into the final tables listed above, and as such they should not be considered as part of
the final database.


[Back to top.](#sparkify-data-warehouse)