# Sparkify Data Lake

## Contents

* [About](#about)
* [Repository Contents](#repository-contents)
* [Output Data Organization](#output-data-organization)
* [Running the Project](#running-the-project)

## About
The purpose of this project is to create an etl pipeline using
[Spark](https://spark.apache.org/) in order to prepare raw song and log data
for analytics. The raw data in JSON format will be read from S3, processed on a
Spark cluster and the results will be stored back to S3 as Parquet files. The
processed data will be organized as a star schema to facilitate analytics.

## Repository Contents

- The `etl.py` contains the code to perform the ETL process and generate the desired
  output files. It can be run from the terminal or submitted to a Spark cluster.
  
- The `setup_util` package is a utility to quickly set up or take down an EMR cluster.
  To use it, run
  ```bash
  python -m setup_util [create|delete]
  ```
  Running with `create` option will spin up the cluster, save some metadata in a
  `cluster_meta.json` file and generate a `connection.bash` script which you can use
  to ssh into the master node.

- The `cfg-templates` directory contains templates for the `dl.cfg` file to use either locally
  or on an AWS EMR cluster.

## Output Data Organization

The output data is organized following a star schema. Since the main focus of the analytics
group is songplays, the `songplays` table is the fact table. This fact table can be analyzed 
along the following dimension tables:
- `users`
- `songs`
- `artists`
- `time`

Each table is stored in a directory of the same name in the specified output location (which
could be in the local filesystem, on S3, or even on HDFS) as a set of parquet files.

Since the `songplays` and `time` tables are likely to be the largest and are likely to be joined 
and filtered on often, they are partitioned by year and month for storage. Additionally, the
`songs` table is partitioned by year and artist for storage.

## Running the Project


[Back to top](#sparkify-data-lake)
