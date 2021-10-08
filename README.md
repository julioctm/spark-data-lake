# Data Lake Project

## **Project description:**
This project intends to attend a needance of the analytics team, 
of the Sparkfy Company, that is a music streaming startup,
moving their Data Warehouse to a Data Lake cloud based with AWS, based on the 
JSON log files they collected from songs and users activities,
as well as a directory with JSON metadata on the songs in their app.
The main objective of this project is to build an ETL pipeline that
extracts data from a S3 bucket, stages them in Spark with schema-on-read
in order to transform this data, and finally saves the results as parquet
files in other S3 bucket.

The ETL pipeline was built as follow:
First the data is extracted from JSON files using Spark.
Then the data is transformed with schema-on-read.
Finally the data is loaded to another S3 bucket as parquet files.

## **Using this Pipeline**
Before start running, is necessary to fill the file dl.cfg with the information
of the IAM.
To run this pipeline, the only thing to do is to run the etl.py, that will extract
the data from JSON files, which are in the S3 bucket, using Spark.
After that, it will start to transform some data and save it to another S3 bucket
as parquet files.

```bash
python etl.py
```

## Files in this repository:

### dl.cfg:
A config file that holds the parameters of the cluster, IAM and S3 bucket.

### etl.py:
Responsible for the ETL pipeline execution.