# About 

This repository was made for the "Data Lake" project by Udacity for their Data Engineer program.

------------------------
# Purpose 

In this project, we will be building a Data Lake hosted on EMR for the startup Sparkify.

Sparkifys has a growing user/song base and wants to move their data warehouse solution to a data lake solution.

Sparkifys user, song and metadata data resides in a S3 bucket in a JSON format.
Our goals are to:
- Create a EMR cluster
- Build ETL pipeline that:
	- Extracts Sparkifys data from S3 bucket
	- Loads them in to EMR cluster with spark
	- Transforms data into a set of dimensional tables
	- Load the set of dimensional tables into another S3 bucket

------------------------

# Repository Structure

 - dl.cfg: a configuration that stores IAM credentials.
 - etl.py: Extracts the data from Sparkifys S3 bucket, Loads it into spark, transforms it into dimensionals and loads said dimensionals into another S3 bucket.
 - README.md: this file


------------------------

## Table schema 

![Database Diagram](/Database_Diagram.png "Database Diagram")

### Songplays table
- start_time TIMESTAMP 
- user_id STRING  
- level STRING
- song_id STRING 
- artist_id STRING
- session_id LONG
- location STRING
- user_agent STRING

### Users table
- user_id STRING
- first_name STRING
- last_name STRING
- gender STRING
- level STRING

### Songs table
- song_id STRING
- title STRING
- artist_id STRING
- year LONG
- duration DOUBLE 

### Artists table
- artist_id STRING 
- name STRING
- location STRING
- latitude DOUBLE 
- longitude DOUBLE 

### Time table
- start_time TIMESTAMP 
- hour INTEGER
- day INTEGER
- week INTEGER
- month INTEGER
- year INTEGER
- weekday INTEGER

------------------------

# Requirements
- A EMR cluster
- Python3 with the following packages:
	- pyspark
	- Configparser

------------------------

# How to Run
The EMR cluster should be up before running the project.

Enter IAM credentials inside of dl.cfg

```
python etl.py
```
