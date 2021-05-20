
## Project Description

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we aim build an ETL pipeline for a data lake hosted on S3. The ETL pipeline will extract Sparkify's JSON logs on user activity from S3, process them using Spark, and will load the data back into S3 as a set of dimensional tables.

The Spark process is deployed on a cluster using AWS.

## Design & Deployment

The input files are located under these paths;
 
**Song data:** s3://udacity-dend/song_data
**Log data:** s3://udacity-dend/log_data

**Database Model**

**Fact Table**
**songplays** - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
**users** - users in the app
user_id, first_name, last_name, gender, level
**songs** - songs in music database 
song_id, title, artist_id, year, duration
**artists** - artists in music database
artist_id, name, location, lattitude, longitude
**time** - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

The ETL flow will have two main functions;
**process_song_data():**  Will read the song files, extract and write songs & artists data. Songs data is partitioned by year and artist_id values.
**process_log_data():** Will read the log files, extract and write users & time data. Then will join songs data that is extracted from the previous step and write songplays. Time and songplays data are partitioned by year and month values.

These five tables are written to parquet files in a separate analytics directory on S3. 

**output_data** = "s3://celebis/sparkify-output/"

Each table has its own folder within the directory. Duplicates are removed while extracting data.

**dl.cfgcontains** the AWS credentials
Access Key ID: ****
Secret Access Key: *****

From the terminal **> python etl.py** command must be run. The user must make sure that necessary rights are granted in order to write under the S3 bucket.
