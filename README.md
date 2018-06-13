# This repository is a fork of the following:

# Spark ETL Sample Project

The goal of this project is to perform an extract, transform and load (ETL) process to migrate data into a local Apache Spark cluster.

* Language: **Python**
* Technologies: **Spark, GPG Encryption**

# ETL Process
1. Decrypt local GPG-encrypted CSV files
2. Load CSV tabular data into Spark DataFrames
3. Save DF data to Parquet files
4. Write query to determine average age
5. Write query to determine age at the 75th percentile

# Approach
1. Fork this repo
2. Ask questions to get clarification
3. Install Apache Spark
4. Write code in Python using data files and GPG keys stored in this repo
5. Commit code to your repo and share link

# This fork of the repository now has:
1. A jupyter file to interactively do the above and get the answers
2. It also has a get_age_avg.scala script that can be run using spark-shell and does just the latter SQL work
3. The above script can be run automatically in an ETL process along with the following:
4. It also has a python file that can be used in a more production-like manner to implement ETL
5. If the python script is used with the scala script, only the first part of the python script should be used.
