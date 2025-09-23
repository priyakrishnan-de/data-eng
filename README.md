# # data-eng

This is repo where data engineering learning project code is stored.


## **Excel with the Steps followed**

Excel with the steps and commands for using basic GitBash commands, docker, local airflow, gcloud and terraform basics.

**Reference Sheet:**

https://docs.google.com/spreadsheets/d/1a-i5N9MtxRWyhSBlCvmqUK_TV9Ea-E_V/edit?usp=sharing&ouid=117556559172603166026&rtpof=true&sd=true



## **Data Model description, SQL Queries and Analysis on Gold Layer**

1. All business use cases including Use Case categories and use cases for arriving at gold layer tables/views are available in word doc:

[Avito Data Model Description and Gold Layer Use cases](https://docs.google.com/document/d/1Wmr29XFnuO2jOzSeWZmGXSAP_c3udd4Y/edit?usp=drive_link&ouid=117556559172603166026&rtpof=true&sd=true)


2. All queries for creating gold layer tables are views are available in "sql-local" folder and all files start with "load_gold_layer". 

_sql-local/load_gold_layer$$<Use-Case-Category-Name>$$-$$<Use-Case-Name>$$.sql_

Here, "Use Case Category" and "Use Case Name" stand for the 24 use cases for gold layer which is available in word doc.


## **Local - Avito context project**

Steps for local project is over here:

https://github.com/priyakrishnan-de/data-eng?tab=readme-ov-file#avito-context-dataset---data-engineering-project-in-local


## **GCP - Avito context project**

Steps for Avito project in GCP is here:

https://github.com/priyakrishnan-de/data-eng/blob/main/gcp/readme.md


Sheet "GCP" has the steps and commands followed in GCP.
https://docs.google.com/spreadsheets/d/1a-i5N9MtxRWyhSBlCvmqUK_TV9Ea-E_V/edit?usp=sharing&ouid=117556559172603166026&rtpof=true&sd=true


## **Azure - Avito context project**

Steps for Avito project in Azure is here:

https://github.com/priyakrishnan-de/data-eng/blob/main/azure/readme.md


Sheet "Azure" has the steps and commands followed in Azure.
https://docs.google.com/spreadsheets/d/1a-i5N9MtxRWyhSBlCvmqUK_TV9Ea-E_V/edit?usp=sharing&ouid=117556559172603166026&rtpof=true&sd=true


## **SSH**
Sample config file to ssh VM instance in GCP

_.ssh/config_


## **Terraform**
Sample terraform config file to spin up Google cloud bucket and Bigquery instance

_terraform/main.tf_

_terraform/variables.tf_



## **Python basic exercises**
1. Count the number of each elements in a List

_python/count-elements.py_

2. Compare 2 lists and print common elements

_python/compare-lists.py_

3. Combine 2 lists

_python/combine-lists.py_

4. Split a string of a sentence into lists of 2 words. Input: "Today is a good day to learn Python". Output: [[Today, is], [a, good], [day, to], [learn, Python]]

_python/split-sen.py_



# **Avito context Dataset - Data Engineering Project in Local**

_**Ingest raw data**_

This file loads 10 files from local folder to postgres tables running in local directly in python (no airflow) except trainsearchstream. File names are mapped to table names and data is inserted in respective tables in a for loop. Each of the tables are created if they do not exist and data is inserted in the most efficient way based on file size:
There are three methods of loading data used in this file - 1) small csv files 2) large TSV files 3) large CSV files

_python/avito-context/avito-ingestdata.py_

_**Simulate trainsearchstream data**_

This file simulates data for trainsearchstream from testseacrhstream using python (no airflow). Both tables have same structure except additional column of "isClick" in trainseachstream (as original 7z file for trainsearchsteam is corrupted, used this script to load data). Logic followed is "Isclick" is null when objecttype is 3, else it could be 0 or 1 based on Kaggle instructions. Randomly around 5% of records inserted with isclick as 1. 

_python/avito-context/avito-simulate-trainsearchstream.py_

Now all 11 tables are loaded (All raw data ingested).

_**Establish connectivity from airflow to postgres**_

Establish connection from local airflow to local postgres instance, with postgres running outside docker
In order for Airflow instance to connect to local postgres (running outside docker), host machine IP need to be used in Airflow "Connections" record. Instead of IP, host.docker.internal can be mentioned in connection record.
Also, pg_hba.conf should have an entry which mentions IP of host machine (instead of localhost which is not recognized by docker).  
Note: Did not use postgres instance inside docker for now - will attempt using docker postgres instance after completing this project end to end in local postgres as it is easier to navigate.

_airflow/dags/postgres_conn_test.py_ - uses Airflow connection ID

_airflow/dags/postgres_local_test.py_ - Directly includes credentials, instead of IP - host.docker.internal is mentioned

_**Producer - Continous simulation of additional data for trainsearchstream**_

Airflow - DAG created to continously produce Ad clicks & searches ie "trainsearchstream" table with objecttype/isclick logic followed. This inserts records in batches of 10 when DAG is active as per DAG schedule interval.

_airflow/dags/producer-simulate.py_

_**Consumer extract to CSV - Continous extraction of new records since last run to local in CSV format**_

Airflow - DAG to continously extract data created in "trainsearchstream" since the last run ie simulated data/delta alone is extracted. This is done by having an csv extract marker table where the last extracted id is stored. 
Data is extracted in CSV file with timestamp and file is stored in "tmp/csv_timestamp.csv" within the Airflow worker docker container.

_airflow/dags/consumer-extract-lastrun.py_

The files can be viewed with this command:

`Docker exec –it <containerid> bash`

To copy these files from docker container to local, this command can be used:

`docker cp <workerconatinerid>:/tmp ./airflow_tmp`

_**BRONZE Layer / Staging - Raw data of delta records to Postgres and Local path**_

Airflow - DAG to continously extract data created in "trainsearchstream" since the last run and insert them into another staging table "trainsearchstream_staging" with basic transformations such as de-duplication, cleansing and typecasting before inserting the records. This is considered as Bronze layer. 
This uses "staging_extract marker" table which has a row for every delta run. 

_airflow/dags/load_delta_staging.py_


_**SILVER Layer - Joins and Enrichment**_

Aiflow - DAG to continously extract data in Bronze /staging table "trainsearchstream_staging" and insert them into another staging table "trainsearchstream_silver", delta is extracted based on max id already in silver table. 

Silver layer load query is available here:

_sql-local/load_delta_silver.py_


Enrichment:  new columns High_ctr and ad_type defined based on business logic. High_ctr = True if Histctr > 0.5, ad_type is  1: 'regular-free', 2: 'regular-highlighted', 3: 'contextual-payperclick' based on object type 1,2,3

_airflow/dags/load_delta_silver.py_

_**GOLD Layer - Aggregrations based on use cases**_

Airflow - DAG to continously aggregate data from silver layer and join with few other tables as necessary to create aggregate/summary tables in gold layer. This layer is not optimized and more use cases were arrived and done for practice.

_airflow/dags/load_gold_layer.py_