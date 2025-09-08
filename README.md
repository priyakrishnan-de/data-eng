# # data-eng
This is repo where data engineering learning project code is stored.

## **.ssh**
Sample config file to ssh VM instance in GCP

_config_

## **terraform**
Sample terraform config file to spin up Google cloud bucket and Bigquery instance

*main.tf*

*variables.tf*


## **Python basic exercises**
1. Count the number of each elements in a List

_/python/count-elements.py_

2. Compare 2 lists and print common elements

_/python/compare-lists.py_

3. Combine 2 lists

_/python/combine-lists.py_

4. Split a string of a sentence into lists of 2 words. Input: "Today is a good day to learn Python". Output: [[Today, is], [a, good], [day, to], [learn, Python]]

_/python/split-sen.py_


# **Avito context Dataset - Data Engineering Project in Local**

**Step 1. Ingest raw data**
This file loads 10 files from local folder to postgres tables running in local directly in python (no airflow) except trainsearchstream. File names are mapped to table names and data is inserted in respective tables in a for loop. Each of the tables are created if they do not exist and data is inserted in the most efficient way based on file size:
There are three methods of loading data used in this file - 1) small csv files 2) large TSV files 3) large CSV files

_/python/avito-context/avito-ingestdata.py_

**Step 2.Simulate trainsearchstream data**
This file simulates data for trainsearchstream from testseacrhstream using python (no airflow). Both tables have same structure except additional column of "isClick" in trainseachstream (as original 7z file for trainsearchsteam is corrupted, used this script to load data). Logic followed is "Isclick" is null when objecttype is 3, else it could be 0 or 1 based on Kaggle instructions. Randomly around 5% of records inserted with isclick as 1. 

_/python/avito-context/avito-simulate-trainsearchstream.py_

Now all 11 tables are loaded (All raw data ingested).

**Step 3. Establish connectivity from airflow to postgres**
Establish connection from local airflow to local postgres instance, with postgres running outside docker
In order for Airflow instance to connect to local postgres (running outside docker), host machine IP need to be used in Airflow "Connections" record. Instead of IP, host.docker.internal can be mentioned in connection record.
Also, pg_hba.conf should have an entry which mentions IP of host machine (instead of localhost which is not recognized by docker).  
Note: Did not use postgres instance inside docker for now - will attempt using docker postgres instance after completing this project end to end in local postgres as it is easier to navigate.

_/airflow/dags/postgres_conn_test.py_ - uses Airflow connection ID
_/airflow/dags/postgres_local_test.py_ - Directly includes credentials, instead of IP - host.docker.internal is mentioned

**Step 4. Producer - Continous simulation of additional data for trainsearchstream**
Airflow - DAG created to continously produce Ad clicks & searches ie "trainsearchstream" table with objecttype/isclick logic followed. This inserts records in batches of 10 when DAG is active as per DAG schedule interval.

_/airflow/dags/producer-simulate.py_

**Step 5. Consumer extract to CSV - Continous extraction of new records since last run to local in CSV format**
Airflow - DAG to continously extract data created in "trainsearchstream" since the last run ie simulated data/delta alone is extracted. This is done by having an csv extract marker table where the last extracted id is stored. 
Data is extracted in CSV file with timestamp and file is stored in "tmp/csv_timestamp.csv" within the Airflow worker docker container.

_/airflow/dags/consumer-extract-lastrun.py_

The files can be viewed with this command:

`Docker exec –it <containerid> bash`

To copy these files from docker container to local, this command can be used:

`docker cp <workerconatinerid>:/tmp ./airflow_tmp`

**Step 5. BRONZE Layer / Staging - Raw data of delta records**
Airflow - DAG to continously extract data created in "trainsearchstream" since the last run and insert them into another staging table "trainsearchstream_staging" with basic transformations such as de-duplication, cleansing and typecasting before inserting the records. This is considered as Bronze layer. 
This uses "staging_extract marker" table which has a row for every delta run. 

_/airflow/dags/load_delta_staging.py_

**Step 6. SILVER Layer - Joins and Enrichment**
Aiflow - DAG to continously extract data in Bronze /staging table "trainsearchstream_staging" and insert them into another staging table "trainsearchstream_silver", delta is extracted based on max id already in silver table. 
Silver layer load query is available in Word doc:
[Avito Data Model Description_Silver_Gold layer queries.docx](https://github.com/priyakrishnan-de/data-eng/blob/main/Avito%20Data%20Model%20Description_Silver_Gold%20layer%20queries.docx)

Enrichment:  new columns High_ctr and ad_type defined based on business logic. High_ctr = True if Histctr > 0.5, ad_type is  1: 'regular-free', 2: 'regular-highlighted', 3: 'contextual-payperclick' based on object type 1,2,3

_/airflow/dags/load_delta_silver.py_

**Step 7. GOLD Layer - Aggregrations based on use cases**
Airflow - DAG to continously aggregate data from silver layer and join with few other tables as necessary to create aggregate/summary tables in gold layer. This layer is not optimized and more use cases were arrived and done for practice.
All business use cases for arriving at gold layer tables/views along with queries are available in word doc:
[Avito Data Model Description_Silver_Gold layer queries.docx](https://github.com/priyakrishnan-de/data-eng/blob/main/Avito%20Data%20Model%20Description_Silver_Gold%20layer%20queries.docx)


# **Avito context project - Data Engineering Project in GCP**

**Step 1. Creation of basic resources required in GCP**
Resources created on need basis before each step.
Initially, bucket was created, then VM followed by Cloud SQL, Cloud Composer, Dataflow, Dataproc whenever needed, just before execution.

**Step 2. Move datasets from local to GCP**
The datasets were first uploaded to Buckets using glcoud command. Google cloud SDK was installed in local and authenticated.

GCP Resources used: GCS Bucket, 

**Step 3. Ingest raw data****
This step utilizes airflow DAG running within Cloud composer to ingest 8 large datasets from GCS bucket into Postgresql in cloud SQL. 
DAG's were independently called in paralle as they were not dependent on each other.

_gcp_dag_avito-ingestrawdata_

Old files which were not efficient while running in GCP Cloud composer/Airflow:

_gcp_dag_avito-ingestrawdata_1
gcp_dag_avito-ingestrawdata_2_


**_Connectivity:_**

Required private connection between Cloud SQL and Cloud composer. For Cloud SQL, both private IP and public IP was enabled.
Cloud composer connects to Cloud sql using private IP of Cloud SQL.
Both Cloud SQL and Cloud composer are attached to same "default" network and "default" subnetwork for this to work.


**Step 3. Simulation of TrainSearchStream from TestSearchStream - One time**

This step of inserting simulated data into TrainSearchStream  (one time run) from TestSearchStream along with Including new column "IsClick" was done using local airflow to test the connectivity from local airflow to GCP Postgresql.
Two DAG's - one to create tables if they dont exist followed by insertion of new records in sequence.

_gcp-localairflow-simulate-trainsearchstream.py_

**_Connectivity:_**

This required public IP of Cloud SQL to be specified in Airflow connection record in local airflow.
Also, IP address of local was added to the allowed network in Cloud SQL instance under Network configuration.

This requires ingress firewall rule to allow connections to Cloud SQL Instance (destination - Public IP of Cloud SQL).


**Step 4. Event based pipelines with Cloud Run Function**

**4A. Continous simulation / insertion of data into TrainSearchStream through producer/**

This step of continously simulating new data into TrainSearchStream was done using local airflow DAG connecting to Cloud SQL Postgres.

_gcp-localairflow-producer-trainsearchstream_

**4B. Cloud run function (Dockerised Cloud function)/**

Following files were created to create an application for build followed by run.

_Dockerfile

main.py

requirements.txt_

Once the build and run is completed, Service URL is provided as an output of deploying the conatiner.
https://<cloud run service name>-<projectnumber>.<region>.run.app


Once the service is availble, Scheduler is created in order to schedule the service as per required frequency.

This identifies the newly inserted records (delta) and extracts them to Parquet format and stores in GCS.

**Connectivity:**

Private connection was established between Cloud Run function and Cloud SQL through Serverless VPC Access.

Serverless VPC network was updated with unallocated subnetwork (10.x.x.x) from which IP's will be allocated. 

Cloud Run function was also attached to the same netwrok "default" as Cloud SQL.

This connector name needs to be provided in run time whle executing the python file.










