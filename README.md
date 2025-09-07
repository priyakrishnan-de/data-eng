# # data-eng
This is repo where data engineering learning project code is stored.

## **.ssh**

config
Sample config file to ssh VM instance in GCP

## **terraform**
Sample terraform config file to spin up Google cloud bucket and Bigquery instance
_main.tf
variables.tf_

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

Step 1. This file loads 10 files from local folder to postgres tables running in local directly in python (no airflow) except trainsearchstream. File names are mapped to table names and data is inserted in respective tables in a for loop. Each of the tables are created if they do not exist and data is inserted in the most efficient way based on file size:
There are three methods of loading data used in this file - 1) small csv files 2) large TSV files 3) large CSV files

_/python/avito-context/avito-ingestdata.py_

Step 2. This file simulates data for trainsearchstream from testseacrhstream using python (no airflow). Both tables have same structure except additional column of "isClick" in trainseachstream (as original 7z file for trainsearchsteam is corrupted, used this script to load data). Logic followed is "Isclick" is null when objecttype is 3, else it could be 0 or 1 based on Kaggle instructions. Randomly around 5% of records inserted with isclick as 1. 

_/python/avito-context/avito-simulate-trainsearchstream.py_

Now all 11 tables are loaded (All raw data ingested).

Step 3. Establish connection from local airflow to local postgres instance, with postgres running outside docker
In order for Airflow instance to connect to local postgres (running outside docker), host machine IP need to be used in Airflow "Connections" record. Instead of IP, host.docker.internal can be mentioned in connection record.
Also, pg_hba.conf should have an entry which mentions IP of host machine (instead of localhost which is not recognized by docker).  
Note: Did not use postgres instance inside docker for now - will attempt using docker postgres instance after completing this project end to end in local postgres as it is easier to navigate.

_/airflow/dags/postgres_conn_test.py_ - uses Airflow connection ID
_/airflow/dags/postgres_local_test.py_ - Directly includes credentials, instead of IP - host.docker.internal is mentioned

Step 3. Airflow - DAG created to continously produce Ad clicks & searches ie "trainsearchstream" table with objecttype/isclick logic followed. This inserts records in batches of 10 when DAG is active as per DAG schedule interval.

_/airflow/dags/producer-simulate.py_

Step 4. Airflow - DAG to continously extract data created in "trainsearchstream" since the last run ie simulated data/delta alone is extracted. This is done by having an csv extract marker table where the last extracted id is stored. 
Data is extracted in CSV file with timestamp and file is stored in "tmp/csv_timestamp.csv" within the Airflow worker docker container.

_/airflow/dags/consumer-extract-lastrun.py_

The files can be viewed with this command:
_Docker exec –it <containerid> bash_

To copy these files from docker container to local, this command can be used:
_docker cp <workerconatinerid>:/tmp ./airflow_tmp_

Step 5. BRONZE Layer - Airflow - DAG to continously extract data created in "trainsearchstream" since the last run and insert them into another staging table "trainsearchstream_staging" with basic transformations such as de-duplication, cleansing and typecasting before inserting the records. This is considered as Bronze layer. 
This uses "staging_extract marker" table which has a row for every delta run. 

_/airflow/dags/load_delta_staging.py_

Step 6. SILVER Layer - Aiflow - DAG to continously extract data in Bronze /staging table "trainsearchstream_staging" and insert them into another staging table "trainsearchstream_silver", delta is extracted based on max id already in silver table. 
Silver layer load query is available in Word doc:
[Avito Data Model Description_Silver_Gold layer queries.docx](https://github.com/priyakrishnan-de/data-eng/blob/main/Avito%20Data%20Model%20Description_Silver_Gold%20layer%20queries.docx)

Enrichment:  new columns High_ctr and ad_type defined based on business logic. High_ctr = True if Histctr > 0.5, ad_type is  1: 'regular-free', 2: 'regular-highlighted', 3: 'contextual-payperclick' based on object type 1,2,3

_/airflow/dags/load_delta_silver.py_

Step 7. GOLD Layer - Airflow - DAG to continously aggregate data from silver layer and join with few other tables as necessary to create aggregate/summary tables in gold layer. This layer is not optimized and more use cases were arrived and done for practice.
All business use cases for arriving at gold layer tables/views along with queries are available in word doc:
[Avito Data Model Description_Silver_Gold layer queries.docx](https://github.com/priyakrishnan-de/data-eng/blob/main/Avito%20Data%20Model%20Description_Silver_Gold%20layer%20queries.docx)


# **Avito context project - Data Engineering Project in GCP**


