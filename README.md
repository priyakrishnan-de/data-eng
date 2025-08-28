# data-eng
This is repo where data engineering learning project code is stored.

**.ssh**
config
Has configs to ssh to VM instances in GCP. Stopped these VM instances for now, so IP's are incorrect now. 

**terraform**
Terraform to spin up Google cloud bucket and Bigquery instance
_main.tf
variables.tf_

**Python exercises (Sreenu)**
1. Count the number of each elements in a List
_count-elements.py_

2. Compare 2 lists and print common elements
_compare-lists.py_

3. Combine 2 lists
_combine-lists.py_

4. Split a string of a sentence into lists of 2 words. Input: "Today is a good day to learn Python". Output: [[Today, is], [a, good], [day, to], [learn, Python]]
_split-sen.py_

   
**Avito context Project in Local**

Instead of using only GCP capabilities as advised in confluence exercise page, I'm doing this in local first using python and local airflow.

Step 1. This file loads 10 files from local folder to postgres tables running in local directly in python (no airflow) except trainsearchstream. File names are mapped to table names and data is inserted in respective tables in a for loop. Each of the tables are created if they do not exist and data is inserted in the most efficient way based on file size:
There are three methods of loading data used in this file - 1) small csv files 2) large TSV files 3) large CSV files

_avito-context/avito-ingestdata.py_

Step 2. This file simulates data for trainsearchstream from testseacrhstream using python (no airflow). Both tables have same structure except additional column of "isClick" in trainseachstream (as original 7z file for trainsearchsteam is corrupted, used this script to load data). Logic followed is "Isclick" is null when objecttype is 3, else it could be 0 or 1 based on Kaggle instructions. Randomly around 5% of records inserted with isclick as 1. 

_avito-context/avito-simulate-trainsearchstream.py_

Now all 11 tables are loaded (All raw data ingested).

Step 3. Establish connection from local airflow to local postgres instance, with postgres running outside docker
In order for Airflow instance to connect to local postgres (running outside docker), host machine IP need to be used in Airflow "Connections" record. Instead of IP, host.docker.internal can be mentioned in connection record.
Also, pg_hba.conf should have an entry which mentions IP of host machine (instead of localhost which is not recognized by docker).  
Note: Did not use postgres instance inside docker for now - will attempt using docker postgres instance after completing this project end to end in local postgres as it is easier to navigate.

_postgres_conn_test.py_ - uses Airflow connection ID
_postgres_local_test.py_ - Directly includes credentials, instead of IP - host.docker.internal is mentioned

Step 3. Airflow - DAG created to continously produce Ad clicks & searches ie "trainsearchstream" table with objecttype/isclick logic followed. This inserts records in batches of 10 every 2 minutes when DAG is active.

_producer-simulate.py_

Step 4. Airflow - DAG to continously extract data created in "trainsearchstream" since the last run ie simulated data/delta alone is extracted. This is done by having an extract marker table where the last extracted id is stored. For the first run, one record was inserted in this table with max(id) from trainsearchstream before new simulated data was inserted.  
Data is extracted in CSV file with timestamp and file is stored in "tmp/csv_timestamp.csv" within the Airflow worker docker conatiner.

_consumer-extract-lastrun.py_

The files can be viewed with this command:
_Docker exec –it <containerid> bash_

To copy these files from docker container to local, this command can be used:
_docker cp <workerconatinerid>:/tmp ./airflow_tmp_

(Due to volume mounting issues, did not further change mount volumes to write directly to local folder for now).

Step 5. Airflow - DAG to continously extract data created in "trainsearchstream" since the last run and insert them into another staging table "trainsearchstream_staging". This is basically having same logic as previous step, except that the delta is inserted into staging table for further processing. This also uses "extract marker" table. Before running this for first time, only 1 record was retained in extract marker table in order to get all simulated records from "trainsearchstream".

_trainsearchstream_delta_staging.py_
