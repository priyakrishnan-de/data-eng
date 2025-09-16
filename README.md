# # data-eng
This is repo where data engineering learning project code is stored.

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

All business use cases including Use Case categories and use cases for arriving at gold layer tables/views are available in word doc:

[Avito Data Model Description and Gold Layer Use cases](https://docs.google.com/document/d/1Wmr29XFnuO2jOzSeWZmGXSAP_c3udd4Y/edit?usp=drive_link&ouid=117556559172603166026&rtpof=true&sd=true)

All queries for creating 24 gold layer tables are views are available in "sql-local" folder and all files start with "load_gold_layer":

_sql-local/load_gold_layer$$<Use-Case-Category-Name>$$-$$<Use-Case-Name>$$.sql_

Use case categories and use cases available in word doc.


# **Avito context project - Data Engineering Project in GCP**

All steps and commands followed in creation of resources and execution of pipelines/ services are documented here for reference (work in progress):
[GCP Reference sheet](https://docs.google.com/spreadsheets/d/1a-i5N9MtxRWyhSBlCvmqUK_TV9Ea-E_V/edit?usp=sharing&ouid=117556559172603166026&rtpof=true&sd=true)

This sheet also has references on using GitBash, using docker, local airflow, gcloud and terraform basics.


_**Step 1. Creation of basic resources required in GCP**_

Resources created on need basis before each step.
Initially, bucket was created, then VM followed by Cloud SQL, Cloud Composer, Dataflow, Dataproc whenever needed, just before execution.

_**Step 2. Move datasets from local to GCP**_

The datasets were first uploaded from local to **GCS bucket** using glcoud command. Google cloud SDK was installed in local and authenticated.


_**Step 3. Ingest raw data**_

This step utilizes airflow DAG running within **Cloud composer** to ingest 8 large datasets from GCS bucket into Postgresql in **Cloud SQL**. 
DAG's were independently called in paralle as they were not dependent on each other.

_airflow/dags/gcp_dag_avito-ingestrawdata_

Old files which were not efficient while running large TSV files in GCP Cloud composer/Airflow:

_airflow/dags/gcp_dag_avito-ingestrawdata_1_

_airflow/dags/gcp_dag_avito-ingestrawdata_2_


**Connectivity:**

Required private connection between Cloud SQL and Cloud composer. For Cloud SQL, both private IP and public IP was enabled.
Cloud composer connects to Cloud sql using private IP of Cloud SQL.
Both Cloud SQL and Cloud composer are attached to same "default" network and "default" subnetwork for this to work.



_**Step 4A. One time Simulation of TrainSearchStream from TestSearchStream**_

This step of inserting simulated data into TrainSearchStream  (one time run) from TestSearchStream along with Including new column "IsClick" (based on ObjectType) was done using local airflow to test the connectivity from local airflow to GCP Postgresql.
Two DAG's - one to create tables if they dont exist followed by insertion of new records in sequence.

_airflow/dags/gcp-localairflow-simulate-trainsearchstream.py_

**Connectivity:**

This required public IP of Cloud SQL to be specified in Airflow connection record in local airflow.
Also, IP address of local was added to the allowed network in Cloud SQL instance under Network configuration.

This requires ingress firewall rule to allow connections to Cloud SQL Instance (destination - Public IP of Cloud SQL).


_**Step 4B. Continous simulation of data into TrainSearchStream through producer in BRONZE layer**_

This step of continously simulating new data into TrainSearchStream was done using local airflow DAG connecting to Cloud SQL Postgres. 

_airflow/dags/gcp-localairflow-producer-trainsearchstream_


_**Step 4C. BRONZE Layer base data - Identify delta from TrainSearchStream and insert into TrainSearchStream_Staging**_

Airflow - DAG to continously extract data created in "trainsearchstream" since the last run and insert them into another staging table "trainsearchstream_staging" with basic transformations such as de-duplication, cleansing and typecasting before inserting the records. This is considered as Bronze layer. 
This uses "staging_extract marker" table which has a row for every delta run. 

_airflow/dags/load_delta_staging.py_


_**Step 5. Event driven data moevment with Cloud Run Function (dockerised Cloud function) and Cloud Scheduler**_

Instead of Cloud function, went with Cloud run which packages the service into a container.

Following files were created to create an application for build followed by run using **Cloud run** and **Cloud Scheduler**.

_cloudrun/Dockerfile_

_cloudrun/main.py_

_cloudrun/requirements.txt_

Once the build and run is completed, Service URL is provided as an output of deploying the container.

**https://{cloudrun-service-name}-<<projectnumber>>.<<region>>.run.app**

Once the service is availble, Scheduler is created in order to schedule the service as per required frequency.

This identifies the newly inserted records (delta) and extracts them to Parquet format and stores in GCS.

Cloud Scheduler for Data export of delta records into GCS:

_trigger-auto-data-export_


**Connectivity:**

Private connection was established between Cloud Run function and Cloud SQL through Serverless VPC Access.

Serverless VPC network was updated with unallocated subnetwork (10.x.x.x) from which IP's will be allocated. 

Cloud Run function was also attached to the same network "default" as Cloud SQL.

This connector name needs to be provided in run time whle executing the python file.



_**Step 6. ETL Pipeline using Google dataflow**_

This dataflow pipeline identifies delta and moves the records in Avro format from source (using SQL command) to the specified target (sink) with path provided for staging, temp and templates. Transformations such as de-duplication, null handling and typecast done.

_dataflow/delta-to-csv-pipeline.py_

_dataflow/setup.py_


Name of the Dataflow pipeline: 

_dataflow/avitodelta-cloud-csv_

**Connectivity:**

Required Cloud SQL Proxy which was done through VM.

From VM, connectivity was established to Cloud SQL through private network.



_**Step 7. Data Transformation with Datproc and PySpark into SILVER LAYER Cloud SQL**_

1. Created Dataproc cluster with command, GCP creates default service account automatically. Ensure this GCP service account is provided "DatProc Editor" role.
Also, ensure current user running this command in Powershell has "DataProc Editor" role. Atleast should have "DataProc Job User" + "DataProc Job Viewer".


`gcloud dataproc clusters create avito-pyspark-cluster \ --region=asia-east1 \ --zone=asia-east1-a \ --network=default \ --enable-component-gateway \ --master-machine-type=n1-standard-4 \ --worker-machine-type=n1-standard-4 \ --num-workers=2 \ --image-version=2.1-debian11`


2. Create pyspark file with the logic and setup.py file.

   _dataflow/dataproc_bronze_to_silver.py_

   Has logic to left join brnoze table with SearchInfo and AdsInfo and also with category and location table. New columns for high_ctr was added for enrinchment.
   This wil replace the current data in silver table.

   _dataflow/setup.py_

   Has dependencies for pandas, numpy

4. Downloaded the latest JDBC driver "postgresql-42.7.6.jar " and uploaded it to the default bucket created by dataproc. The pasth for JAR has to be specified while running the pyspark job.


5. Run the pyspark job using this command to test the pyspark. Have to add cluster (using existing cluster), region, path to setup file. Also added properties for hearbeatinterval and timeout due to the long time it takes to run this job. Database variables are hardcoded in the job.

   `gcloud dataproc jobs submit pyspark \
    --cluster="avito-pyspark-cluster" \
    --region="asia-east1" \
    --py-files=./setup.py \
    --jars=gs://dataproc-staging-asia-east1-<ProjectNumber>-rrk2ibyt/postgresql-42.7.6.jar \
    ./dataproc_bronze_to_silver.py \
    --properties=spark.executor.heartbeatInterval=300000ms,spark.network.timeout=600000ms`


**Schedule the DataProc PySpark with Workflow Template and Cloud scheduler**

1. Create a workflow Template, same region and cluster (if any), else default cluster will be created and destroyed.

`gcloud dataproc workflow-templates create avito-silver-wf  --region=asia-east1`

2. Copy the two py files to a bucket from current folder/directory

Sample Data-Proc Bucket: gs://dataproc-temp-asia-east1-<ProjectNumber>-ggyw9m6j/

`gcloud storage cp ./dataproc_bronze_to_silver.py gs://<<Data-Proc-bucket>>/pyspark/dataproc_bronze_to_silver.py`

`gcloud storage cp ./setup.py gs://<<Data-Proc-bucket>>/pyspark/setup.py`

3. Add Pyspark job step. In addition to earlier parameters, include new ones for workflow template and step-id for Scheduler.
   
`gcloud dataproc workflow-templates add-job pyspark gs://<<Data-Proc-Bucket>>/pyspark/dataproc_bronze_to_silver.py \
  --step-id=bronze-to-silver-step \
  --workflow-template=avito-silver-wf \
  --region=asia-east1 \
  --py-files=gs://<<Data-Proc-Bucket>>/pyspark/setup.py \
  --jars=gs:///<<Data-Proc-Bucket>>/postgresql-42.7.6.jar \
  --properties=spark.executor.heartbeatInterval=300000ms,spark.network.timeout=600000ms`


4. Ensure to provide default compute Service account these roles first

_Data Proc Worker_

_Storage Object Viewer_

5. Service Accunts > Go to the Compute service account

Click Grant Access > Select user who is running the command and provide "Service Account Token Creator" against Service Account for impersonation.

6. Enable API for impersonation

gcloud services enable iamcredentials.googleapis.com

7. Set cluster for Workflow template, if not done while creation. (reusing existing cluster).

`gcloud dataproc workflow-templates set-managed-cluster avito-silver-wf \
  --region=asia-east1 \
  --cluster-name=bronze-to-silver-temp \
  --num-workers=2 \
  --master-machine-type=n1-standard-4 \
  --worker-machine-type=n1-standard-4`


8. Restart this workflow any time, need to use Service account to run it from powershell.

`gcloud dataproc workflow-templates instantiate avito-silver-wf \
  --region=asia-east1 \
  --impersonate-service-account=<ProjectNumber>-compute@developer.gserviceaccount.com \`


9. Set up Cloud Scheduler to run on a scheduled basis

    `gcloud scheduler jobs create http bronze-to-silver-job \
  --schedule="0 2 * * *" \
  --uri="https://dataproc.googleapis.com/v1/projects/<ProjectID>/regions/asia-east1/workflowTemplates/avito-silver-wf:instantiate" \
  --http-method=POST \
  --oidc-service-account-email=<ProjectNumber>-compute@developer.gserviceaccount.com  \
  --oidc-token-audience="https://dataproc.googleapis.com/" \
  --time-zone="Asia/Kolkata" \
  --location=asia-east1`


10. After the job is scheduled, can be verified by following commands.

  View All scheduled Jobs

  `gcloud scheduler jobs list --location=asia-east1 --project=<ProjectID>`

  View details of a specific job

  `gcloud scheduler jobs describe bronze-to-silver-job \
  --location=asia-east1 \
  --project=<ProjectID>`

  
   
11. To run the scheduler manully for testing from Powershell.

    `gcloud scheduler jobs run bronze-to-silver-job \
  --location=asia-east1 \
  --project=<ProjectID>`
   

Following are setup in GCP, refer commands in excel.

GCP Workflow for Silver: 

_avito-silver-wf_

GCP Workflow Step ID (just a step name given under workflow):

_bronze_to_silver_step_

GCP Cloud Scheduler for Gold:

_bronze-to-silver_job_


_**Step 8. Aggregation tables created in GOLD LAYER in Cloud SQL + Gold layer data stored as CSV files in GCS Bucket**_


Follow same steps as for Step 7.

1. pyspark file for Gold layer performs both the actions of aggregationg tables for gold layer in Cloud SQL as well as storing the same data in GCS Bucket with time stamp.

_dataproc/dataproc_silver_gold_csv.py_


2. pyspark file which only loads tables into Gold Layer SQL Table (does not write to GCS Bucket)

_dataproc/dataproc_silver_gold.py_


Following are setup in GCP, refer commands in excel.

GCP Workflow for Gold: 

_avito-gold-wf_

GCP Workflow Step ID (just a step name given under workflow):

_silver_to_gold_step_

GCP Cloud Scheduler for Gold:

_silver-to-gold_job_


_**Step 9. Medallion Architecture**_

Summary of layers, these are already in previous steps.

_Raw data_: All tables ingested

_One time Simulated data_: TrainSearchstream (from TestSearchstream)

Continous Simulation of 10 new records every 10 minutes into TrainSearchstream through Cloud Composer/Airflow

_BRONZE Layer_: New records inserted into "TrainSearchstream" is moved to **"TrainSearchStream_Staging"** continously every 30 minutes through Cloud Composer/Airflow. 
This table along with rest of the RAW DATA tables (except TestSearchStream/TrainSearchStream) considered as BRONZE layer.
CSV of records from each run are stored in GCS Bucket.

_SILVER Layer_: "TrainSeacrchStream_staging" is joined with "SearchInfo", "AdsInfo", "Location", "Category",  new columns added to load to **"TrainSearchstream_Silver"**.
Data is truncated and inseretd every time. This is done through Cloud scheduler job which calls Dataproc Workflow template.

_GOLD Layer_: "TrainSearchStream_Silver" is aggregated and based on use case, data from "PhoneSearchStream", "VisitsStream" is also aggregated to store in various gold tables or gold views as applicable. 
Data is truncated and inseretd every time. This is done through Cloud scheduler job which calls Dataproc Workflow template.
In addition, CSV of records from each run are stored in GCS Bucket in format  _TableName_<timestamp>.csv_

All gold layer tables and views can be viewed in this doc:

[Avito Data Model Description and Gold Layer Use cases](https://docs.google.com/document/d/1Wmr29XFnuO2jOzSeWZmGXSAP_c3udd4Y/edit?usp=drive_link&ouid=117556559172603166026&rtpof=true&sd=true)



_**Step 10.Data Visualization with Google Data Studio / Looker**_

Due to issues in enabling Looker Studio in region asia-east1, data was pushed to BigQuery alone for now.


**Pushing datasets from Cloud SQL to BigQuery:**



