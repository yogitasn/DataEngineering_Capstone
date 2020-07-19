# DataEngineering_Capstone

### Project Summary
This project creates a data lake on Amazon Web Services with main focus on building a data warehouse and data pipeline. The data lake is built around the I94 Immigration data provided by the US government. The data warehouse will help US official to analyze immigration patterns to understand what factors drive people to move and also can be used for tourism purpose.
 
### Project Scopes
The scope of this project will be to build a data ware house on AWS that will help answer common business questions as well as powering dashboards. To do that, a conceptual data model and a data pipeline will be defined.

### Data sources
* I94 Immigration Data: This data comes from the [US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html).
* I94 Data dictionary: Dictionary accompanies the I94 Immigration Data
* U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
* Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).


### Architecture
Data are uploaded to Amazon S3. AWS will act as the data lake where all raw files are stored. Data will then be loaded to staging tables on Amazon Redshift. The ETL process will take data from those staging tables and create data mart tables. An Airflow instance can be deployed on a Google Compute Engine or locally to orchestrate the pipeline.

Here are the justifications for the technologies used:

* Amazon S3: act as the data lake, vertically scalable.
* Amazon Redshift: act as data base engine for data warehousing, data mart and ETL processes. BigQuery is a serverless solution that can easily and effectively process petabytes scale dataset.
* Apache Airflow: orchestrate the workflow by issuing command line to load data to BigQuery or SQL queries for ETL process. Airflow does not have to process any data by itself, thus allowing the architecture to scale.

### ETL Flow
* Data is copied to s3 buckets.
* Once the data is moved to S3 bucket, spark job is triggered which reads the data from working zone and apply transformation. Dataset is repartitioned and moved to the    Processed Zone.
* Airflow ETL job picks up data from processed zone and stages it into the Redshift staging tables.
* Using the Redshift staging tables and UPSERT operation is performed on the Data Warehouse tables to update the dataset.
* ETL job execution is completed once the Data Warehouse is updated.
* Airflow DAG runs the data quality check on all Warehouse tables once the ETL job execution is completed.
* Airflow DAG has Analytics queries configured in a Custom Designed Operator. These queries are run and again a Data Quality Check is done on some selected Analytics Table.
* Dag execution completes after these Data Quality check.

### Environment Setup
Hardware Used
EMR - I used a 3 node cluster with below Instance Types:
> m5.xlarge
> 4 vCore, 16 GiB memory, EBS only storage
> EBS Storage:64 GiB

Redshift: For Redshift I used 2 Node cluster with Instance Types dc2.large

Airflow
> Using local airflow workspace 

