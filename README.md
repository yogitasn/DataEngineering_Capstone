# DataEngineering_Capstone

## Project Summary
This project creates a data lake on Amazon Web Services with main focus on building a data warehouse and data pipeline. The data lake is built around the I94 Immigration data provided by the US government. The data warehouse will help US official to analyze immigration patterns to understand what factors drive people to move and also can be used for tourism purpose.
 
## Project Scopes
The scope of this project will be to build a data ware house on AWS that will help answer common business questions as well as powering dashboards. To do that, a conceptual data model and a data pipeline will be defined.

## Data sources
* I94 Immigration Data: This data comes from the US National Tourism and Trade Office.
* I94 Data dictionary: Dictionary accompanies the I94 Immigration Data
* U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it here.
* Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from here.


## ETL Flow
* Data Collected from the API is moved to landing zone s3 buckets.
* ETL job has s3 module which copies data from landing zone to working zone.
* Once the data is moved to working zone, spark job is triggered which reads the data from working zone and apply transformation. Dataset is repartitioned and moved to the    Processed Zone.
* Warehouse module of ETL jobs picks up data from processed zone and stages it into the Redshift staging tables.
* Using the Redshift staging tables and UPSERT operation is performed on the Data Warehouse tables to update the dataset.
* ETL job execution is completed once the Data Warehouse is updated.
* Airflow DAG runs the data quality check on all Warehouse tables once the ETL job execution is completed.
* Airflow DAG has Analytics queries configured in a Custom Designed Operator. These queries are run and again a Data Quality Check is done on some selected Analytics Table.
* Dag execution completes after these Data Quality check.


## Environment Setup
Hardware Used
EMR - I used a 3 node cluster with below Instance Types:
