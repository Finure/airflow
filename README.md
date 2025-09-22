# Finure Airflow repo

## 1. What it is

This repository provides a custom Apache Airflow image for the Finure platform, containing DAGs for orchestrating data validation and ETL pipelines. The main DAG (`data_pipeline.py`) automates the process of downloading CSV data from Google Cloud Storage, validating and cleaning the data, uploading results back to GCS, and notifying downstream systems, Argo Events & Slack. This Airflow image is intended to be deployed in a Kubernetes cluster as part of the Finure infrastructure, and requires cluster/cloud resources to function.

## 2. Features
- **Custom Airflow Image:** Dockerfile and requirements.txt for building a custom Airflow image
- **Data Validation & Cleaning:** DAG validates and cleans credit application CSVs from GCS, enforcing schema and value rules
- **ETL Automation:** Automates download, validation, cleaning, and upload of datasets to GCS
- **Rejects Reporting:** Generates a rejects report for invalid rows and uploads to GCS
- **Argo & Slack Notifications:** Notifies Argo workflows and Slack channels with validation results and file locations
- **Extensible:** Easily add new DAGs or Python dependencies as needed
- **Kubernetes Ready:** Deployed via a Kubernetes cluster as part of the Finure platform

## 3. Prerequisites
- Kubernetes cluster bootstrapped ([Finure Terraform](https://github.com/finure/terraform))
- Infrastructure setup via Flux ([Finure Kubernetes](https://github.com/finure/kubernetes))

If running locally for development/testing:
- Docker 
- Python 3.12+
- Airflow CLI 

## 4. File Structure
```
airflow/
├── Dockerfile           # Custom Airflow image build file
├── requirements.txt     # Python dependencies for Airflow
├── dags/
│   └── data_pipeline.py # DAG for data pipeline
├── .airflowignore       # Airflow ignore rules
├── .gitignore           # Git ignore rules
├── README.md            # Project documentation
```

## 5. How to Run Manually

> **Note:** Manual execution is for development/testing only. Production use is via Kubernetes deployment.

1. Build the custom Airflow image:
	```bash
	docker build -t finure/airflow .
	```
2. Run Airflow locally:
	```bash
	docker run -d -p 8080:8080 finure/airflow
	```
	The Airflow web UI will be available at `http://localhost:8080`
3. Add or modify DAGs in the `dags/` folder as needed


## Additional Information

This repo is primarily designed to be used in the Finure project. While the dags can be adapted for other use cases, it is recommended to use it as part of the Finure platform for full functionality and support.