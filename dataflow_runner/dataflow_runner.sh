#!/bin/bash
DATAFLOW_JOB_NAME_GCS=etl-flights
GCP_PROJECT_ID=trustly-data-services-test
GCP_REGION=europe-west1
DATAFLOW_STAGING_LOCATION=gs://trustly-data-services-test-dataflow-temp-sysint/etl-flights-staging/
DATAFLOW_TEMP_LOCATION=gs://trustly-data-services-test-dataflow-temp-sysint/etl-flights-temp/
GCP_SUBNETWORK=regions/europe-west1/subnetworks/dataflow-subnetwork
DATAFLOW_SERVICE_ACCOUNT=svc-dataflow-worker@trustly-data-services-test.iam.gserviceaccount.com

GITHUB_SHA=test
cleaned_github_ref=test

# set gcloud account to svc account: svc-dataflow-worker
gcloud config set account $DATAFLOW_SERVICE_ACCOUNT
gcloud config get-value  account

# ADD INPUT DATA TO GCS
GCS_DATA=gs://trustly-data-services-test-dataflow-temp-sysint/etl-flights-data/
gsutil cp -r input_data $GCS_DATA

# OUTPUT FILE TO RETRIVED
GCS_OUTPUT_FILE=gs://trustly-data-services-test-dataflow-temp-sysint/etl-flights-data/output_data/dataflow_output.csv

python3 etl_flight_data/beam_pipeline.py \
    --setup_file ./setup.py \
    --runner=DataflowRunner \
    --max_num_workers=1 \
    --project=$GCP_PROJECT_ID \
    --region=$GCP_REGION \
    --subnetwork=$GCP_SUBNETWORK \
    --service_account_email=$DATAFLOW_SERVICE_ACCOUNT \
    --job_name=$DATAFLOW_JOB_NAME_GCS \
    --staging_location=$DATAFLOW_STAGING_LOCATION \
    --temp_location=$DATAFLOW_TEMP_LOCATION \
    --labels={"commit_id"=$GITHUB_SHA,"github_ref"=$cleaned_github_ref} \
    --output_file=$GCS_OUTPUT_FILE \
    --airports_file="${GCS_DATA}input_data/airports.dat" \
    --routes_file="${GCS_DATA}input_data/routes.dat" \

# gsutil cp -r $GCS_OUTPUT_FILE .

# set gcloud account to personal
gcloud config set account mario.loera@trustly.com
gcloud config get-value  account
