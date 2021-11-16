# url-shortener-counter

This is a small Dataflow Job that receives a message via pubsub every time someone accesses a shortened URL. It accumulates the items using a Fixed Time Window, groups by Id and updates FireStore with the amount of clicks.

The technology behind it: 
* [Python 3.8](https://www.python.org/)
* [Apache Beam](https://beam.apache.org/)
* [Google Firestore](https://cloud.google.com/firestore)
* [Google PubSub](https://cloud.google.com/pubsub)
* [Dataflow](https://cloud.google.com/dataflow)

## Pipeline

![Pipeline](doc/pipeline.jpg?raw=true "Pipeline")

## Installing / Getting started

### **Creating the environment using `Virtual Environments`**

In the terminal run the following command:
```console
python -m venv url-shortener-counter
source tutorial-env/bin/activate
pip install -r requirements.txt
``` 

### **Creating the environment using `Anaconda`**

In the terminal run the following command:
```console
conda env create --file=environment.yml
source activate url-shortener-counter
``` 

## Running on the local machine

Set environment variables:
```console
export PROJECT_ID=[your_projec_id]
export GOOGLE_APPLICATION_CREDENTIALS=/temp/url-counter-handy-service-account.json
```

In the terminal run the following command:
```console
python main.py \
--project_id=${PROJECT_ID} \
--input_subscription=projects/${PROJECT_ID}/subscriptions/url-clicks-counter \
--collection=urls \
--fixed_windows=60
```

## Running on GCP Dataflow

Template generation:
```console
python main.py \
--runner=DataflowRunner \
--project=${PROJECT_ID} \
--staging_location=gs://${GCP_BUCKET}/staging \
--temp_location=gs://${GCP_BUCKET}/temp \
--template_location=gs://${GCP_BUCKET}/templates/url-shortener-counter-1.0.0 \
--region=southamerica-east1 \
--project_id=${PROJECT_ID} \
--input_subscription=projects/${PROJECT_ID}/subscriptions/url-clicks-counter \
--collection=urls \
--fixed_windows=60 \
--requirements_file=requirements-dataflow.txt
```

Run the template on Dataflow:

See the documentation [Running classic templates](https://cloud.google.com/dataflow/docs/guides/templates/running-templates)

## Job on Dialogflow

![dialigflow job](doc/dialigflow-job.jpg?raw=true "dialigflow job")
