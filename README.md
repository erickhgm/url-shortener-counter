# url-shortener-counter

This is a small Dataflow Job that receives a message via pubsub every time someone accesses a shortened URL. It accumulates the items using a Fixed Time Window, groups by Id and updates FireStore with the amount of clicks.

The technology behind it: 
* [Python 3.8](https://www.python.org/)
* [Apache Beam](https://beam.apache.org/)
* [Google Firestore](https://cloud.google.com/firestore)
* [Google PubSub](https://cloud.google.com/pubsub)
* [Dataflow](https://cloud.google.com/dataflow)

## Installing / Getting started

### **Create the environment using `Virtual Environments`**

In the terminal run the following command:
```console
python -m venv url-shortener-counter
source tutorial-env/bin/activate
pip install -r requirements.txt
``` 

### **Create the environment using `Anaconda`**

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
