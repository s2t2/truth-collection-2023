# truth-collection-2023

## Setup

Obtain a username and password for the truth social network (i.e. `TRUTH_USERNAME` and `TRUTH_PASSWORD`).

Create a Google Cloud project and BigQuery database (i.e. `DATASET_ADDRESS`). Also obtain service account credentials and download the resulting JSON file into this repo as "google-credentials.json" (which has been ignored from version control).

Setup local ".env" file with credentials:

```sh
TRUTH_USERNAME="_______"
TRUTH_PASSWORD="_______"

# BigQuery Credentials:
GOOGLE_APPLICATION_CREDENTIALS="/Users/path/to/truth-collection-2023/google-credentials.json"
DATASET_ADDRESS="your-project.truth_2023_development"
```

Setup local environment:

```sh
conda create -n truth-env python=3.10
conda activate truth-env
```

Install packages:

```sh
pip install -r requirements.txt
```

## Usage

Connect to BigQuery:

```sh
python -m app.bq_service
```

Connect to the social network:

```sh
python -m app.truth_service
```

### Timeline Collection

First migrate timeline statuses table:

```sh
DESTRUCTIVE=false python -m app.bq_migrate.timeline_statuses
```

Collect timeline statuses for a given user:

```sh
python -m app.bq_collect.timeline_statuses
#COLLECTION_USERNAME="abc123" python -m app.bq_collect.timeline_statuses
```

Collect timeline statuses for all previously collected and mentioned users:

```sh
python -m app.bq_collect.all_timelines
# USERS_LIMIT=5 python -m app.bq_collect.all_timelines
```

```sh
python -m app.bq_collect.all_timelines_threaded
# USERS_LIMIT=5 MAX_THREADS=3 python -m app.bq_collect.all_timelines_threaded
```

## Testing

```sh
pytest
```
