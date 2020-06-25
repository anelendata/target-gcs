# target_gcs

Read in stdin and write out to Google Cloud Storage.

## Example usage

### Install

```
python3 -m venv ./venv
source ./venv/bin/activate
```

Then

```
pip install https://github.com/anelendata/target_gcs/tarball/master
```
Or
```
git clone git@github.com:anelendata/target_gcs.git
pip install -e target_gcs
```

### Configure

[Sample configuration file](./sample_config.json)

Note: As in the sample, you can use the following parameters in the blob name:

- etl_datetime (ISO 8601 format)
- etl_tstamp (unix time stamp)

Set the path to [Google Cloud API's application credential JSON file](https://cloud.google.com/docs/authentication/getting-started):

```
export GOOGLE_APPLICATION_CREDENTIALS=./path_to/your_cred_file.json
```

### Test

Make sure your service account associated with the crendential file has
sufficient [GCS permissions](https://cloud.google.com/storage/docs/access-control/iam).
If the bucket specified in the config does not exist, target_gcs tries to create one.
In this case, the account needs Storage Admin. Otherwise, Object Createor at minimum.

```
echo -e '{"line": 1, "value": "hello"}\n{"line": 2, "value": "world"}' | target_gcs -c ./your-config.json
```

Here is the example to get USGS earthquake events data:

```
curl "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2020-06-24&endtime=2020-06-25" | target_gcs -c ./your-config.json
```

## Extra: Creating a schemaless, externally partitioned BigQuery table from GCS files

```
git clone git@github.com:anelendata/target_gcs.git
cd target_gcs
pip install google-cloud-bigquery
```

```
python create_schemaless_table.py -p your-project-id -g gs://your-bucket/your-dataset -d your-dataset-name -t your-table-name
```

Note: dataset must exist.

---

Copyright &copy; 2020 Anelen Co, LLC
