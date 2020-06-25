import argparse, datetime, json, logging, os
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions


try:
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--project', help='GCP project ID', required=True)
    parser.add_argument('-g', '--gcs_uri', help='Cloud Storage URI (gs://xxx/yyy', required=True)
    parser.add_argument('-d', '--dataset', help='BigQuery dataset name', required=True)
    parser.add_argument('-t', '--table', help='BigQuery table name', required=True)
    args = parser.parse_args()
except ImportError:
    args = None

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = logging.getLogger()


def create_bq_table(project, gcs_uri, dataset, table):
    bq_client = bigquery.Client(project=project)
    table_ref = bq_client.dataset(dataset).table(table)
    table = bigquery.Table(table_ref)

    hive_partition_options = HivePartitioningOptions()
    hive_partition_options.mode = "AUTO"
    hive_partition_options.source_uri_prefix = gcs_uri

    extconfig = bigquery.ExternalConfig('CSV')
    extconfig.schema = [bigquery.SchemaField('line', 'STRING')]
    extconfig.options.field_delimiter = u'\u00ff'
    extconfig.options.quote_character = ''
#   extconfig.compression = 'GZIP'
    extconfig.options.allow_jagged_rows = False
    extconfig.options.allow_quoted_newlines = False
    extconfig.max_bad_records = 10000000
    extconfig.source_uris=[os.path.join(gcs_uri, "*")]
    extconfig.hive_partitioning = hive_partition_options

    table.external_data_configuration = extconfig

    bq_client.create_table(table)


def main():
    create_bq_table(args.project, args.gcs_uri, args.dataset, args.table)


if __name__ == "__main__":
    main()
