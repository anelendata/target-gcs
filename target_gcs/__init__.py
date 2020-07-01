import argparse, datetime, io, json, logging, sys

from oauth2client import tools

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
from google.cloud import storage


try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    flags = parser.parse_args()

except ImportError:
    flags = None

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = logging.getLogger()


class GCSObjectStreamUpload(object):
    """
    adopted from: https://dev.to/sethmlarson/python-data-streaming-to-google-cloud-storage-with-resumable-uploads-458h
    """
    def __init__(
            self,
            client: storage.Client,
            bucket_name: str,
            blob_name: str,
            chunk_size: int=256 * 1024
        ):
        self._client = client
        self._bucket = self._client.bucket(bucket_name)
        self._blob = self._bucket.blob(blob_name)

        self._buffer = b''
        self._buffer_size = 0
        self._chunk_size = chunk_size
        self._read = 0

        self._transport = AuthorizedSession(
            credentials=self._client._credentials
        )
        self._request = None  # type: requests.ResumableUpload

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.stop()

    def start(self):
        url = (
            f'https://www.googleapis.com/upload/storage/v1/b/'
            f'{self._bucket.name}/o?uploadType=resumable'
        )
        self._request = requests.ResumableUpload(
            upload_url=url, chunk_size=self._chunk_size
        )
        self._request.initiate(
            transport=self._transport,
            content_type='application/octet-stream',
            stream=self,
            stream_final=False,
            metadata={'name': self._blob.name},
        )

    def stop(self):
        self._request.transmit_next_chunk(self._transport)

    def write(self, data: bytes) -> int:
        data_len = len(data)
        self._buffer_size += data_len
        self._buffer += data
        del data
        while self._buffer_size >= self._chunk_size:
            try:
                self._request.transmit_next_chunk(self._transport)
            except common.InvalidResponse:
                self._request.recover(self._transport)
        return data_len

    def read(self, chunk_size: int) -> bytes:
        # I'm not good with efficient no-copy buffering so if this is
        # wrong or there's a better way to do this let me know! :-)
        to_read = min(chunk_size, self._buffer_size)
        memview = memoryview(self._buffer)
        self._buffer = memview[to_read:].tobytes()
        self._read += to_read
        self._buffer_size -= to_read
        return memview[:to_read].tobytes()

    def tell(self) -> int:
        return self._read


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def process_singer_format(lines, stream, on_invalid_record="force", encoding="utf-8"):
    if on_invalid_record != "force":
        from jsonschema import validate
        from jsonschema.exceptions import ValidationError
    import singer

    state = None

    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception("A record for stream {} was encountered before a corresponding schema".format(msg.stream))

            schema = schemas[msg.stream]

            if on_invalid_record != "force":
                validate(msg.record, schema)

            record_str = json.dumps(msg.record) + "\n"
            stream.write(record_str.encode(encoding))
            rows[msg.stream] += 1

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug('Setting state to {}'.format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            rows[table] = 0
            errors[table] = None

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in errors.keys():
        if not errors[table]:
            logger.info('Loaded {} row(s)'.format(rows[table]))
        else:
            logger.info('Errors:', errors[table], sep=" ")

    return state


def process_general(lines, stream):
    for line in lines:
        stream.write(line.encode(encoding))


def main():
    with open(flags.config) as input:
        config = json.load(input)

    project_id = config["project_id"]
    bucket_name = config["bucket"]
    encoding = config.get("encoding", "utf-8")
    etl_datetime = datetime.datetime.utcnow()

    params = {
        "etl_datetime": etl_datetime.isoformat(),
        "etl_tstamp": etl_datetime.timestamp(),
    }
    blob_name = config["blob"].format(**params)

    logger.info("Writing to " + bucket_name + "/" + blob_name)

    client = storage.Client(project=project_id)
    try:
        bucket = client.get_bucket(bucket_name)
    except Exception as e:
        print(e)
        bucket = client.create_bucket(bucket_name)
    input = io.TextIOWrapper(sys.stdin.buffer, encoding=encoding)


    state = None
    with GCSObjectStreamUpload(client=client, bucket_name=bucket_name, blob_name=blob_name) as s:
        if config.get("singer_format", False):
            state = process_singer_format(input, s, encoding=encoding)
        else:
            process_general(input, s, encoding=encoding)

    if state:
        emit_state(state)


if __name__ == "__main__":
    main()
