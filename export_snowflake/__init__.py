#!/usr/bin/env python3

import argparse
import io
import json
import traceback
import logging
import os
import sys
import copy
import boto3
import time

import multiprocessing
import concurrent.futures

from typing import Dict, List, Optional
from joblib import Parallel, delayed, parallel_backend
from jsonschema import Draft7Validator, FormatChecker
from singer import get_logger
from datetime import datetime, timedelta

from export_snowflake.file_formats import csv
from export_snowflake.file_formats import parquet
from export_snowflake import stream_utils

from export_snowflake.db_sync import DbSync
from export_snowflake.file_format import FileFormatTypes
from export_snowflake.exceptions import (
    RecordValidationException,
    UnexpectedValueTypeException,
    InvalidValidationOperationException,
    SymonException
)

LOGGER = get_logger('export_snowflake')

# Tone down snowflake.connector log noise by only outputting warnings and higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

DEFAULT_BATCH_SIZE = 64 # Default batch size in MB
DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel

# for symon error logging
ERROR_START_MARKER = '[target_error_start]'
ERROR_END_MARKER = '[target_error_end]'

def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {'type': ['null', 'string'],
                                                                            'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = {'type': ['null', 'string'],
                                                                          'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {'type': ['null', 'string']}

    return extended_schema_message


def emit_state(state: Optional[Dict]):
    """Print state to stdout"""
    if state is not None:
        line = json.dumps(state)
        LOGGER.info('Emitting state %s', line)
        sys.stdout.write(f"{line}\n")
        sys.stdout.flush()


def get_snowflake_statics(config):
    """Retrieve common Snowflake items will be used multiple times

    Params:
        config: configuration dictionary

    Returns:
        tuple of retrieved items: table_cache, file_format_type
    """
    db = DbSync(config)
    # The file format is detected at DbSync init time
    file_format_type = db.file_format.file_format_type
    
    return file_format_type


# pylint: disable=too-many-locals,too-many-branches,too-many-statements,invalid-name
def persist_lines(config, lines, table_cache=None, file_format_type: FileFormatTypes = None) -> None:
    """Main loop to read and consume singer messages from stdin

    Params:
        config: configuration dictionary
        lines: iterable of singer messages
        table_cache: Optional dictionary of Snowflake table structures. This is useful to run the less
                     INFORMATION_SCHEMA and SHOW queries as possible.
                     If not provided then an SQL query will be generated at runtime to
                     get all the required information from Snowflake
        file_format_type: Optional FileFormatTypes value that defines which supported file format to use
                          to load data into Snowflake.
                          If not provided then it will be detected automatically

    Returns:
        tuple of retrieved items: table_cache, file_format_type
    """
    state = None
    flushed_state = None
    schemas = {}
    key_properties = {}
    validators = {}
    records_to_load = {}
    row_count = {}
    stream_to_sync = {}
    total_row_count = {}
    current_batch_size = {}
    batch_size = config.get('batch_size', DEFAULT_BATCH_SIZE) * 1024 * 1024
    batch_wait_limit_seconds = config.get('batch_wait_limit_seconds', None)
    flush_timestamp = datetime.utcnow()
    archive_load_files = config.get('archive_load_files', False)
    archive_load_files_data = {}

    # Loop over lines from stdin
    for line in lines:
        line_size = sys.getsizeof(line)
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error('Unable to parse:\n%s', line)
            raise

        if 'type' not in o:
            raise Exception(f"Line is missing required key 'type': {line}")

        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")
            if o['stream'] not in schemas:
                raise Exception(
                    f"A record for stream {o['stream']} was encountered before a corresponding schema")

            # Get schema for this record's stream
            stream = o['stream']

            # Symon: adjust_timesptamps_in_record validates date/datetime/time fields and resets invalid values to max_date. 
            # skipping this for performance optimization + our records are output of computation so dates are valid.
            # stream_utils.adjust_timestamps_in_record(o['record'], schemas[stream])

            # Validate record
            if config.get('validate_records'):
                try:
                    validators[stream].validate(stream_utils.float_to_decimal(o['record']))
                except Exception as ex:
                    if type(ex).__name__ == "InvalidOperation":
                        raise InvalidValidationOperationException(
                            f"Data validation failed and cannot load to destination. RECORD: {o['record']}\n"
                            "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                            "or more) Try removing 'multipleOf' methods from JSON schema.") from ex
                    raise RecordValidationException(f"Record does not pass schema validation. RECORD: {o['record']}") \
                        from ex

            primary_key_string = stream_to_sync[stream].record_primary_key_string(o['record'])
            if not primary_key_string:
                primary_key_string = f'RID-{total_row_count[stream]}'

            if stream not in records_to_load:
                records_to_load[stream] = {}

            # increment row count only when a new PK is encountered in the current batch
            if primary_key_string not in records_to_load[stream]:
                row_count[stream] += 1
                total_row_count[stream] += 1
                current_batch_size[stream] += line_size

            # append record
            if config.get('add_metadata_columns') or config.get('hard_delete'):
                records_to_load[stream][primary_key_string] = stream_utils.add_metadata_values_to_record(o)
            else:
                records_to_load[stream][primary_key_string] = o['record']

            if archive_load_files and stream in archive_load_files_data:
                # Keep track of min and max of the designated column
                stream_archive_load_files_values = archive_load_files_data[stream]
                if 'column' in stream_archive_load_files_values:
                    incremental_key_column_name = stream_archive_load_files_values['column']
                    incremental_key_value = o['record'][incremental_key_column_name]
                    min_value = stream_archive_load_files_values['min']
                    max_value = stream_archive_load_files_values['max']

                    if min_value is None or min_value > incremental_key_value:
                        stream_archive_load_files_values['min'] = incremental_key_value

                    if max_value is None or max_value < incremental_key_value:
                        stream_archive_load_files_values['max'] = incremental_key_value

            flush = False
            if current_batch_size[stream] >= batch_size:
                flush = True
                LOGGER.info("Flush triggered by batch_size (%s) reached in %s",
                            batch_size, stream)
            elif (batch_wait_limit_seconds and
                  datetime.utcnow() >= (flush_timestamp + timedelta(seconds=batch_wait_limit_seconds))):
                flush = True
                LOGGER.info("Flush triggered by batch_wait_limit_seconds (%s)",
                            batch_wait_limit_seconds)

            if flush:
                # flush all streams, delete records if needed, reset counts and then emit current state
                if config.get('flush_all_streams'):
                    filter_streams = None
                else:
                    filter_streams = [stream]

                # Flush and return a new state dict with new positions only for the flushed streams
                flushed_state = flush_streams(
                    records_to_load,
                    row_count,
                    current_batch_size,
                    stream_to_sync,
                    config,
                    state,
                    flushed_state,
                    archive_load_files_data,
                    filter_streams=filter_streams)

                flush_timestamp = datetime.utcnow()

                # emit last encountered state
                emit_state(copy.deepcopy(flushed_state))

        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")

            stream = o['stream']
            new_schema = stream_utils.float_to_decimal(o['schema'])

            # Update and flush only if the the schema is new or different than
            # the previously used version of the schema
            if stream not in schemas or schemas[stream] != new_schema:

                schemas[stream] = new_schema
                validators[stream] = Draft7Validator(schemas[stream], format_checker=FormatChecker())

                # flush records from previous stream SCHEMA
                # if same stream has been encountered again, it means the schema might have been altered
                # so previous records need to be flushed
                if row_count.get(stream, 0) > 0:
                    # flush all streams, delete records if needed, reset counts and then emit current state
                    if config.get('flush_all_streams'):
                        filter_streams = None
                    else:
                        filter_streams = [stream]
                    flushed_state = flush_streams(records_to_load,
                                                  row_count,
                                                  current_batch_size,
                                                  stream_to_sync,
                                                  config,
                                                  state,
                                                  flushed_state,
                                                  archive_load_files_data,
                                                  filter_streams=filter_streams)

                    # emit latest encountered state
                    emit_state(flushed_state)

                # key_properties key must be available in the SCHEMA message.
                if 'key_properties' not in o:
                    raise Exception("key_properties field is required")

                # Log based and Incremental replications on tables with no Primary Key
                # cause duplicates when merging UPDATE events.
                # Stop loading data by default if no Primary Key.
                #
                # If you want to load tables with no Primary Key:
                #  1) Set ` 'primary_key_required': false ` in the export-snowflake config.json
                #  or
                #  2) Use fastsync [postgres-to-snowflake, mysql-to-snowflake, etc.]
                if config.get('primary_key_required', True) and len(o['key_properties']) == 0:
                    LOGGER.critical('Primary key is set to mandatory but not defined in the [%s] stream', stream)
                    raise Exception("key_properties field is required")

                key_properties[stream] = o['key_properties']

                if config.get('add_metadata_columns') or config.get('hard_delete'):
                    stream_to_sync[stream] = DbSync(config,
                                                    add_metadata_columns_to_schema(o),
                                                    table_cache,
                                                    file_format_type)
                else:
                    stream_to_sync[stream] = DbSync(config, o, table_cache, file_format_type)

                if archive_load_files:
                    archive_load_files_data[stream] = {
                        'tap': config.get('tap_id'),
                    }

                    # In case of incremental replication, track min/max of the replication key.
                    # Incremental replication is assumed if o['bookmark_properties'][0] is one of the columns.
                    incremental_key_column_name = stream_utils.get_incremental_key(o)
                    if incremental_key_column_name:
                        LOGGER.info("Using %s as incremental_key_column_name", incremental_key_column_name)
                        archive_load_files_data[stream].update(
                            column=incremental_key_column_name,
                            min=None,
                            max=None
                        )
                    else:
                        LOGGER.warning(
                            "archive_load_files is enabled, but no incremental_key_column_name was found. "
                            "Min/max values will not be added to metadata for stream %s.", stream
                        )

                stream_to_sync[stream].create_schema_if_not_exists()
                stream_to_sync[stream].sync_table()

                row_count[stream] = 0
                total_row_count[stream] = 0
                current_batch_size[stream] = 0

        elif t == 'ACTIVATE_VERSION':
            LOGGER.debug('ACTIVATE_VERSION message')

        elif t == 'STATE':
            LOGGER.debug('Setting state to %s', o['value'])
            state = o['value']

            # # set flushed state if it's not defined or there are no records so far
            if not flushed_state or sum(row_count.values()) == 0:
                flushed_state = copy.deepcopy(state)

        else:
            raise Exception(f"Unknown message type {o['type']} in message {o}")

    # if some bucket has records that need to be flushed but haven't reached batch size
    # then flush all buckets.
    if sum(row_count.values()) > 0:
        # flush all streams one last time, delete records if needed, reset counts and then emit current state
        flushed_state = flush_streams(records_to_load, row_count, current_batch_size, stream_to_sync, config, state, flushed_state,
                                      archive_load_files_data)

    # emit latest state
    emit_state(copy.deepcopy(flushed_state))


# pylint: disable=too-many-arguments
def flush_streams(
        streams,
        row_count,
        current_batch_size,
        stream_to_sync,
        config,
        state,
        flushed_state,
        archive_load_files_data,
        filter_streams=None):
    """
    Flushes all buckets and resets records count to 0 as well as empties records to load list
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param current_batch_size: dictionary with current batch size per stream
    :param stream_to_sync: Snowflake db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :param archive_load_files_data: dictionary of dictionaries containing archive load files data
    :return: State dict with flushed positions
    """
    parallelism = config.get("parallelism", DEFAULT_PARALLELISM)
    max_parallelism = config.get("max_parallelism", DEFAULT_MAX_PARALLELISM)

    # Parallelism 0 means auto parallelism:
    #
    # Auto parallelism trying to flush streams efficiently with auto defined number
    # of threads where the number of threads is the number of streams that need to
    # be loaded but it's not greater than the value of max_parallelism
    if parallelism == 0:
        n_streams_to_flush = len(streams.keys())
        if n_streams_to_flush > max_parallelism:
            parallelism = max_parallelism
        else:
            parallelism = n_streams_to_flush

    # Select the required streams to flush
    if filter_streams:
        streams_to_flush = filter_streams
    else:
        streams_to_flush = streams.keys()

    # Single-host, thread-based parallelism
    with parallel_backend('threading', n_jobs=parallelism):
        Parallel()(delayed(load_stream_batch)(
            stream=stream,
            records=streams[stream],
            row_count=row_count,
            current_batch_size=current_batch_size,
            db_sync=stream_to_sync[stream],
            no_compression=config.get('no_compression'),
            delete_rows=config.get('hard_delete'),
            temp_dir=config.get('temp_dir'),
            archive_load_files=copy.copy(archive_load_files_data.get(stream, None))
        ) for stream in streams_to_flush)

    # reset flushed stream records to empty to avoid flushing same records
    for stream in streams_to_flush:
        streams[stream] = {}

        # Update flushed streams
        if filter_streams:
            # update flushed_state position if we have state information for the stream
            if state is not None and stream in state.get('bookmarks', {}):
                # Create bookmark key if not exists
                if 'bookmarks' not in flushed_state:
                    flushed_state['bookmarks'] = {}
                # Copy the stream bookmark from the latest state
                flushed_state['bookmarks'][stream] = copy.deepcopy(state['bookmarks'][stream])

        # If we flush every bucket use the latest state
        else:
            flushed_state = copy.deepcopy(state)

        if stream in archive_load_files_data:
            archive_load_files_data[stream]['min'] = None
            archive_load_files_data[stream]['max'] = None

    # Return with state message with flushed positions
    return flushed_state


def load_stream_batch(stream, records, row_count, current_batch_size, db_sync, no_compression=False, delete_rows=False,
                      temp_dir=None, archive_load_files=None):
    """Load one batch of the stream into target table"""
    # Load into snowflake
    if row_count[stream] > 0:
        flush_records(stream, records, db_sync, temp_dir, no_compression, archive_load_files)

        # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
        if delete_rows:
            db_sync.delete_rows(stream)

        # reset row count for the current stream
        row_count[stream] = 0
        current_batch_size[stream] = 0


def flush_records(stream: str,
                  records: List[Dict],
                  db_sync: DbSync,
                  temp_dir: str = None,
                  no_compression: bool = False,
                  archive_load_files: Dict = None) -> None:
    """
    Takes a list of record messages and loads it into the snowflake target table

    Args:
        stream: Name of the stream
        records: List of dictionary, that represents multiple csv lines. Dict key is the column name, value is the
                 column value
        db_sync: A DbSync object
        temp_dir: Directory where intermediate temporary files will be created. (Default: OS specific temp directory)
        no_compression: Disable to use compressed files. (Default: False)
        archive_load_files: Data needed for archive load files. (Default: None)

    Returns:
        None
    """
    # Generate file on disk in the required format
    filepath = db_sync.file_format.formatter.records_to_file(records,
                                                             db_sync.flatten_schema,
                                                             compression=not no_compression,
                                                             dest_dir=temp_dir,
                                                             data_flattening_max_level=
                                                             db_sync.data_flattening_max_level)

    # Get file stats
    row_count = len(records)
    size_bytes = os.path.getsize(filepath)

    # Upload to s3 and load into Snowflake
    s3_key = db_sync.put_to_stage(filepath, stream, row_count, temp_dir=temp_dir)
    db_sync.load_file(s3_key, row_count, size_bytes)

    # Delete file from local disk
    os.remove(filepath)

    if archive_load_files:
        stream_name_parts = stream_utils.stream_name_to_dict(stream)
        if 'schema_name' not in stream_name_parts or 'table_name' not in stream_name_parts:
            raise Exception(f"Failed to extract schema and table names from stream '{stream}'")

        archive_schema = stream_name_parts['schema_name']
        archive_table = stream_name_parts['table_name']
        archive_tap = archive_load_files['tap']

        archive_metadata = {
            'tap': archive_tap,
            'schema': archive_schema,
            'table': archive_table,
            'archived-by': 'pipelinewise_export_snowflake'
        }

        if 'column' in archive_load_files:
            archive_metadata.update({
                'incremental-key': archive_load_files['column'],
                'incremental-key-min': str(archive_load_files['min']),
                'incremental-key-max': str(archive_load_files['max'])
            })

        # Use same file name as in import
        archive_file = os.path.basename(s3_key)
        archive_key = f"{archive_tap}/{archive_table}/{archive_file}"

        db_sync.copy_to_archive(s3_key, archive_key, archive_metadata)

    # Delete file from S3
    db_sync.delete_from_stage(stream, s3_key)

def upload_to_snowflake_per_thread(s3_client, job, error_queue, config, schema, file_format_type):
    bucket, key, filename, stream = job
    try:
        if not error_queue.empty():
            # exit if error occurred in other thread
            # error_queue is shared among all threads, populate the error_queue with "ERROR" and return
            status = error_queue.get()
            if status == "ERROR":
                error_queue.put("ERROR")
                return
            
        start_time = time.time()

        s3_client.download_file(bucket, key, filename)
                
        # upload to snowflake
        file_path = filename.replace("\\", "/")
        db_sync = DbSync(config, schema, None, file_format_type)

        s3_key = db_sync.put_to_stage(file_path, stream, temp_dir=None)
            
        # remove from local after uploading to snowflake
        elasped_time = time.time() - start_time
        LOGGER.info(f"Time taken to download and upload {filename} is {elasped_time}")
        LOGGER.info(f'start_time: {start_time}, end_time: {time.time()} for {filename}')
        os.remove(file_path)
    except Exception as e:
        LOGGER.info(
            f"Error occurred in upload thread: {filename}")
        # send exit signal to other thread
        error_queue.put("ERROR")
        raise e

    

def transfer_data_from_s3_to_snowflake(s3_csv_lists, config, schema, file_format_type):
    parallel_transfer_start_time = time.time()
    
    error_queue = multiprocessing.Queue(1)
    # boto3 clients are generally thread-safe, but not for resources or sessions
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#multithreading-or-multiprocessing-with-clients
    session = boto3.session.Session()
    s3_client = session.client('s3')
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(upload_to_snowflake_per_thread, s3_client, csv_list, error_queue, config, schema, file_format_type) for csv_list in s3_csv_lists]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                raise SymonException(f'An error occurred while loading data to Snowflake: ' + exc, 'snowflake.clientError')

    parallel_transfer_end_time = time.time()
    stream = config["stream"]
    LOGGER.info(f"Elapsed time usage for {stream} is {parallel_transfer_end_time - parallel_transfer_start_time}")


def export_to_snowflake(config, o, file_format_type): 
    # retrieve the csv.gz lists and the schema file from the s3 bucket
    s3_client = boto3.client('s3')

    bucket = config["bucket"]
    prefix = config["prefix"]
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    s3_csv_lists = []
    schema_file = None
    for obj in response['Contents']:
        if obj['Key'].endswith('.csv.gz'):
            # append the exportTaskID to the file name to avoid conflict
            file_name = obj['Key'].split("/")[-2] + '-' + obj['Key'].split("/")[-1]
            s3_csv_lists.append((bucket, obj['Key'], file_name, config["stream"]))
        # for both python and spark parquet_to_csv steps, we always have a schema.json with the schema details
        if obj['Key'].endswith('.json'):
            schema_file = obj['Key']

    if schema_file is None:
        raise Exception("Schema file not found in the s3 bucket")
    if len(s3_csv_lists) == 0:
        raise Exception("No csv files found in the s3 bucket")

    local_schema_file = 'local_schema.json'
    s3_client.download_file(bucket, schema_file, local_schema_file)

    f = open(local_schema_file)
    schema = json.load(f)

    # generate the schema object for DbSync
    stream = config["stream"]
    o = {
        "stream": stream,
        "schema": {"properties": schema['properties']},
        "key_properties": config["key_columns"]
    }

    # check if the schema(Snowflake table) exists and create it if not
    # sync_table will check the schema with the table in Snowflake, if there's a miss matching in columns, raise an error
    db_sync = DbSync(config, o, None, file_format_type)
    db_sync.create_schema_if_not_exists()
    db_sync.sync_table()

    # the csv files in s3 are compressed, changed the parquet_to_csv to write into gzip format, also remove the headers
    transfer_data_from_s3_to_snowflake(s3_csv_lists, config, o, file_format_type)

    # Snowflake locks the table when we run query against it, so we need to load the file sequentially
    try: 
        start_time = time.time()
        for obj in s3_csv_lists:
            stage_file_path = obj[2].replace("\\", "/")
            
            db_sync.load_file(stage_file_path)
        end_time = time.time()
        LOGGER.info(f"Elapsed time usage to load {len(s3_csv_lists)} files for {stream}: {end_time - start_time}")
    except Exception as e:
        LOGGER.info("Error occurred while loading the file: ", e)
        # errors are already handled inside the load_file function, just raise here
        raise e
    finally:
        # delete the remote file anyway
        # Snowflake will charge for the memory usage if we don't delete the file
        for obj in s3_csv_lists: 
            stage_file_path = obj[2].replace("\\", "/")
            db_sync.delete_from_stage(stream, stage_file_path)
        
        # local csv.gz files are removed after uploading to snowflake
        os.remove(local_schema_file)
        LOGGER.info("Deleted all files from the stage and local")

def main():
    """Main function"""
    try:
        error_info = None
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('-c', '--config', help='Config file')
        args = arg_parser.parse_args()

        if args.config:
            with open(args.config, encoding="utf8") as config_input:
                config = json.load(config_input)
        else:
            config = {}

        # get file_format details from snowflake
        file_format_type = get_snowflake_statics(config)
        
        export_to_snowflake(config, None, file_format_type)

        LOGGER.debug("Exiting normally")
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            try:
                error_file_path = args.config.get('error_file_path', None)
                if error_file_path is not None:
                    try:
                        with open(error_file_path, 'w', encoding='utf-8') as fp:
                            json.dump(error_info, fp)
                    except:
                        pass
                # log error info as well in case file is corrupted
                error_info_json = json.dumps(error_info)
                error_start_marker = args.config.get('error_start_marker', ERROR_START_MARKER)
                error_end_marker = args.config.get('error_end_marker', ERROR_END_MARKER)
                LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')
            except:
                # error occurred before args was parsed correctly, log the error
                error_info_json = json.dumps(error_info)
                LOGGER.info(f'{ERROR_START_MARKER}{error_info_json}{ERROR_END_MARKER}')


if __name__ == '__main__':
    main()
