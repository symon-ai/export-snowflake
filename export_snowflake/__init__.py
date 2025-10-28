#!/usr/bin/env python3

import argparse
import io
import json
import traceback
import logging
import os
import sys
import boto3
import time
import snowflake.connector

from singer import get_logger
from datetime import datetime, timedelta

from export_snowflake.file_formats import csv
from export_snowflake.file_formats import parquet
from export_snowflake import stream_utils

from export_snowflake.db_sync import DbSync
from export_snowflake.file_format import FileFormatTypes
from export_snowflake.exceptions import (
    SymonException
)

LOGGER = get_logger('export_snowflake')

# Set logger to DEBUG level to see all debug messages
LOGGER.setLevel(logging.DEBUG)

# Also set handlers to DEBUG level (Singer library might have set them to INFO)
for handler in LOGGER.handlers:
    handler.setLevel(logging.DEBUG)

# If no handlers exist, add a console handler with DEBUG level
if not LOGGER.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('time=%(asctime)s name=%(name)s level=%(levelname)s message=%(message)s', 
                                   datefmt='%Y-%m-%d %H:%M:%S')
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)

# Tone down snowflake.connector log noise by only outputting warnings and higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

# for symon error logging
ERROR_START_MARKER = '[target_error_start]'
ERROR_END_MARKER = '[target_error_end]'

LOCAL_SCHEMA_FILE_PATH = 'local_schema.json'

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

def direct_transfer_data_from_s3_to_snowflake(config, o, file_format_type):    
    # retrieve the schema file from s3 bucket
    # if the table doesn't exist, create the new table based on the schema
    LOGGER.debug('[EXPORT-SNOWFLAKE] Starting direct transfer from S3 to Snowflake')
    LOGGER.debug(f'[EXPORT-SNOWFLAKE] S3 Bucket: {config.get("bucket")}')
    LOGGER.debug(f'[EXPORT-SNOWFLAKE] S3 Prefix: {config.get("prefix")}')
    LOGGER.debug(f'[EXPORT-SNOWFLAKE] Stream: {config.get("stream")}')
    LOGGER.debug(f'[EXPORT-SNOWFLAKE] File format type: {file_format_type}')
    
    s3_client = boto3.client('s3')

    bucket = config["bucket"]
    prefix = config["prefix"]
    LOGGER.debug(f'[EXPORT-SNOWFLAKE] Listing objects in s3://{bucket}/{prefix}')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    schema_file = None
    for obj in response['Contents']:
        # for both python and spark parquet_to_csv steps, we always have a schema.json with the schema details
        if obj['Key'].endswith('.json'):
            schema_file = obj['Key']
            LOGGER.debug(f'[EXPORT-SNOWFLAKE] Found schema file: {schema_file}')
    
    if schema_file is None:
        LOGGER.error('[EXPORT-SNOWFLAKE] Schema file not found in the s3 bucket')
        raise Exception("Schema file not found in the s3 bucket")

    LOGGER.debug(f'[EXPORT-SNOWFLAKE] Downloading schema file from s3://{bucket}/{schema_file}')
    s3_client.download_file(bucket, schema_file, LOCAL_SCHEMA_FILE_PATH)

    f = open(LOCAL_SCHEMA_FILE_PATH)
    schema = json.load(f)
    LOGGER.debug(f'[EXPORT-SNOWFLAKE] Schema loaded: {json.dumps(schema)}')

    # generate the schema object for DbSync
    stream = config["stream"]
    o = {
        "stream": stream,
        "schema": {"properties": schema['fields']},
        "key_properties": config["key_columns"]
    }
    LOGGER.debug(f'[EXPORT-SNOWFLAKE] DbSync object created with stream: {stream}, key_properties: {config["key_columns"]}')
    
    db_sync = DbSync(config, o, None, file_format_type)
    remove_temp_external_stage = False

    try:
        transfer_start_time = time.time()
        LOGGER.debug('[EXPORT-SNOWFLAKE] Starting data transfer process')

        # check if the schema(Snowflake table) exists and create it if not
        LOGGER.debug('[EXPORT-SNOWFLAKE] Checking if Snowflake schema exists')
        db_sync.create_schema_if_not_exists()
        
        # sync_table will check the schema with the table in Snowflake, if there's a miss matching in columns, raise an error
        LOGGER.debug('[EXPORT-SNOWFLAKE] Syncing table schema')
        db_sync.sync_table()
        
        # generate a new stage with stream in Snowflake
        # the stage will be an external stage that points to the s3
        # the following merge query will be processed directly against the external stage
        LOGGER.debug(f'[EXPORT-SNOWFLAKE] Generating temporary external S3 stage for s3://{bucket}/{prefix}')
        stage_generation_query = db_sync.generate_temporary_external_s3_stage(bucket, prefix, config.get('s3_credentials', None), config.get('storage_integration', None))
        LOGGER.debug(f'[EXPORT-SNOWFLAKE] Stage generation query: {stage_generation_query}')
        
        # after creating the external stage, we could load the file directly from the s3 to Snowflake
        # need to specify the patterns in the s3 bucket to filter out the target csv.gz files
        LOGGER.debug('[EXPORT-SNOWFLAKE] Loading file from S3 to Snowflake')
        db_sync.load_file(stage_generation_query)
        
        transfer_end_time = time.time()
        stream = config["stream"]
        elapsed_time = transfer_end_time - transfer_start_time
        LOGGER.debug(f'[EXPORT-SNOWFLAKE] Transfer completed successfully')
        LOGGER.info(f'[EXPORT-SNOWFLAKE] Elapsed time usage for {stream} is {elapsed_time} seconds')
    except snowflake.connector.errors.ProgrammingError as e:
        err_msg = str(e)
        LOGGER.error(f'[EXPORT-SNOWFLAKE] Snowflake ProgrammingError: {err_msg}')
        storage_integration = config.get('storage_integration', '').upper()
        if f"Location" in err_msg and "not allowed by integration" in err_msg:
            s3_allowed_location = f"s3://{bucket}/{prefix[:prefix.rfind('/') + 1]}"
            LOGGER.error(f'[EXPORT-SNOWFLAKE] Storage integration error: Location not allowed')
            raise SymonException(f'Snowflake storage integration "{storage_integration}" must include "{s3_allowed_location}" in S3_ALLOWED_LOCATIONS.', "snowflake.clientError")
        if f"Insufficient privileges to operate on integration" in err_msg:
            LOGGER.error(f'[EXPORT-SNOWFLAKE] Storage integration error: Insufficient privileges')
            raise SymonException(f'USAGE privilege is missing on storage integration "{storage_integration}".', "snowflake.clientError")
        raise
    except Exception as e:
        LOGGER.error(f"[EXPORT-SNOWFLAKE] Error occurred in direct_transfer_data_from_s3_to_snowflake: {e}")
        LOGGER.debug(f"[EXPORT-SNOWFLAKE] Exception details: {traceback.format_exc()}")
        raise e
    finally:
        # remove the schema file from local
        LOGGER.debug('[EXPORT-SNOWFLAKE] Cleaning up: Removing local schema file')
        os.remove(LOCAL_SCHEMA_FILE_PATH)
        
        # Snowflake will only remove the external stage object, the s3 bucket and files will remain
        try:
            LOGGER.debug('[EXPORT-SNOWFLAKE] Removing external S3 stage from Snowflake')
            db_sync.remove_external_s3_stage()
        except Exception as e:
            LOGGER.error(f"[EXPORT-SNOWFLAKE] Error occurred while removing external stage: {e}")
            pass

def main():
    """Main function"""
    try:
        error_info = None
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('-c', '--config', help='Config file')
        args = arg_parser.parse_args()

        if args.config:
            LOGGER.debug(f'[EXPORT-SNOWFLAKE] Reading config from: {args.config}')
            print(f'[EXPORT-SNOWFLAKE] Reading config from: {args.config}')  # Backup print to ensure visibility
            with open(args.config, encoding="utf8") as config_input:
                config = json.load(config_input)
            LOGGER.debug(f'[EXPORT-SNOWFLAKE] Full config received: {json.dumps(config, indent=2)}')
            # Log error_file_path to compare with TypeScript side
            error_file_path = config.get('error_file_path', None)
            LOGGER.debug(f'[EXPORT-SNOWFLAKE] error_file_path from config: {error_file_path}')
            print(f'[EXPORT-SNOWFLAKE] error_file_path from config: {error_file_path}')  # Backup print
        else:
            config = {}

        # get file_format details from snowflake
        LOGGER.debug('[EXPORT-SNOWFLAKE] Getting file format details from Snowflake')
        file_format_type = get_snowflake_statics(config)
        LOGGER.debug(f'[EXPORT-SNOWFLAKE] File format type: {file_format_type}')
        
        LOGGER.debug('[EXPORT-SNOWFLAKE] Calling direct_transfer_data_from_s3_to_snowflake')
        direct_transfer_data_from_s3_to_snowflake(config, None, file_format_type)

        LOGGER.debug("[EXPORT-SNOWFLAKE] Exiting normally")
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
                # Calculate error sizes in bytes and KB
                message_size_bytes = len(error_info.get('message', ''))
                traceback_size_bytes = len(error_info.get('traceback', ''))
                total_error_size_bytes = message_size_bytes + traceback_size_bytes
                
                message_size_kb = message_size_bytes / 1024
                traceback_size_kb = traceback_size_bytes / 1024
                total_error_size_kb = total_error_size_bytes / 1024
                
                LOGGER.debug(f'[EXPORT-SNOWFLAKE] Error occurred - message: {message_size_bytes} bytes ({message_size_kb:.2f} KB), traceback: {traceback_size_bytes} bytes ({traceback_size_kb:.2f} KB), total: {total_error_size_bytes} bytes ({total_error_size_kb:.2f} KB)')
                LOGGER.debug(f'[EXPORT-SNOWFLAKE] Error occurred, error_info: {json.dumps(error_info)}')
                
                # Log working directory and paths for debugging
                current_working_dir = os.getcwd()
                LOGGER.debug(f'[EXPORT-SNOWFLAKE] Current working directory: {current_working_dir}')
                
                error_file_path = config.get('error_file_path', None)
                LOGGER.debug(f'[EXPORT-SNOWFLAKE] error_file_path from config (relative): {error_file_path}')
                
                if error_file_path is not None:
                    try:
                        # Get absolute path
                        absolute_error_path = os.path.abspath(error_file_path)
                        LOGGER.debug(f'[EXPORT-SNOWFLAKE] error_file_path (absolute): {absolute_error_path}')
                        LOGGER.debug(f'[EXPORT-SNOWFLAKE] Writing error to file: {absolute_error_path}')
                        
                        with open(error_file_path, 'w', encoding='utf-8') as fp:
                            json.dump(error_info, fp)
                        
                        # Verify file was written
                        if os.path.exists(error_file_path):
                            file_size = os.path.getsize(error_file_path)
                            LOGGER.debug(f'[EXPORT-SNOWFLAKE] Successfully wrote error to file: {absolute_error_path} (size: {file_size} bytes)')
                        else:
                            LOGGER.error(f'[EXPORT-SNOWFLAKE] File does not exist after write attempt: {absolute_error_path}')
                    except Exception as write_error:
                        LOGGER.error(f'[EXPORT-SNOWFLAKE] Error writing to file {error_file_path}: {write_error}')
                        pass
                # log error info as well in case file is corrupted
                error_info_json = json.dumps(error_info)
                error_json_size_bytes = len(error_info_json)
                error_json_size_kb = error_json_size_bytes / 1024
                LOGGER.debug(f'[EXPORT-SNOWFLAKE] Serialized error JSON size: {error_json_size_bytes} bytes ({error_json_size_kb:.2f} KB)')
                
                error_start_marker = config.get('error_start_marker', ERROR_START_MARKER)
                error_end_marker = config.get('error_end_marker', ERROR_END_MARKER)
                LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')
            except Exception as final_error:
                # error occurred before config was loaded correctly, log the error
                LOGGER.error(f'[EXPORT-SNOWFLAKE] Final error handler exception: {final_error}')
                error_info_json = json.dumps(error_info)
                LOGGER.info(f'{ERROR_START_MARKER}{error_info_json}{ERROR_END_MARKER}')


if __name__ == '__main__':
    main()
