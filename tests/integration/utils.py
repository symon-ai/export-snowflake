import os
import json
from dotenv import load_dotenv

load_dotenv()

def get_db_config():
    config = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # The following values needs to be defined in environment variables with
    # valid details to a Snowflake instace, AWS IAM role and an S3 bucket
    # --------------------------------------------------------------------------
    # Snowflake instance
    config['account'] = os.environ.get('EXPORT_SNOWFLAKE_ACCOUNT')
    config['dbname'] = os.environ.get('EXPORT_SNOWFLAKE_DBNAME')
    config['user'] = os.environ.get('EXPORT_SNOWFLAKE_USER')
    config['password'] = os.environ.get('EXPORT_SNOWFLAKE_PASSWORD')
    config['warehouse'] = os.environ.get('EXPORT_SNOWFLAKE_WAREHOUSE')
    config['default_export_schema'] = os.environ.get("EXPORT_SNOWFLAKE_SCHEMA")
    config['stage'] = os.environ.get("EXPORT_SNOWFLAKE_STAGE")
    config['file_format'] = os.environ.get("EXPORT_SNOWFLAKE_FILE_FORMAT_CSV")

    # AWS IAM and S3 bucket
    config['aws_access_key_id'] = os.environ.get('EXPORT_SNOWFLAKE_AWS_ACCESS_KEY')
    config['aws_secret_access_key'] = os.environ.get('EXPORT_SNOWFLAKE_AWS_SECRET_ACCESS_KEY')
    config['s3_bucket'] = os.environ.get('EXPORT_SNOWFLAKE_S3_BUCKET')
    config['s3_key_prefix'] = os.environ.get('EXPORT_SNOWFLAKE_S3_KEY_PREFIX')
    config['s3_acl'] = os.environ.get('EXPORT_SNOWFLAKE_S3_ACL')

    # External stage in snowflake with client side encryption details
    config['client_side_encryption_master_key'] = os.environ.get('CLIENT_SIDE_ENCRYPTION_MASTER_KEY')

    # --------------------------------------------------------------------------
    # The following variables needs to be empty.
    # The tests cases will set them automatically whenever it's needed
    # --------------------------------------------------------------------------
    config['disable_table_cache'] = None
    config['schema_mapping'] = None
    config['add_metadata_columns'] = None
    config['hard_delete'] = None
    config['flush_all_streams'] = None
    config['validate_records'] = None
    config['auth_method'] = 'basic'

    return config


def get_test_config():
    db_config = get_db_config()

    return db_config


def get_test_tap_lines(filename):
    # Constrain the requested resource to the sibling `resources/` directory only,
    # guarding against path manipulation (CWE-73). Reject anything that is not a
    # bare filename (absolute paths, directory components, `..` traversal) and then
    # confirm the resolved real path stays inside the resources directory.
    if filename != os.path.basename(filename):
        raise ValueError('Invalid resource filename: {}'.format(filename))

    resources_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), 'resources'))
    resource_path = os.path.realpath(os.path.join(resources_dir, filename))
    if os.path.dirname(resource_path) != resources_dir:
        raise ValueError('Invalid resource filename: {}'.format(filename))

    lines = []
    with open(resource_path) as tap_stdout:
        for line in tap_stdout.readlines():
            lines.append(line)

    return lines
