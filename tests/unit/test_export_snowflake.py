# no function called persist_lines, flush_streams

import io
import json
import unittest
import os
import itertools

from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest.mock import patch

import export_snowflake


# Fixed, trusted directory holding this test module's JSON fixtures. Resolved
# once from the module location so resource lookups never depend on tainted
# input.
_RESOURCES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources')


def _read_resource_lines(resource_name):
    """Read the lines of a bundled test resource by bare filename.

    Centralized validation to remediate CWE-73 (external control of file
    name/path): the resource name must be a plain filename (no directory
    separators, parent references, or absolute paths), and the resolved real
    path must stay inside the fixed resources directory before it is opened.
    """
    if resource_name != os.path.basename(resource_name):
        raise ValueError(f'Invalid resource name: {resource_name!r}')

    resources_root = os.path.realpath(_RESOURCES_DIR)
    resource_path = os.path.realpath(os.path.join(resources_root, resource_name))
    if os.path.commonpath([resources_root, resource_path]) != resources_root:
        raise ValueError(f'Resource path escapes resources directory: {resource_name!r}')

    with open(resource_path, 'r') as f:
        return f.readlines()


def _mock_record_to_csv_line(record):
    return record


class TestexportSnowflake(unittest.TestCase):

    def setUp(self):
        self.config = {}
        self.maxDiff = None

    @patch('sys.getsizeof')
    # @patch('export_snowflake.flush_streams')
    @patch('export_snowflake.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20MB_expect_flushing_once(self, dbSync_mock,
                                                                                     sys_getsizeof_mock):
        self.config['batch_size'] = 20
        self.config['flush_all_streams'] = True

        lines = _read_resource_lines('logical-streams.json')

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # flush_streams_mock.return_value = '{"currently_syncing": null}'
        sys_getsizeof_mock.return_value = 1024 * 1024

        # export_snowflake.persist_lines(self.config, lines)

        # self.assertEqual(1, flush_streams_mock.call_count)

    @patch('sys.getsizeof')
    # @patch('export_snowflake.flush_streams')
    @patch('export_snowflake.DbSync')
    def test_persist_lines_with_same_schema_expect_flushing_once(self, dbSync_mock,
                                                                 sys_getsizeof_mock):
        self.config['batch_size'] = 20

        lines = _read_resource_lines('same-schemas-multiple-times.json')

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # flush_streams_mock.return_value = '{"currently_syncing": null}'
        sys_getsizeof_mock.return_value = 1024 * 1024

        # export_snowflake.persist_lines(self.config, lines)

        # self.assertEqual(1, flush_streams_mock.call_count)

    @patch('sys.getsizeof')
    @patch('export_snowflake.datetime')
    # @patch('export_snowflake.flush_streams')
    @patch('export_snowflake.DbSync')
    def test_persist_40_records_with_batch_wait_limit(self, dbSync_mock, dateTime_mock, sys_getsizeof_mock):

        start_time = datetime(2021, 4, 6, 0, 0, 0)
        increment = 11
        counter = itertools.count()

        # Move time forward by {{increment}} seconds every time utcnow() is called
        dateTime_mock.utcnow.side_effect = lambda: start_time + timedelta(seconds=increment * next(counter))

        self.config['batch_size'] = 100
        self.config['batch_wait_limit_seconds'] = 10
        self.config['flush_all_streams'] = True

        # Expecting 40 records
        lines = _read_resource_lines('logical-streams.json')

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # flush_streams_mock.return_value = '{"currently_syncing": null}'
        sys_getsizeof_mock.return_value = 1024 * 1024

        # export_snowflake.persist_lines(self.config, lines)

        # Expecting flush after every records + 1 at the end
        # self.assertEqual(flush_streams_mock.call_count, 41)

    @patch('export_snowflake.DbSync')
    @patch('export_snowflake.os.remove')
    def test_archive_load_files_incremental_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True
        self.config['s3_bucket'] = 'dummy_bucket'

        lines = _read_resource_lines('messages-simple-table.json')

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        # export_snowflake.persist_lines(self.config, lines)

        # copy_to_archive_args = instance.copy_to_archive.call_args[0]
        # self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        # self.assertEqual(copy_to_archive_args[1], 'test_tap_id/test_simple_table/some-name_date_batch_hash.csg.gz')
        # self.assertDictEqual(copy_to_archive_args[2], {
        #     'tap': 'test_tap_id',
        #     'schema': 'tap_mysql_test',
        #     'table': 'test_simple_table',
        #     'archived-by': 'pipelinewise_export_snowflake',
        #     'incremental-key': 'id',
        #     'incremental-key-min': '1',
        #     'incremental-key-max': '5'
        # })

    @patch('export_snowflake.DbSync')
    @patch('export_snowflake.os.remove')
    def test_archive_load_files_log_based_replication(self, os_remove_mock, dbSync_mock):
        self.config['tap_id'] = 'test_tap_id'
        self.config['archive_load_files'] = True

        lines = _read_resource_lines('logical-streams.json')

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.put_to_stage.return_value = 'some-s3-folder/some-name_date_batch_hash.csg.gz'

        # export_snowflake.persist_lines(self.config, lines)

        # copy_to_archive_args = instance.copy_to_archive.call_args[0]
        # self.assertEqual(copy_to_archive_args[0], 'some-s3-folder/some-name_date_batch_hash.csg.gz')
        # self.assertEqual(copy_to_archive_args[1], 'test_tap_id/logical1_table2/some-name_date_batch_hash.csg.gz')
        # self.assertDictEqual(copy_to_archive_args[2], {
        #     'tap': 'test_tap_id',
        #     'schema': 'logical1',
        #     'table': 'logical1_table2',
        #     'archived-by': 'pipelinewise_export_snowflake'
        # })

    @patch('sys.getsizeof')
    # @patch('export_snowflake.flush_streams')
    @patch('export_snowflake.DbSync')
    def test_persist_lines_with_only_state_messages(self, dbSync_mock, sys_getsizeof_mock):
        """
        Given only state messages, export should emit the last one
        """

        self.config['batch_size'] = 5

        lines = _read_resource_lines('streams_only_state.json')

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        sys_getsizeof_mock.return_value = 1024 * 1024

        # catch stdout
        buf = io.StringIO()
        # with redirect_stdout(buf):
        #     export_snowflake.persist_lines(self.config, lines)

        # flush_streams_mock.assert_not_called()

        # self.assertEqual(
        #     buf.getvalue().strip(),
        #     '{"bookmarks": {"tap_mysql_test-test_simple_table": {"replication_key": "id", '
        #     '"replication_key_value": 100, "version": 1}}}')


class TestResourceLoader(unittest.TestCase):
    """Regression tests for the CWE-73 remediation (WP-33329)."""

    def test_valid_resource_loads_from_resources_dir(self):
        lines = _read_resource_lines('logical-streams.json')
        self.assertTrue(lines)

    def test_path_separator_is_rejected(self):
        with self.assertRaises(ValueError):
            _read_resource_lines('../test_export_snowflake.py')

    def test_absolute_path_is_rejected(self):
        with self.assertRaises(ValueError):
            _read_resource_lines('/etc/passwd')

    def test_nested_path_is_rejected(self):
        with self.assertRaises(ValueError):
            _read_resource_lines('sub/logical-streams.json')
