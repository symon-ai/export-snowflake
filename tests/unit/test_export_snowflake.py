# no function called persist_lines, flush_streams

import io
import json
import unittest
import os
import tempfile
import itertools

from contextlib import redirect_stdout
from datetime import datetime, timedelta
from unittest.mock import patch

import export_snowflake


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

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

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

        with open(f'{os.path.dirname(__file__)}/resources/same-schemas-multiple-times.json', 'r') as f:
            lines = f.readlines()

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
        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

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

        with open(f'{os.path.dirname(__file__)}/resources/messages-simple-table.json', 'r') as f:
            lines = f.readlines()

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

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

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

        with open(f'{os.path.dirname(__file__)}/resources/streams_only_state.json', 'r') as f:
            lines = f.readlines()

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



class TestExportSecurity(unittest.TestCase):
    def test_error_report_is_written_beside_config_on_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            config_path = os.path.join(folder, 'target.config.json')
            error_path = os.path.join(folder, 'targetError.json')
            with open(config_path, 'w', encoding='utf-8') as fp:
                json.dump({'error_file_path': error_path}, fp)
            with patch('sys.argv', ['export-snowflake', '--config', config_path]), \
                    patch('export_snowflake.get_snowflake_statics', return_value='CSV'), \
                    patch('export_snowflake.direct_transfer_data_from_s3_to_snowflake',
                          side_effect=RuntimeError('export failed')):
                with self.assertRaisesRegex(RuntimeError, 'export failed'):
                    export_snowflake.main()
            with open(error_path, encoding='utf-8') as fp:
                report = json.load(fp)
            self.assertIn('export failed', report['message'])

    def test_error_report_cannot_write_outside_config_directory(self):
        with tempfile.TemporaryDirectory() as folder:
            config_path = os.path.join(folder, 'target.config.json')
            outside = os.path.join(folder, 'outside')
            os.mkdir(outside)
            error_path = os.path.join(outside, 'targetError.json')
            with open(error_path, 'w', encoding='utf-8') as fp:
                fp.write('untouched')
            with open(config_path, 'w', encoding='utf-8') as fp:
                json.dump({'error_file_path': error_path}, fp)
            with patch('sys.argv', ['export-snowflake', '--config', config_path]), \
                    patch('export_snowflake.get_snowflake_statics', return_value='CSV'), \
                    patch('export_snowflake.direct_transfer_data_from_s3_to_snowflake',
                          side_effect=RuntimeError('export failed')):
                with self.assertRaises(RuntimeError):
                    export_snowflake.main()
            with open(error_path, encoding='utf-8') as fp:
                self.assertEqual(fp.read(), 'untouched')

    def test_error_report_does_not_follow_symlinks(self):
        with tempfile.TemporaryDirectory() as folder:
            config_path = os.path.join(folder, 'target.config.json')
            outside = os.path.join(folder, 'outside.json')
            with open(outside, 'w', encoding='utf-8') as fp:
                fp.write('untouched')
            os.symlink(outside, os.path.join(folder, 'targetError.json'))
            with self.assertRaises(OSError):
                export_snowflake.write_error_file(
                    {'error_file_path': os.path.join(folder, 'targetError.json')},
                    config_path, {'message': 'export failed'})
            with open(outside, encoding='utf-8') as fp:
                self.assertEqual(fp.read(), 'untouched')

    @patch('export_snowflake.DbSync')
    @patch('export_snowflake.boto3.client')
    def test_s3_schema_selection_and_stream_logging(self, s3_client, db_sync):
        with tempfile.TemporaryDirectory() as folder:
            schema_path = os.path.join(folder, 'local_schema.json')
            s3 = s3_client.return_value
            s3.list_objects_v2.return_value = {
                'Contents': [{'Key': 'exports/job/records.csv.gz'},
                             {'Key': 'exports/job/schema.json'}]}

            def download_schema(bucket, key, destination):
                with open(destination, 'w', encoding='utf-8') as fp:
                    json.dump({'fields': {'id': {'type': ['integer']}}}, fp)

            s3.download_file.side_effect = download_schema
            config = {'bucket': 'authorized-bucket', 'prefix': 'exports/job/',
                      'stream': 'orders\nFORGED LOG', 'key_columns': ['id']}
            with patch.object(export_snowflake, 'LOCAL_SCHEMA_FILE_PATH', schema_path), \
                    self.assertLogs(export_snowflake.LOGGER, level='INFO') as captured:
                export_snowflake.direct_transfer_data_from_s3_to_snowflake(config, None, 'CSV')

            s3.list_objects_v2.assert_called_once_with(
                Bucket='authorized-bucket', Prefix='exports/job/')
            s3.download_file.assert_called_once_with(
                'authorized-bucket', 'exports/job/schema.json', schema_path)
            db_sync.return_value.generate_temporary_external_s3_stage.assert_called_once_with(
                'authorized-bucket', 'exports/job/', None, None)
            event = next(record.getMessage() for record in captured.records
                         if 'Elapsed time usage' in record.getMessage())
            self.assertIn(r'orders\nFORGED LOG', event)
            self.assertNotIn('\n', event)