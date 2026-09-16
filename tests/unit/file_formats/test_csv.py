import unittest
import os
import gzip
import tempfile

import export_snowflake.file_formats.csv as csv


def _mock_record_to_csv_line(record, schema, data_flattening_max_level=0):
    return record


def _safe_temp_path(name):
    """Validate a temp-file path before it is passed to open().

    Resolves the path to a normalized absolute path and asserts it stays
    inside the system temp directory. This removes the path-manipulation
    taint flow (CWE-73) reported for open() calls that reuse a
    ``NamedTemporaryFile`` name.
    """
    temp_dir = os.path.realpath(tempfile.gettempdir())
    resolved = os.path.realpath(name)
    if os.path.commonpath([temp_dir, resolved]) != temp_dir:
        raise ValueError(f'Refusing to open path outside temp dir: {resolved}')
    return resolved


class TestCsv(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.config = {}

    def test_safe_temp_path_rejects_path_outside_temp_dir(self):
        # WP-33345: the CWE-73 remediation must reject a tainted path that
        # escapes the system temp directory instead of opening it.
        with self.assertRaises(ValueError):
            _safe_temp_path('/etc/passwd')

        # A legitimate temp-file path must still resolve and be returned.
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            resolved = _safe_temp_path(temp_file.name)
            self.assertEqual(resolved, os.path.realpath(temp_file.name))
        finally:
            os.remove(temp_file.name)

    def test_write_record_to_uncompressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write uncompressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with open(_safe_temp_path(csv_file.name), 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate uncompressed CSV file
        with open(_safe_temp_path(csv_file.name), 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        os.remove(csv_file.name)

    def test_write_records_to_compressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write gzip compressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False)
        with gzip.open(_safe_temp_path(csv_file.name), 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate gzip compressed CSV file
        with gzip.open(_safe_temp_path(csv_file.name), 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        os.remove(csv_file.name)

    def test_record_to_csv_line(self):
        record = {
            'key1': '1',
            'key2': '2030-01-22',
            'key3': '10000-01-22 12:04:22',
            'key4': '25:01:01',
            'key5': 'I\'m good',
            'key6': None,
        }

        schema = {
            'key1': {
                'type': ['null', 'string', 'integer'],
            },
            'key2': {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'date'},
                    {'type': ['null', 'string']}
                ]
            },
            'key3': {
                'type': ['null', 'string'], 'format': 'date-time',
            },
            'key4': {
                'anyOf': [
                    {'type': ['null', 'string'], 'format': 'time'},
                    {'type': ['null', 'string']}
                ]
            },
            'key5': {
                'type': ['null', 'string'],
            },
            'key6': {
                'type': ['null', 'string'], 'format': 'time',
            },
        }

        self.assertEqual(csv.record_to_csv_line(record, schema),
                         '"1","2030-01-22","10000-01-22 12:04:22","25:01:01","I\'m good",')

    # def test_create_copy_sql(self):
    #     self.assertEqual(csv.create_copy_sql(table_name='foo_table',
    #                                          stage_name='foo_stage',
    #                                          file_format_name='foo_file_format',
    #                                          columns=[{'name': 'COL_1'},
    #                                                   {'name': 'COL_2'},
    #                                                   {'name': 'COL_3',
    #                                                    'trans': 'parse_json'}]),

    #                      "COPY INTO foo_table (COL_1, COL_2, COL_3) FROM "
    #                      "'@foo_stage/foo_s3_key.csv' "
    #                      "FILE_FORMAT = (format_name='foo_file_format')")

    # def test_create_merge_sql(self):
    #     self.assertEqual(csv.create_merge_sql(table_name='foo_table',
    #                                          stage_name='foo_stage',
    #                                          file_format_name='foo_file_format',
    #                                          columns=[{'name': 'COL_1', 'trans': ''},
    #                                                   {'name': 'COL_2', 'trans': ''},
    #                                                   {'name': 'COL_3', 'trans': 'parse_json'}],
    #                                          pk_merge_condition='s.COL_1 = t.COL_1'),

    #                      "MERGE INTO foo_table t USING ("
    #                      "SELECT ($1) COL_1, ($2) COL_2, parse_json($3) COL_3 "
    #                      "FROM '@foo_stage/foo_s3_key.csv' "
    #                      "(FILE_FORMAT => 'foo_file_format')) s "
    #                      "ON s.COL_1 = t.COL_1 "
    #                      "WHEN MATCHED THEN UPDATE SET COL_1=s.COL_1, COL_2=s.COL_2, COL_3=s.COL_3 "
    #                      "WHEN NOT MATCHED THEN "
    #                      "INSERT (COL_1, COL_2, COL_3) "
    #                      "VALUES (s.COL_1, s.COL_2, s.COL_3)")
