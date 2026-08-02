import unittest
import os
import gzip
import tempfile

import export_snowflake.file_formats.csv as csv


# Trusted directory the temp fixtures created by these tests are anchored to.
_TMP_DIR = os.path.realpath(tempfile.gettempdir())


def _safe_remove(path):
    """Remove a file only if it resolves inside the trusted temp directory.

    Guards against CWE-73 path manipulation: the fully-resolved absolute path
    must live inside ``_TMP_DIR`` (the system temp dir the fixtures are created
    in). Any path that escapes that trusted directory (e.g. via ``..``
    traversal or an absolute path elsewhere) raises ``ValueError`` instead of
    removing an arbitrary, potentially attacker-controlled file.
    """
    resolved = os.path.realpath(path)
    if os.path.commonpath([_TMP_DIR, resolved]) != _TMP_DIR:
        raise ValueError(
            'Refusing to remove path outside trusted temp dir: {}'.format(path))
    os.remove(resolved)


def _mock_record_to_csv_line(record, schema, data_flattening_max_level=0):
    return record


class TestCsv(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.config = {}

    def test_write_record_to_uncompressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write uncompressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False, dir=_TMP_DIR)
        with open(csv_file.name, 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate uncompressed CSV file
        with open(csv_file.name, 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        _safe_remove(csv_file.name)

    def test_write_records_to_compressed_file(self):
        records = {
            'pk_1': 'data1,data2,data3,data4',
            'pk_2': 'data5,data6,data7,data8'
        }
        schema = {}

        # Write gzip compressed CSV file
        csv_file = tempfile.NamedTemporaryFile(delete=False, dir=_TMP_DIR)
        with gzip.open(csv_file.name, 'wb') as f:
            csv.write_records_to_file(f, records, schema, _mock_record_to_csv_line)

        # Read and validate gzip compressed CSV file
        with gzip.open(csv_file.name, 'rt') as f:
            self.assertEqual(f.readlines(), ['data1,data2,data3,data4\n',
                                             'data5,data6,data7,data8\n'])

        _safe_remove(csv_file.name)

    def test_safe_remove_rejects_path_outside_trusted_dir(self):
        # WP-33347: CWE-73 guard — _safe_remove must refuse to delete a path
        # that resolves outside the trusted temp directory, and must not touch
        # the filesystem when it refuses.
        target = os.path.join(os.path.dirname(__file__), 'not_deleted.txt')
        with open(target, 'w') as f:
            f.write('keep me')
        try:
            with self.assertRaises(ValueError):
                _safe_remove(target)
            # Traversal-style escape from the trusted dir is also rejected.
            with self.assertRaises(ValueError):
                _safe_remove(os.path.join(_TMP_DIR, '..', 'etc', 'passwd'))
            self.assertTrue(os.path.exists(target))
        finally:
            os.remove(target)

    def test_safe_remove_deletes_file_inside_trusted_dir(self):
        # WP-33347: files legitimately created under the trusted temp dir are
        # still removed as before, so fixture cleanup stays equivalent.
        csv_file = tempfile.NamedTemporaryFile(delete=False, dir=_TMP_DIR)
        csv_file.close()
        self.assertTrue(os.path.exists(csv_file.name))
        _safe_remove(csv_file.name)
        self.assertFalse(os.path.exists(csv_file.name))

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
