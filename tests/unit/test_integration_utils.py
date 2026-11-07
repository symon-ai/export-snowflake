import os
import unittest

# The integration `utils` module only needs the (dev) `python-dotenv` package to
# import; it does NOT require live Snowflake/AWS credentials to exercise the
# resource-path validation in `get_test_tap_lines`, so it is safe to unit test.
from tests.integration import utils


class TestGetTestTapLines(unittest.TestCase):
    """Regression coverage for the CWE-73 path-manipulation fix in
    ``tests.integration.utils.get_test_tap_lines`` (WP-33357)."""

    def test_reads_legitimate_resource_file(self):
        # A plain resource filename still resolves and reads correctly.
        lines = utils.get_test_tap_lines('messages-simple-table.json')
        self.assertTrue(lines)
        self.assertTrue(all(isinstance(line, str) for line in lines))

    def test_rejects_parent_directory_traversal(self):
        with self.assertRaises(ValueError):
            utils.get_test_tap_lines('../../../../../../etc/passwd')

    def test_rejects_absolute_path(self):
        with self.assertRaises(ValueError):
            utils.get_test_tap_lines(os.path.join(os.sep, 'etc', 'passwd'))


if __name__ == '__main__':
    unittest.main()
