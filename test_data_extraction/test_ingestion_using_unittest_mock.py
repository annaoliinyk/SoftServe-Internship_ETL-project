import unittest
from unittest.mock import patch

from OpenSkyDataExtractor.get_states import DataIngestion


class TestDataIngestion(unittest.TestCase):
    @patch('configs.get_api_user_credentials.get_credentials_from_file')
    def test_get_states(self, mock_get_states_method):
        mock_get_states_method.return_value = ''
        actual_states = DataIngestion().get_states()
        self.assertIsNotNone(actual_states)
