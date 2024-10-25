import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from task_2 import ProcessTransactions  # Import the composite transform from your pipeline file

class TestProcessTransactions(unittest.TestCase):
    def test_process_transactions(self):
        input_data = [
            '21,2011-01-01',
            '354325,2001-01-01',
            '210931,2020-01-01',
            '1,2019-01-01',
            '21029344,2020-01-01',
            '21,2011-01-01'
        ]
        expected_output = [
            '{"date": "2011-01-01", "total_amount": 42.00}',
            '{"date": "2020-01-01", "total_amount": 21240275.00}'
        ]

        with TestPipeline() as p:
            input_pcoll = p | beam.Create(input_data)
            output_pcoll = input_pcoll | ProcessTransactions()
            assert_that(output_pcoll, equal_to(expected_output))

if __name__ == '__main__':
    unittest.main()
