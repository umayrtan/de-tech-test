# de-tech-test
# Apache Beam Transaction Processing Pipeline

This project contains an Apache Beam pipeline for processing transaction data from a CSV file. The pipeline filters transactions based on specific criteria, sums the transaction amounts by date, and outputs the results in JSONL.gz format.

## Project Structure
- `task_1.py`: Contains the main Apache Beam pipeline code, satisfying task 1.
- `task_2.py`: Contains the main Apache Beam pipeline code with the transformation tasks grouped, satisfying task 2.
- `unit_test.py`: Contains the unit test for the pipeline.

## Pipeline Overview

The pipeline performs the following steps:
1. **Read CSV**: Reads transaction data from a CSV file.
2. **Parse CSV**: Parses each line of the CSV file.
3. **Filter Transactions**: Filters transactions based on amount and date.
4. **Sum by Date**: Sums the transaction amounts by date.
5. **Format Output**: Formats the output as JSON strings.
6. **Write Results**: Writes the results to a jsonl.gz file.

## Running the task 1 Pipeline

To run the first task pipeline, execute the `task_1.py` script:


`python task_1.py`

## Running the task 2 Pipeline

Or to run the second task pipeline, execute the `task_2.py` script:

`python task_2.py`

## Unit Tests
Unit tests are provided in the `unit_test.py` file. The tests verify the functionality of the ProcessTransactions composite transform.

To run the tests, execute the `unit_test.py` script:

`python unit_test.py`

## Dependencies
Apache Beam
Python 3.x
You can install the required dependencies using pip:

`pip install apache-beam`

## Example Input
The input CSV file should have the following format:

transaction_amount,date
21,2011-01-01
354325,2001-01-01
210931,2020-01-01
1,2019-01-01
21029344,2020-01-01
21,2011-01-01

## Example Output
The output JSONL file will have the following format:

{"date": "2011-01-01", "total_amount": 42.00}
{"date": "2020-01-01", "total_amount": 21240275.00}




