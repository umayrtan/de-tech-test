# de-tech-test
# Apache Beam Transaction Processing Pipeline

This project contains an Apache Beam pipeline for processing transaction data from a CSV file. The pipeline filters transactions based on specific criteria, sums the transaction amounts by date, and outputs the results in JSON format.

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

## Running the Pipeline

To run the first task pipeline, execute the `task_1.py` script:

```sh
python task_1.py

or to run the second task pipeline, execute the `task_2.py` script:

```sh
python task_2.py

