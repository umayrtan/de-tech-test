import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import datetime

class ParseCsv(beam.DoFn):
    def process(self, element):
        columns = element.split(',')
        try:
            transaction_amount = float(columns[0])
            date_str = columns[1]
            date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
            yield {
                'transaction_amount': transaction_amount,
                'date_str': date_str,
                'date': date,
                'original_line': element  # Include the original CSV line
            }
        except ValueError:
            print(f"Error parsing: {element}")

class FilterTransactions(beam.DoFn):
    def process(self, element):
        if element['transaction_amount'] > 20 and element['date'].year >= 2010:
            yield (element['date_str'], element['transaction_amount'])
        else:
            # Print the original CSV line format
            print(element['original_line'])

class SumTransactionByDate(beam.CombineFn):
    def create_accumulator(self):
        return 0.00

    def add_input(self, accumulator, input):
        return accumulator + input

    def merge_accumulators(self, accumulators):
        return sum(accumulators)

    def extract_output(self, accumulator):
        return accumulator

class ProcessTransactions(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'Parse CSV' >> beam.ParDo(ParseCsv())
            | 'Filter Transactions' >> beam.ParDo(FilterTransactions())
            | 'Sum by Date' >> beam.CombinePerKey(SumTransactionByDate())
            | 'Format Output' >> beam.Map(lambda x: f'{{"date": "{x[0]}", "total_amount": {x[1]:.2f}}}')
        )

def run():
    options = PipelineOptions()
    
    with beam.Pipeline(options=options) as p:
        (
            p 
            # | 'Read CSV' >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
            | 'Read CSV' >> beam.io.ReadFromText('dummy_data/transactions.csv', skip_header_lines=1)
            | 'Process Transactions' >> ProcessTransactions()
            | 'Write Results' >> beam.io.WriteToText('output/results.jsonl.gz', file_name_suffix='jsonl.gz')
        )

if __name__ == '__main__':
    run()
