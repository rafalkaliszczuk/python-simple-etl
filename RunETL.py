import argparse
from datetime import datetime
from DataProcessingPipeline import DataProcessingPipeline
from schema import read_schema_file

start_time = datetime.now()
print(f'{start_time} Start extracting data')

parser = argparse.ArgumentParser(description = 'Arguments for loading Manual Upload excel file')
parser.add_argument('--file_path', type = str, default = "Manual Upload.xlsx", help = 'Path to the file location')
parser.add_argument('--schema_file', type = str, default = "schema.json", help = 'Path to the schema file')
parser.add_argument('--save_invalid', action = 'store_true', help = 'True/False (default) - flag argument')

args = parser.parse_args()

file_path = args.file_path
schema_file = args.schema_file
save_invalid = args.save_invalid

print(f'--file_path: {file_path}')
print(f'--schema_file: {schema_file}')
print(f'--save_invalid: {save_invalid}')

schema = read_schema_file(schema_file)

pipeline = DataProcessingPipeline()
pipeline.load_data(file_path = file_path, schema = schema)\
       .trim_spaces()\
       .validate_data(save_invalid = save_invalid)\
       .hash_transform()\
       .write_to_postgres()

end_time = datetime.now()
time_diff = end_time - start_time
print(f'{end_time} Finished writing data. Elapsed time: {time_diff}')




