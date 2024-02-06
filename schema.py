import json

def read_schema_file(file_path: str = "C:\\Users\\rkali\\Desktop\\SSIS vs Python ETL\\schema.json") -> dict:
    try:
        with open(file_path, 'r') as file:
            schema = json.load(file)
        return schema
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return None