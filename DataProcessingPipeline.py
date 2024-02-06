import os
import pandas as pd
from datetime import datetime
import hashlib
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
import numpy as np
from config import read_config_file

register_adapter(np.int32, AsIs)

def load_data_and_add_metadata(file_path: str, schema: dict = None) -> pd.DataFrame:
    """
    This function reads data from an Excel file located at the specified path using pandas' `read_excel` method.
    It allows an optional schema dictionary to define data types during loading.
    Two new columns, '_Record_Source' and '_Load_Date', are then added to the DataFrame.
    '_Record_Source' contains the file path, and '_Load_Date' contains the current date and time.
    The resulting DataFrame is returned.

    Input:
    - file_path (str): The path to the Excel file.
    - schema (dict, optional): A dictionary specifying the data types for columns during loading.

    Output:
    - pd.DataFrame: A DataFrame containing the loaded data with additional '_Record_Source' and '_Load_Date' columns.

    """
    try:
        # Load excel data
        df = pd.read_excel(file_path, dtype = schema)
        # Add '_Record_Source' column
        df['_Record_Source'] = file_path
        # Add '_Load_Time' column
        load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['_Load_Date'] = load_time
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def trim_all_spaces(df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Iterates through the columns of a DataFrame and removes leading and trailing white-space characters for object data types.

    Input:
    - df (pd.DataFrame): The input DataFrame to be processed.

    Output:
    - pd.DataFrame: A new DataFrame with white-spaces trimmed for object data types.
    """
    def remove_whitespaces(text: str) -> str:
        text_clean = text.strip()
        return text_clean
    
    try: 
        assert isinstance(df, pd.DataFrame), "Error: Input must be a pandas DataFrame"
    except AssertionError as e:
        print(e)
        return None

    for column_name in df.columns:
        if df[column_name].dtype == 'O':  # 'O' oznacza object
            df[column_name] = df[column_name].map(remove_whitespaces)

    return df      

def data_validation(df: pd.DataFrame, save_invalid: bool = False) -> pd.DataFrame:
    """
    Validates a DataFrame against specified business rules and optionally saves invalid rows to a CSV file.

    Input:
    - df (pd.DataFrame): The input DataFrame to be validated.
    - save_invalid (bool, optional): If True, saves invalid rows to a CSV file (default is False) in the 'Error Output' folder.
    The CSV file is named 'invalid_data_<current_date_time>.csv'.

    Output:
    - pd.DataFrame: A new DataFrame with invalid rows removed.

    Business Rules:
    1. Cost Object Type must be one of ('Cost Center', 'WBS', 'COPA').
    2. Year must be greater than or equal to 2000.
    3. Period must be in the range [1, 12].
    """
    # Ignore SettingWithCopyWarning
    pd.options.mode.chained_assignment = None  

    # Rule 1: Cost Object Type: ('Cost Center', 'WBS', 'COPA')
    valid_cost_object_types = {'Cost Center', 'WBS', 'COPA'}
    invalid_rows_rule1 = df[~df['Cost_Object_Type'].isin(valid_cost_object_types)]
    invalid_rows_rule1['Error_Type'] = 'Invalid Cost Object Type'

    # Rule 2: Valid Year format: >= 2000
    invalid_rows_rule2 = df[df['Year'] < 2000]
    invalid_rows_rule2['Error_Type'] = 'Invalid Year Format'
    
    # Rule 3: Valid Period format: [1, 12]
    invalid_rows_rule3 = df[(df['Period'] < 1) | (df['Period'] > 12)]
    invalid_rows_rule3['Error_Type'] = 'Invalid Period Format'
   
    invalid_rows_combined = pd.concat([invalid_rows_rule1, invalid_rows_rule2, invalid_rows_rule3])

    if save_invalid and not invalid_rows_combined.empty:
        # Creating Error Output folderif it does not exist
        error_output_folder = "Error Output"
        if not os.path.exists(error_output_folder):
            os.makedirs(error_output_folder)

        invalid_df = pd.concat([invalid_rows_rule1, invalid_rows_rule2, invalid_rows_rule3])

        # Create a unique file name based on date and time
        error_file_path = os.path.join(error_output_folder, f'invalid_data_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv')
        
        invalid_df.to_csv(error_file_path, index=True)

    # Drop invalid records from the df and reset index
    df = df.drop(index=invalid_rows_combined.index).reset_index(drop=True)

    return df

def hash_transformation(df):
    """
    Perform MD5 hash transformation on the specified columns of a DataFrame.

    Input:
    - df (pd.DataFrame): The input DataFrame.

    Output:
    - pd.DataFrame: A new DataFrame with specified columns transformed using MD5 hashing.
    """

    def md5_hash(input):
        
        # None replacement
        if input is None:
            input = '_'

        # Convert data to a string before hashing
        input = str(input)

        # Create an MD5 hash objec
        md5_hash_object = hashlib.md5()

        # Update the hash object with the bytes of the input text
        md5_hash_object.update(input.encode('utf-8'))

        # Get the hexadecimal representation of the hash
        hashed_text = md5_hash_object.hexdigest()

        return hashed_text

    # Creating _HSKs and additional columns
    df['CONTROLLING_AREA_HSK'] = df['Controlling_Area'].apply(md5_hash)

    df['COMPANY_CODE_HSK'] = df['Company_Code'].apply(md5_hash)

    df['COST_OBJECT_HSK'] = df.apply(lambda row: md5_hash(row['Cost_Object'] + '|' + row['Controlling_Area'])
                                     if row['Cost_Object_Type'] == 'Cost Center'
                                     else md5_hash(row['Cost_Object']),
                                     axis = 1) # rowwise apply
    
    df['COST_ELEMENT_CTR_AREA_HSK'] = (df['Cost_Element'].astype(str) + df['Controlling_Area']).apply(md5_hash)

    df['CURRENCY_HSK'] = df['Currency'].apply(md5_hash)

    df['FISCAL_PERIOD'] = df['Year'].astype(str) + df['Period'].astype(str).str.zfill(2) + '01'

    return df

def write_to_postgres(df):
    """
    Write DataFrame to a PostgreSQL database table named 'manual_upload_tlnk'.

    Parameters:
    - df (pd.DataFrame): The DataFrame to be written to the database.

    This function establishes a connection to a PostgreSQL database using parameters retrieved from the `read_config_file()` function.
    It then truncates the existing 'manual_upload_tlnk' table in the database and performs an INSERT operation to populate the table with data from the input DataFrame.
    The SQL query for truncation is executed first to remove all existing records from the table.
    Subsequently, the function iterates over the rows of the input DataFrame, converts each row into a tuple, and executes an INSERT query to add the row's data to the 'manual_upload_tlnk' table.
    The operation is committed, and the number of processed rows is printed.
    In case of an error during the database operation, an exception message is printed.
    Finally, the database connection is closed.

    Note: The structure of the 'manual_upload_tlnk' table and the SQL INSERT query should match the columns of the input DataFrame.
    """
    try:
        connection = None
        params = read_config_file()
        print('Connecting to the postgreSQL database ...')
        connection = psycopg2.connect(**params) # extracting everything from params to the .connect() method

        records = df.to_records(index = False)

        # Create a cursor
        cursor = connection.cursor()

        # Truncate the table
        truncate_query = "TRUNCATE TABLE manual_upload_tlnk;"
        cursor.execute(truncate_query)
        print('Successfully truncated the MANUAL_UPLOAD_TLNK table.')
              
        insert_query = """
        INSERT INTO manual_upload_tlnk 
            (
                controlling_area,
                company_code,
                cost_object,
                cost_object_type,
                cost_element,
                year,
                period,
                currency,
                amount,
                _record_source,
                _load_date,
                controlling_area_hsk,
                company_code_hsk,
                cost_object_hsk,
                cost_element_ctr_area_hsk,
                currency_hsk,
                fiscal_period
            ) 
        VALUES %s;
        """

        psycopg2.extras.execute_values(cursor, insert_query, records, page_size = 100)

        #processed_rows = 0 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        
        # for index, row in df.iterrows():
        #     # Makes tuples from rows
        #     values = tuple(row)
        #     # Execute INSERT statement
        #     cursor.execute(insert_query, values)
        #     processed_rows += 1

        # Commit changes
        connection.commit()

        print(f'Successfully inserted {len(records)} rows into the MANUAL_UPLOAD_TLNK table.')

        cursor.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')


class DataProcessingPipeline:
    """
    A class representing a data processing pipeline for loading, transforming, and writing data.

    This class encapsulates a series of data processing steps that can be applied sequentially to a DataFrame.
    The pipeline consists of the following methods:

    - `load_data(file_path, schema=None)`: Loads data from an Excel file into a DataFrame and adds metadata columns.
    - `trim_spaces()`: Trims leading and trailing white-space characters from object data types in the DataFrame.
    - `validate_data(save_invalid=False)`: Performs data validation against predefined business rules, optionally saving invalid records to a CSV file.
    - `hash_transform()`: Applies MD5 hash transformation to specified columns of the DataFrame.
    - `write_to_postgres()`: Writes the DataFrame to a PostgreSQL database table.

    Usage:
    ```
    pipeline = DataProcessingPipeline()
    pipeline.load_data(file_path='example.xlsx', schema = "schema.json")\
           .trim_spaces()\
           .validate_data(save_invalid = False)\
           .hash_transform()\
           .write_to_postgres()
    ```

    Each method updates the internal DataFrame (`self.df`), allowing for a flexible and customizable data processing flow.
    """
    def __init__(self):
        self.df = None

    def load_data(self, file_path, schema=None):
        self.df = load_data_and_add_metadata(file_path, schema)
        return self

    def trim_spaces(self):
        if self.df is not None:
            self.df = trim_all_spaces(self.df)
        return self

    def validate_data(self, save_invalid=False):
        if self.df is not None:
            self.df = data_validation(self.df, save_invalid)
        return self

    def hash_transform(self):
        if self.df is not None:
            self.df = hash_transformation(self.df)
        return self

    def write_to_postgres(self):
        if self.df is not None:
            write_to_postgres(self.df)
        return self