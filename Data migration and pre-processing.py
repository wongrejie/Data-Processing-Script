import os
import json
import datetime
from sqlalchemy import create_engine 
import pandas as pd
import inspect
import sys
import psycopg2

SETTING_DIR = os.path.dirname(os.path.abspath(__file__))

# executable_path = sys.argv[0] if getattr(sys, 'frozen', False) else sys.executable
# executable_directory = os.path.dirname(executable_path)
# SETTING_DIR = executable_directory

def create_folder(path):
    os.makedirs(path, exist_ok=True)


def connect_to_postgresql(connection_path):
    try:
        connection = create_engine(connection_path)
        con = connection.connect()
        if con:
            log(f"Connected to :{connection}")
        return con
    except Exception as e:
        log(f"Error connecting to PostgreSQL: {str(e)}")


def log(*args):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    caller_frame = inspect.stack()[1]
    caller_line_number = caller_frame.lineno
    log_message = f"[{timestamp}] Code Line:{caller_line_number} {' '.join(map(str, args))}"
    LOG_FILE_PATH = os.path.join(SETTING_DIR, 'log.txt')
    with open(LOG_FILE_PATH, 'a') as log_file:
        log_file.write(log_message + '\n')
    print(log_message)


def get_path(SETTING_DIR):
    SETTING_FILE = os.path.join(SETTING_DIR, 'setting.json')

    if os.path.exists(SETTING_FILE):
        with open(SETTING_FILE, 'r') as setting_data:
            data = json.load(setting_data)
        if data and any(data.values()):
            return data
        else:
            log("Please key in the details in the setting file")
    else:
        data = {
            "origin_path": "",
            "destination_path": "",
            "origin_name": "",
            "destination_name": "",
            "function_name": "",
            "function_name2": "",
        }
        SETTING_PATH = os.path.join(SETTING_DIR, 'setting.json')
        with open(SETTING_PATH, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        log(
            f"Software created a setting file at this directory {SETTING_PATH} please key in the details in the setting file")


def get_postgres_paths():
    setting_data = get_path(SETTING_DIR)
    return (
        setting_data.get("origin_path", ""),
        setting_data.get("destination_path", ""),
        setting_data.get("origin_name", ""),
        setting_data.get("destination_name", ""),
        setting_data.get("function_name", ""),
        setting_data.get("function_name2", ""),
       
    )


origin_path, destination_path, origin_name, destination_name, function_name,function_name2 = get_postgres_paths()

origin_conn = connect_to_postgresql(origin_path)
tables_data = {}  

with origin_conn:

    query_top_7_dates = f'''
            SELECT DISTINCT CONVERT(DATE, trans_date) AS max_dates
            FROM "{origin_name}"
            ORDER BY max_dates DESC
            OFFSET 0 ROWS FETCH NEXT 7 ROWS ONLY
        '''
    query_top_7_dates_data = f'''
            SELECT * 
            FROM "{origin_name}" 
            WHERE CONVERT(DATE, trans_date) IN (
                SELECT DISTINCT CONVERT(DATE, trans_date) AS max_dates
                FROM "{origin_name}"
                ORDER BY max_dates DESC
                OFFSET 0 ROWS FETCH NEXT 7 ROWS ONLY
                ) 
                ORDER BY trans_date,[JobSuffix],oper_num'''
    
    top_7_dates = pd.read_sql_query(query_top_7_dates, origin_conn)['max_dates']
    updated_data_in_origin = pd.read_sql_query(query_top_7_dates_data, origin_conn)
    

        # Get the second top date as minimum_date
    minimum_date = top_7_dates.iloc[-1]
    
def replace_null_resources(row):
    if pd.isnull(row["Resources"]) and row["trans_type"] in ['Move', 'Run']:
        machine_data = updated_data_in_origin[
            (updated_data_in_origin["JobSuffix"] == row["JobSuffix"]) &
            (updated_data_in_origin["oper_num"] == row["oper_num"]) &
            (updated_data_in_origin["trans_type"] == 'Machine')
        ]

        if not machine_data.empty:
            last_machine_resources = machine_data["Resources"].last_valid_index()
            if last_machine_resources is not None:
                new_resource = updated_data_in_origin.loc[last_machine_resources, "Resources"]
                print(f"Replacing null in row {row.name} with last valid 'Machine' resource: {new_resource}")
                return(new_resource.upper().replace(" ", ""))[:7]
                
        # If no "Machine" trans_type is found or last_machine_resources is None
        return 'MANUAL PROCESS'  
    else:
        return (row["Resources"].upper().replace(" ", ""))[:7] if pd.notnull(row["Resources"]) else row["Resources"]

 # Apply the function to each row
updated_data_in_origin["updated_resources"] = updated_data_in_origin.apply(replace_null_resources, axis=1)  

destination_conn = connect_to_postgresql(destination_path)

with destination_conn:

    query_delete_data = f'''
            DELETE FROM "{destination_name}"
            WHERE DATE(trans_date) >= '{minimum_date}'
        '''
    destination_conn.execute(query_delete_data)
    log(f"Deleted data with trans_date >= {minimum_date} from {destination_name}")

    try:
        updated_data_in_origin.to_sql(destination_name, destination_conn, index=False, if_exists='append')
        log("Data inserted into the database.")
    except pd.io.sql.DatabaseError as e:
        log(f"Error inserting data into the database: {str(e)}")

try:
   
    connection = psycopg2.connect(destination_path)
    cursor = connection.cursor()

    # Executing the first function
    query = f"SELECT {function_name}();"
    cursor.execute(query)
    result = cursor.fetchone()
    log("Function result real-time:", result)

    # Commit the transaction
    connection.commit()

    # Executing the second function
    query2 = f"SELECT {function_name2}();"
    cursor.execute(query2)
    result2 = cursor.fetchone()
    log("Function result historical:", result2)

    # Commit the transaction
    connection.commit()

except psycopg2.Error as e:
    log("Error connecting to the database:", e)

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()



