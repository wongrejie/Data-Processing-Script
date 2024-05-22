import datetime
import json
import psycopg2
import subprocess
import os

SETTING_DIR = os.path.dirname(os.path.abspath(__file__))

def read_paths_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
        return data

def json_file_data():
    return {
        "db_host": "",
        "db_port": "",
        "db_name": "",
        "db_user": "",
        "db_password": "",
        "backup_folder": "",
    }

def create_json_file(log_file):
    data = json_file_data()
    setting_path = find_settings_file()

    try:
        with open(setting_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        log("Json File created, please key in the information", log_file)
    except Exception as e:
        log(f"Error creating JSON file: {str(e)}", log_file)
        exit(1)

    return setting_path
   
def log(message, log_file):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] {message}"
    with open(log_file, 'a') as log_file:
        log_file.write(log_message + '\n')
    print(log_message)

def find_settings_file():
    for root, _, files in os.walk(SETTING_DIR):
        for file_name in files:
            if file_name == 'setting.json':
                return os.path.join(root, file_name)
    return None

def connect_to_postgresql(data, log_file):
    try:
        connection = psycopg2.connect(
            host=data['db_host'],
            port=data['db_port'],
            database=data['db_name'],
            user=data['db_user']
        )
    except psycopg2.Error as e:
        log("Error connecting to the database:", log_file)
        exit(1)
    return connection

def execute_query(query, cursor):
    cursor.execute(query)
    return cursor.fetchall()

def perform_backup(data, log_file):
    query_list = [
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public'", 
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'VIEW'",
        "SELECT proname FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')",
    ]

    for index, query in enumerate(query_list):
        current_time = datetime.datetime.now()
        timestamp_str = current_time.strftime("%Y-%m-%d")

        # Determine folder name based on the query
        if index == 0:
            folder_name = "tables"
        elif index == 1:
            folder_name = "views"
        elif index == 2:
            folder_name = "functions"

        backup_folder = os.path.join(
            data['backup_folder'], timestamp_str, data['db_name'], folder_name)
        os.makedirs(backup_folder, exist_ok=True)
        connection = connect_to_postgresql(data, log_file)
        cursor = connection.cursor()

        log('Backup START', log_file)

        # Backup for functions
        if index == 2:
            functions = execute_query(query, cursor)
            for function in functions:
                function_name = function[0]
                backup_file = os.path.join(backup_folder, f"{function_name}.sql")
                try:
                    function_query = f"SELECT pg_get_functiondef(pg_proc.oid) FROM pg_proc WHERE proname = '{function_name}'"
                    cursor.execute(function_query)
                    function_sql = cursor.fetchone()[0]
                    with open(backup_file, 'w') as sql_file:
                        sql_file.write(function_sql)
                    log(f"Backup for function {function_name} completed successfully.", log_file)
                except psycopg2.Error as e:
                    log(f"Error creating backup for function {function_name}: {str(e)}", log_file)
        else:
            # Backup for tables and views 
            data_names = [row[0] for row in execute_query(query, cursor)]
            for data_name in data_names:
                data_sql_file = os.path.join(backup_folder, f"{data_name}.sql")
            
                try:
                    subprocess.run([
                        data['pg_dump_path'],
                        "--host", data['db_host'],
                        "--port", str(data['db_port']),
                        "--dbname", data['db_name'],
                        "--username", data['db_user'],
                        "--file", data_sql_file,
                        "--schema", "public",
                        "--table", f'"{data_name}"',
                    ])
                    log(f"Backup for data {data_name} completed successfully.", log_file)
                except subprocess.CalledProcessError as e:
                    log(f"Error creating backup for data {data_name}: {str(e)}", log_file)

        connection.close()
        log("Backup END ::: please check the backup folder to get the value", log_file)

def main():
    LOG_FILE_PATH = os.path.join(SETTING_DIR,'log.txt')
    if os.path.exists(os.path.join(SETTING_DIR, 'setting.json')):
        SETTING_PATH = os.path.join(SETTING_DIR, 'setting.json')
    else:
        # create json file if not exist
        SETTING_PATH = create_json_file(LOG_FILE_PATH)

    with open(SETTING_PATH, 'r') as file:
        data = json.load(file)

    os.environ['PGPASSWORD'] = data['db_password']

    perform_backup(data, LOG_FILE_PATH)

if __name__ == "__main__":
    main()
