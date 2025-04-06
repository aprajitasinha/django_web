import os
import datetime
import json
import psycopg2
from psycopg2.extras import execute_values

# Database connection details
DB_CONFIG = {
    "dbname": "data_growth",
    "user": "aryanpatel",
    "password": "12345",
    "host": "localhost",
    "port": "5432",
}
# PostgreSQL query with ON CONFLICT (Assuming "timestamp" is UNIQUE)

query = """
    INSERT INTO sbin.sbin_two_minute (timestamp, open_price, high_price, low_price, close_price, volume)
    VALUES %s
    ON CONFLICT (timestamp) 
    DO UPDATE SET open_price = EXCLUDED.open_price, 
                  high_price = EXCLUDED.high_price, 
                  low_price = EXCLUDED.low_price, 
                  close_price = EXCLUDED.close_price, 
                  volume = EXCLUDED.volume;
"""
# Schema name (fixed for all tables)
SCHEMA_NAME = "sbin"

# Directory containing JSON files
DATA_DIR = "/Users/aryanpatel/Desktop/Python/Responses/"

# Function to connect to the database
def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Connected to database successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to process a single file
def process_file(file_path):
    file_name = os.path.basename(file_path)  # Get filename from path
    table_name = file_name.replace("response_", "").replace(".txt", "")  # Extract table name
    full_table_name = f"{SCHEMA_NAME}.{table_name}"  # Format schema.table_name

    try:
        with open(file_path, "r") as file:
            data = json.load(file)  # Load JSON data
        print(f"Loaded JSON file: {file_path}")
    except Exception as e:
        print(f"Error loading JSON file {file_path}: {e}")
        return

    # Validate JSON structure
    if not isinstance(data, dict) or "candles" not in data or not isinstance(data["candles"], list):
        print(f"Invalid JSON structure in {file_path}. Skipping...")
        return

    # Convert data into tuples for insertion
    values = [
        (
            datetime.datetime.utcfromtimestamp(candle[0]),  # Convert UNIX timestamp to datetime
            candle[1] if candle[1] is not None else 0.0,   # Handle None values
            candle[2] if candle[2] is not None else 0.0,  # High price
            candle[3] if candle[3] is not None else 0.0,  # Low price
            candle[4] if candle[4] is not None else 0.0,  # Close price
            candle[5] if candle[5] is not None else 0.0  # Volume
        )
        for candle in data["candles"]
        if isinstance(candle, list) and len(candle) == 6  # Ensure correct format
    ]

    if not values:
        print(f"No valid data found in {file_path}. Skipping...")
        return

    # SQL Query for Bulk Insert (including schema)
    insert_query = f"""
        INSERT INTO {full_table_name} 
        (timestamp, open_price, high_price, low_price, close_price, volume)
        VALUES %s
        ON CONFLICT (timestamp) DO NOTHING;
    """

    # Connect to the database
    conn = connect_db()
    if not conn:
        return
    cursor = conn.cursor()

    # Check if table exists
    try:
        cursor.execute("SELECT to_regclass(%s);", (full_table_name,))
        table_exists = cursor.fetchone()[0] is not None

        if not table_exists:
            print(f"Table '{full_table_name}' does not exist. Skipping file {file_path}...")
            cursor.close()
            conn.close()
            return
    except Exception as e:
        print(f"Error checking table {full_table_name}: {e}")
        cursor.close()
        conn.close()
        return

    # Insert data
    try:
        execute_values(cursor, insert_query, values)
        conn.commit()
        print(f"Successfully inserted data into {full_table_name} from {file_path}")
    except Exception as e:
        print(f"Error inserting data into {full_table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to process all files in the directory
def process_all_files():
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("response_") and f.endswith(".txt")]

    if not files:
        print("No JSON files found in the directory.")
        return

    for file_name in files:
        file_path = os.path.join(DATA_DIR, file_name)
        print(f"\nProcessing file: {file_name}")
        process_file(file_path)

# Run the script
if __name__ == "__main__":
    process_all_files()
