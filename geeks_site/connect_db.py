import psycopg2
import json

file_path = "/Users/aryanpatel/Desktop/Python/Responses/response.txt"

with open(file_path, "r") as file:
    data = json.load(file)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",    # Change to your database name
    user="aryanpatel",          # Change to your username
    password="12345",  # Change to your password
    host="localhost",          # Change if using a remote server
    port="5432"                # Default PostgreSQL port
)
print("Connected to database successfully!")
cursor = conn.cursor()

# SQL query to insert data
insert_query = """
    INSERT INTO candlestick_data (timestamp, open_price, high_price, low_price, close_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (timestamp) DO NOTHING;
    """
# Loop through each candle data and insert it
for candle in data["candles"]:
    cursor.execute(insert_query, candle)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully!")
