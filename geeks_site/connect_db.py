import psycopg2
import random
from datetime import datetime, timedelta

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
"""insert_query = 
    INSERT INTO candlestick_data (timestamp, open_price, high_price, low_price, close_price, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
   ON CONFLICT (timestamp) DO NOTHING;
   # """
#Loop through each candle data and insert it
for candle in data["candles"]:
    cursor.execute(insert_query, candle)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully!")








# ✅ Connect to PostgreSQL Database
# conn = psycopg2.connect(
#     dbname="postgres",
#     user="aryanpatel",
#     password="12345",
#     host="localhost",
#     port="5432"
# )
# print("✅ Connected to database successfully!")
# cursor = conn.cursor()

# # ✅ Ensure Table Exists (With `TIMESTAMPTZ` for `timestamp`)
# cursor.execute("""
# CREATE TABLE IF NOT EXISTS candlestick_data (
#     timestamp TIMESTAMPTZ PRIMARY KEY,  -- ✅ Store as TIMESTAMPTZ instead of BIGINT
#     open_price FLOAT,
#     high_price FLOAT,
#     low_price FLOAT,
#     close_price FLOAT,
#     volume INT
# );
# """)
# conn.commit()

# # ✅ Generate 8 Years of 10-Minute Interval Data
# start_time = datetime.utcnow() - timedelta(days=8*365)  # Start from 8 years ago
# interval = timedelta(minutes=10)  # 10-minute interval
# data = []
# current_time = start_time  # Start at the correct time

# # ✅ Print the Start Time to Debug
# print("Start Time (Readable):", current_time.strftime('%d-%b-%Y %H:%M:%S'))
# print("Start Time (UNIX Timestamp):", int(current_time.timestamp()))

# # ✅ Generate Data
# while current_time <= datetime.utcnow():
#     timestamp = current_time  # ✅ Keep timestamp as `datetime` (TIMESTAMPTZ)
#     open_price = round(random.uniform(400, 500), 2)  # Random price between 400-500
#     high_price = round(open_price + random.uniform(0, 5), 2)  # High is slightly higher
#     low_price = round(open_price - random.uniform(0, 5), 2)  # Low is slightly lower
#     close_price = round(random.uniform(low_price, high_price), 2)  # Close is within range
#     volume = random.randint(500, 5000)  # Random volume between 500-5000

#     data.append((timestamp, open_price, high_price, low_price, close_price, volume))
#     current_time += interval  # Move to next 10-minute interval

# # ✅ Print Total Records Generated
# print(f"✅ Generated {len(data)} records for 8 years.")

# # ✅ SQL Query to Insert Data (Fixing `timestamp` type)
# insert_query = """
# INSERT INTO candlestick_data (timestamp, open_price, high_price, low_price, close_price, volume)
# VALUES (%s, %s, %s, %s, %s, %s)
# ON CONFLICT (timestamp) DO NOTHING;
# """

# # ✅ Insert Data into PostgreSQL
# cursor.executemany(insert_query, data)
# conn.commit()

# print(f"✅ {len(data)} records inserted successfully!")

# # ✅ Fetch & Display Latest Data
# cursor.execute("""
# SELECT 
#     to_char(timestamp AT TIME ZONE 'Asia/Kolkata', 'DD-Mon-YYYY HH12:MI AM') AS ist_time, 
#     open_price, high_price, low_price, close_price, volume
# FROM candlestick_data
# ORDER BY timestamp DESC
# LIMIT 10;
# """)
# rows = cursor.fetchall()

# print("\n✅ Latest 10 Records:")
# for row in rows:
#     print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]}")

# # ✅ Close Connection
# cursor.close()
# conn.close()
# print("✅ Database connection closed!")
