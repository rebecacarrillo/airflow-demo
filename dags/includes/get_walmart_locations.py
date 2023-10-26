import requests
import sqlite3
import json
import csv

# Kaggle API endpoint for the Walmart Locations dataset
kaggle_dataset_url = "https://www.kaggle.com/debdutta/covid19-dataset-in-india/download"

# Load Kaggle API credentials from a JSON file
with open("kaggle.json") as json_file:
    kaggle_credentials = json.load(json_file)

# Set your Kaggle API credentials
kaggle_username = kaggle_credentials["username"]
kaggle_api_key = kaggle_credentials["key"]

# Define the SQLite database file and connection
db_file = "walmart_locations.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS walmart_locations (
        store_number INTEGER PRIMARY KEY,
        store_name TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT
    )
''')

# Fetch the Walmart location data from Kaggle
response = requests.get(kaggle_dataset_url, auth=(kaggle_username, kaggle_api_key))

if response.status_code == 200:
    # Assuming the data is in CSV format, you can use a CSV parsing library
    # to read and insert the data into the SQLite database. For example, you can use the built-in `csv` module.
    # Here, we'll assume you have a CSV file named 'walmart_locations.csv'.

    # Read the CSV and insert data into the database
    with open('walmart_locations.csv', 'r') as csv_file:
        next(csv_file)  # Skip the header row
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO walmart_locations (store_number, store_name, address, city, state, zip_code)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (row[0], row[1], row[2], row[3], row[4], row[5]))

    # Commit the changes and close the database connection
    conn.commit()
    conn.close()

    print("Data loaded into SQLite database successfully.")
else:
    print("Failed to retrieve the dataset from Kaggle. Status code:", response.status_code)
