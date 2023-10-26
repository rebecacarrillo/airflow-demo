import sqlite3
import csv

# Define the CSV file path and SQLite database file path
csv_file = 'data.csv'
db_file = 'data.db'

# Connect to the SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create a table in the database to store the data
cursor.execute('''CREATE TABLE IF NOT EXISTS data (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    age INTEGER,
                    city TEXT
                )''')

# Commit the table creation to the database
conn.commit()

# Open the CSV file for reading
with open(csv_file, 'r') as csvfile:
    csv_reader = csv.reader(csvfile)
    next(csv_reader)  # Skip the header row if it exists

    # Insert data from the CSV file into the database
    # Okay, we definitely would prefer to do a load
    for row in csv_reader:
        cursor.execute('''INSERT INTO data (name, age, city)
                          VALUES (?, ?, ?)''', (row[0], int(row[1]), row[2]))

# Commit the changes and close the database connection
conn.commit()
conn.close()

print("Data has been loaded from the CSV file into the SQLite database.")
