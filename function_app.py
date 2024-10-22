import azure.functions as func
import logging
import pyodbc
import struct
import json
import os
import pandas as pd
import io
from azure import identity
from azure.identity import DefaultAzureCredential

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="setlistaccess")
def setlistaccess(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    conn = get_conn()
    cursor = conn.cursor()

    logging.info("query: SELECT song_name, artist, length, release_year FROM Songs")

    # Retrieve songs
    cursor.execute("SELECT song_name, artist, length, release_year FROM Songs")
    songs = cursor.fetchall()

    logging.info(f'songs:{songs}' )


    # Format data
    song_list = []
    for song in songs:
        song_list.append({
            "song_name": song[0],
            "artist": song[1],
            "length": song[2],
            "release_year": song[3]
        })

    # Return data as JSON
    return func.HttpResponse(json.dumps(song_list), mimetype="application/json")


@app.route(route="uploadsongs")
def uploadsongs(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Read the uploaded file
        file = req.files.get('csvFile')
        overwrite = req.form['overwrite'] == 'true'

        if not file:
            return func.HttpResponse("CSV file is required", status_code=400)

        # Convert the file to a pandas DataFrame
        content = file.read().decode('utf-8')
        csv_data = io.StringIO(content)
        df = pd.read_csv(csv_data)

        # Ensure the CSV has the required columns
        required_columns = ['song_name', 'artist', 'length', 'release_year']
        if not all(col in df.columns for col in required_columns):
            return func.HttpResponse("Invalid CSV format. Required columns: song_name, artist, length, release_year", status_code=400)

        # Connect to Azure SQL Database
        conn = get_conn()
        cursor = conn.cursor()

        if overwrite:
            # Clear existing data in the table if overwrite is True
            cursor.execute("DELETE FROM Songs")
            conn.commit()

        # Insert the data into the Songs table
        for _, row in df.iterrows():
            cursor.execute("INSERT INTO Songs (song_name, artist, length, release_year) VALUES (?, ?, ?, ?)",
                           row['song_name'], row['artist'], row['length'], row['release_year'])

        # Commit the transaction
        conn.commit()

        return func.HttpResponse(
            json.dumps({"message": "CSV uploaded successfully and data inserted into the database."}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)

def get_conn():
    dbstring = os.getenv('DB_STRING') # top level conn string
    dbname   = os.getenv('DB_NAME') # next level    
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    
    # Connect to Azure SQL Database
    conn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                        "Server=tcp:" + dbstring + ".database.windows.net,1433;"
                        "Database=" + dbname + ";"
                        "Uid=" + username + ";"
                        "Pwd=" + password + ";"
                        "Encrypt=yes;"
                        "TrustServerCertificate=no;"
                        "Connection Timeout=30;")

    logging.info(f"connected to {conn}")

    return conn
