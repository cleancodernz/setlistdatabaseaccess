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

# get songs
@app.route(route="setlistaccess")
def setlistaccess(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Getting active songs in setlistaccess function')
    
    conn = get_conn()
    cursor = conn.cursor()

    logging.info("query: SELECT song_name, artist, length, release_year FROM Songs where active=1")

    # Retrieve songs
    cursor.execute("SELECT song_name, artist, length, release_year FROM Songs where active=1")
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


# Upload Songs vis CSV file
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

        # Insert the data into the Songs table (set all songs to a default of active)
        for _, row in df.iterrows():
            cursor.execute("INSERT INTO Songs (song_name, artist, length, release_year, active) VALUES (?, ?, ?, ?, ?)",
                           row['song_name'], row['artist'], row['length'], row['release_year'], 1)

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

# add songs
@app.route(route="addSong")
def addSong(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Add song request processed.')

    # Parse the incoming JSON
    try:
        req_body = req.get_json()
        song_name = req_body.get('song_name')
        artist = req_body.get('artist')
        length = req_body.get('length')
        release_year = req_body.get('release_year')
    except ValueError:
        return func.HttpResponse("Invalid input", status_code=400)
    
    # Validate that we have all required fields
    if not all([song_name, artist, length, release_year]):
        return func.HttpResponse("Missing song details", status_code=400)

    try:
        # Connect to Azure SQL Database
        conn = get_conn()
        cursor = conn.cursor()

        # Insert the new song into the Songs table
        cursor.execute("""
            INSERT INTO Songs (song_name, artist, length, release_year, active) 
            VALUES (?, ?, ?, ?, ?)
        """, (song_name, artist, length, release_year, 1))  # Active by default

        conn.commit()

    except Exception as e:
        logging.error(f"Error inserting song: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    finally:
        cursor.close()
        conn.close()

    return func.HttpResponse("Song added successfully", status_code=201)

# change song status
@app.route(route="toggleSongStatus")
def toggleSongStatus(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Toggle song status request processed.')

    try:
        req_body = req.get_json()
        song_id = req_body.get('songId')
        is_active = req_body.get('isActive')
    except ValueError:
        return func.HttpResponse("Invalid input", status_code=400)

    if song_id is None or is_active is None:
        return func.HttpResponse("Missing song ID or active status", status_code=400)

    try:
        # Connect to Azure SQL Database
        conn = get_conn()
        cursor = conn.cursor()

        # Toggle song status in the database
        cursor.execute("""
            UPDATE Songs 
            SET active = ? 
            WHERE id = ?
        """, (1 if not is_active else 0, song_id))

        conn.commit()

    except Exception as e:
        logging.error(f"Error updating song status: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    finally:
        cursor.close()
        conn.close()

    return func.HttpResponse("Song status updated successfully", status_code=200)

# get songs main function
@app.route(route="get_songs")
def get_songs(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Get songs request processed.')

    # Check if the request is for active or inactive songs
    is_active = req.params.get('active', 'true').lower() == 'true'

    songs, error = get_songs_action(is_active)
    
    if error:
        return func.HttpResponse(f"Error: {error}", status_code=500)

    return func.HttpResponse(json.dumps(songs), status_code=200, mimetype="application/json")

# get_songs (private method)
def get_songs_action(is_active):
    try:
        # Connect to Azure SQL Database
        conn = get_conn()
        cursor = conn.cursor()

        # Fetch active/inactive songs
        cursor.execute("""
            SELECT id, song_name, artist, length, release_year 
            FROM Songs 
            WHERE active = ?
        """, (1 if is_active else 0))
        
        rows = cursor.fetchall()
        songs = [{"id": row[0], "song_name": row[1], "artist": row[2], "length": row[3], "release_year": row[4]} for row in rows]

    except Exception as e:
        logging.error(f"Error fetching songs: {str(e)}")
        return None, str(e)

    finally:
        cursor.close()
        conn.close()

    return songs, None

# get database connection
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
