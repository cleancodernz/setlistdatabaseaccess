import azure.functions as func
import logging
import pyodbc
import struct
import json
import os
import identity

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="setlistaccess")
def setlistaccess(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    dbname   = os.getenv('DB_NAME') # top level
    dbstring = os.getenv('DB_STRING') # next level
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    

    if name:

        

        # Connect to Azure SQL Database
        # conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
        #                     "Server=tcp:" + dbstring + ".database.windows.net,1433;"
        #                     "Database=" + dbname + ";"
        #                     "Uid=" + username + ";"
        #                     "Pwd=" + password + ";"
        #                     "Encrypt=yes;"
        #                     "TrustServerCertificate=no;"
        #                     "Connection Timeout=30;")
        conn = get_conn()
        cursor = conn.cursor()

        # Retrieve songs
        cursor.execute("SELECT song_name, artist, length, release_year FROM Songs")
        songs = cursor.fetchall()

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
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

def get_conn():
    connstring = os.getenv('DB_CONN_STRING')
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(connstring, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn

