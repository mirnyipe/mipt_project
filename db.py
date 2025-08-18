from sqlalchemy import create_engine
import json
import psycopg2



def get_connections():
    with open("cred.json", "r") as f:
        cred = json.load(f)

    url = f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}"
    engine = create_engine(url)
    
    conn = psycopg2.connect(**cred)
    cursor = conn.cursor()
    cursor.execute("""set search_path to stg, dwh, meta""")

    return engine, conn, cursor