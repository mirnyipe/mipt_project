# db.py
import json
import psycopg2
from sqlalchemy import create_engine


def load_cred():
    with open("cred.json", "r") as f:
        return json.load(f)


def get_engine():
    cred = load_cred()
    url = f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}"
    return create_engine(url)


def get_connection():
    cred = load_cred()
    return psycopg2.connect(
        user=cred["user"],
        password=cred["password"],
        host=cred["host"],
        port=cred["port"],
        database=cred["database"]
    )
