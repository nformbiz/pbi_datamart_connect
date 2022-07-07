import urllib
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

SERVER = os.environ.get('SERVER')
DB = os.environ.get('DB')
USER = os.environ.get('USER')
PW = os.environ.get('PW')


def create_datamart_engine(SERVER: str, DB: str, USER: str, PW: str):
    conn_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        + "SERVER="
        + SERVER
        + ";DATABASE="
        + DB
        + ";UID="
        + USER
        + ";PWD="
        + PW
        + ";Authentication=ActiveDirectoryPassword"
    )

    datamart_connect_str = (
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(conn_string)
    )
    return create_engine(datamart_connect_str)


engine = create_datamart_engine(SERVER, DB, USER, PW)

records = engine.execute("SELECT TOP 100 * FROM myTable").fetchall()


print(len(records))
