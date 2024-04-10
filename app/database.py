import psycopg2
from psycopg2.extras import RealDictCursor

class Connection():
    try:
        conn = psycopg2.connect(host='localhost',database='obsrv',user='postgres',password='drakshitha',cursor_factory=RealDictCursor)
        cursor = conn.cursor()
        print("Database connection was succesfull")
    except Exception as error:
        print("Failed to connect Database")
        print("Error: ",error)

connection = Connection()