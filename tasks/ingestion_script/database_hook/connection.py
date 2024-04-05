import psycopg2
import psycopg2.extras as extras 
from .config import load_config

class PostgreSQLConnection:
    def __init__(self, config=None):
        self.config = load_config()
        print(self.config)
        self.conn = None

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            self.conn = psycopg2.connect(**self.config)
            print('Connected to the PostgreSQL server.')
        except (psycopg2.DatabaseError, Exception) as error:
            print(error)

    def disconnect(self):
        """ Disconnect from the PostgreSQL database server """
        if self.conn is not None:
            self.conn.close()
            print('Disconnected from the PostgreSQL server.')
        else:
            print('Connection is not established.')

    def execute_query(self, query):
        """ Execute a SQL query """
        if self.conn is None:
            print("No connection established. Cannot execute query.")
            return
        try:
            cursor = self.conn.cursor()
            try:
                cursor.execute(query)
            except:
                print(query)
                return False
            if query.strip().upper().startswith('SELECT'):
                rows = cursor.fetchall()
                for row in rows:
                    print(row)
            else:
                print("Query executed successfully.")
                self.conn.commit()
                print(cursor.fetchone())
            cursor.close()
        except (psycopg2.DatabaseError, Exception) as error:
            print(error)
        return True
    
