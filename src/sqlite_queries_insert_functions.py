import sqlite3 as sql
import pandas as pd

class sqlite_con_queries_inserts:
    # Cursor is now created with class
    def __init__(self, path, queries):
        self.path = path
        self.connection = self.create_connection()
        self.cursor = self.connection.cursor()
        self.queries = queries

    # Connection function
    def create_connection(self):
        connection = None
        try:
            connection = sql.connect(self.path)
            print("Connection to SQLite DB successful")
        except sql.Error as e:
            print(f"The error '{e}' occurred")
        return connection

    # Query function inputting cursor with a list of queries.
    def execute_query(self, data=None, print_message=True):
        try:
            for query in self.queries:
                if data:
                    self.cursor.execute(query, data)
                else:
                    self.cursor.execute(query)
            # Commit only if all statements succeeded
            self.cursor.connection.commit()
            if print_message:
                print("Queries executed successfully")
        except sql.Error as e:
            print("An error occurred:", e)
            # Rollback the transaction in case of error
            self.cursor.connection.rollback()
    
    # Inerts static function
    @staticmethod
    def execute_inserts(table, query, path):
        # Convert DataFrame rows to list of dictionaries
        my_list = table.to_dict(orient='records')
        # Iterate through the list of dictionaries and insert data
        for row in my_list:
            try:
                sqlite_con_queries_inserts(path, query).execute_query(data=row, print_message=False)
            except sql.Error as e:
                print("An error occurred:", e)
        print("Insert query executed successfully")