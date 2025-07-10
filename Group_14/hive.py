# 

from pyhive import hive

import os
class HiveTripleStore:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.database = None
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = hive.connect(host=self.host, port=self.port)
            self.cursor = self.conn.cursor()
            print("Connected to Hive server successfully.")
        except Exception as e:
            print(f"Failed to connect to Hive server: {e}")

    def connect_to_db(self, db_name):
        if self.conn:
            self.disconnect()
        try:
            self.conn = hive.connect(host=self.host, port=self.port, database=db_name)
            self.cursor = self.conn.cursor()
            print(f"Connected to database {db_name}.")
        except Exception as e:
            print(f"Failed to connect to database {db_name}: {e}")

    def disconnect(self):
        try:
            if self.cursor:
                self.cursor.close()
                print("Cursor closed.")
            if self.conn:
                self.conn.close()
                print("Connection closed.")
        except Exception as e:
            print(f"Error during disconnection: {e}")

    def show_databases(self):
        if self.cursor:
            try:
                self.cursor.execute("SHOW DATABASES")
                databases = self.cursor.fetchall()
                print("\nAvailable Databases:")
                for db in databases:
                    print(f" - {db[0]}")
            except Exception as e:
                print(f"Failed to fetch databases: {e}")
        else:
            print("You must connect to Hive first.")
    def update_grade(self, table_name, student_id, course_id, grade):
        self.cursor.execute("USE grade")
        overwrite_query = f""" 
        INSERT OVERWRITE TABLE {table_name}
        SELECT
            CASE WHEN student_id = '{student_id}' AND course_id = '{course_id}' THEN '{student_id}' ELSE student_id END AS student_id,
            CASE WHEN student_id = '{student_id}' AND course_id = '{course_id}' THEN '{course_id}' ELSE course_id END AS course_id,
            roll_no,
            email_id,
            CASE WHEN student_id = '{student_id}' AND course_id = '{course_id}' THEN '{grade}' ELSE grade END AS grade
        FROM {table_name}
        """
        self.cursor.execute(overwrite_query)


    def SET(self, table_name, student_id, course_id, grade):
        self.update_grade(table_name, student_id, course_id, grade)
        self.conn.commit()
        return True
    def GET(self, table_name, student_id, course_id):
        # Retrieve the grade for a specific student and course
        query = f"SELECT grade FROM {table_name} WHERE student_id = '{student_id}' AND course_id = '{course_id}'"
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            return None


