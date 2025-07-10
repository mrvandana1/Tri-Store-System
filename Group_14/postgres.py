import psycopg2
import csv
import os
from psycopg2 import OperationalError, Error

class PostgresTripleStore:
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = None
        self.conn = None
        self.cursor = None
    def disconnect(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None
        print("Disconnected from PostgreSQL.")
    def connect(self, database='postgres'):
        if self.conn:
            print("Already connected.")
            return
        
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database
            )
            self.cursor = self.conn.cursor()
            self.database = database
            print(f"Connected to PostgreSQL at {self.host}:{self.port}, using database '{self.database}'.")
        except OperationalError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            self.conn = None
            self.cursor = None

    def connect_to_db(self, database):
        """Reconnect to a specific database"""
        if self.conn:
            self.disconnect()
        self.connect(database)

    def show_databases(self):
        if not self.cursor:
            print("Not connected to any PostgreSQL server.")
            return
        
        try:
            self.cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            databases = self.cursor.fetchall()
            print("\nAvailable Databases:")
            for db in databases:
                print(f" - {db[0]}")
        except Error as e:
            print(f"Error listing databases: {e}")
    def update_grade(self, table_name, student_id, course_id, grade):
        try:
            query = f"UPDATE {table_name} SET grade = %s WHERE student_id = %s AND course_id = %s"
            self.cursor.execute(query, (grade, student_id, course_id))
            self.conn.commit()
            print(f"Updated grade for student_id {student_id} and course_id {course_id} in '{table_name}'.")
        except psycopg2.Error as e:
            self.conn.rollback()
            print(f"Error updating grade: {e}")

        self.conn.commit()
    def SET(self, table_name, student_id, course_id, grade):
        self.update_grade(table_name, student_id, course_id, grade)
        return True

    def GET(self, table_name, student_id, course_id):
        try:
            self.cursor.execute(f"""
                SELECT grade FROM {table_name}
                WHERE student_id = %s AND course_id = %s
            """, (student_id, course_id))
            
            result = self.cursor.fetchone()
            return result[0] if result else None
        except psycopg2.Error as e:
            print(f"Error retrieving grade: {e}")
            return None
    