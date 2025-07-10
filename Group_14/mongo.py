from pymongo import MongoClient
import csv
import os

from pymongo.errors import ConnectionFailure

class MongoTripleStore:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.database = None
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(f'mongodb://{self.host}:{self.port}/', serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            print(f"Connected to MongoDB at {self.host}:{self.port}.")
        except ConnectionFailure as e:
            print(f"Failed to connect to MongoDB: {e}")
            self.client = None
    def connect_to_db(self, database):
        if not self.client:
            print("You must connect to MongoDB first.")
            return
        try:
            self.database = database
            self.db = self.client[self.database]
            print(f"Connected to database '{self.database}' at {self.host}:{self.port}.")
        except Exception as e:
            print(f"Failed to connect to database {database}: {e}")
    def disconnect(self):
        if self.client:
            try:
                self.client.close()
                print(f"Disconnected from MongoDB at {self.host}:{self.port}.")
                self.client = None
                self.db = None
                self.database = None
            except Exception as e:
                print(f"Error while disconnecting from MongoDB: {e}")

    def show_databases(self):
        if not self.client:
            print("No active MongoDB connection.")
            return
        
        try:
            databases = self.client.list_database_names()
            print("\nAvailable Databases:")
            for db in databases:
                print(f" - {db}")
        except Exception as e:
            print(f"Failed to list databases: {e}")
    def update_grade(self, collection_name, student_id, course_id, grade):
        collection = self.db[collection_name]
        result = collection.update_one(
            {'student_id': student_id, 'course_id': course_id},
            {'$set': {'grade': grade}},
            upsert=True
        )
        if result.matched_count > 0:
            print("Grade updated.")
        else:
            print("New document inserted (record not found).")

    def SET(self, collection_name, student_id, course_id, grade):
        self.update_grade(collection_name, student_id, course_id, grade)
        return True

    def GET(self, collection_name, student_id, course_id):
        collection = self.db[collection_name]
        result = collection.find_one({'student_id': student_id, 'course_id': course_id})
        return result['grade'] if result else None
    