import csv
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['grade']
collection = db['student_grade']

# List to hold documents
documents = []

# Open the CSV file
with open('/home/mohan/Downloads/student_course_grades.csv', 'r') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        document = {
            'student_id': row.get('student-ID', '').strip(),
            'course_id': row.get('course-id', '').strip(),
            'roll_no': row.get('roll no', '').strip(),
            'email_id': row.get('email ID', '').strip(),
            'grade': row.get('grade', '').strip()
        }
        documents.append(document)

# Insert all documents
collection.insert_many(documents)

print(f"Inserted {len(documents)} documents successfully!")
