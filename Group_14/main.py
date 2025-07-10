import os
import ast

from mongo import MongoTripleStore
from hive import HiveTripleStore
from postgres import PostgresTripleStore

import re

# Constants
DB_NAME = "grade"
TABLE_NAME = "student_grade"
OPLOG_DIR = "oplogs"

hive_host = "localhost"
hive_port = 10000


# # Initialize DB handlers


hive_server = HiveTripleStore(host=hive_host, port=hive_port)
hive_server.connect()
hive_server.connect_to_db(DB_NAME)

pg = PostgresTripleStore(host="localhost", port=5432,  user="postgres", password="password")
pg.connect()
pg.connect_to_db(DB_NAME)

mongo = MongoTripleStore(host="localhost", port=27017)
mongo.connect()
mongo.connect_to_db(DB_NAME)

# DB mapping
db_map = {
    "PSQL": pg,
    "MONGO": mongo,
    "HIVE": hive_server
}
import os
import ast

def get_oplog_path(db_name):
    return os.path.join("oplogs", f"{db_name.lower()}.oplog")

def get_next_timestamp(oplog_path):

    if not os.path.exists(oplog_path):
        return 1
    with open(oplog_path, "r") as f:
        lines = f.readlines()
        if not lines:
            return 1
        last_ts = max(int(line.strip().split(",")[0]) for line in lines if line.strip())
        return last_ts + 1

def append_to_oplog(db_name, op_string, ts=None):

    path = get_oplog_path(db_name)
    if ts is None:
        ts = get_next_timestamp(path)
    with open(path, "a") as f:
        f.write(f"{ts}, {op_string}\n")


def parse_set_op(op_string):
    """ "SET(('SID103', 'CSE016'), 'A')"
    Returns: ('SID103', 'CSE016', 'A')
    """
    set_pattern = r"SET\(\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*,\s*'([^']+)'\s*\)"
    match = re.match(set_pattern, op_string.strip())
    if match:
        student_id = match.group(1)
        course_id = match.group(2)
        grade = match.group(3)
        return (student_id, course_id, grade)
    else:
        raise ValueError(f"Invalid SET format: {op_string}")
def load_ops(path):

    ops = []
    with open(path, "r") as f:
        for line in f:
            if "SET" in line:
                ts_str, op_string = line.strip().split(",", 1)
                ts = int(ts_str)
                parsed_op = parse_set_op(op_string)
                ops.append((ts, parsed_op))
    return ops
def build_latest_set_dict(ops):
    latest = {}
    for ts, (student_id, course_id, grade) in reversed(ops):
        key = (student_id, course_id)
        if key not in latest:
            latest[key] = (ts, grade)

    return latest

def perform_merge(merge_into, merge_from):
    to_path = get_oplog_path(merge_into)
    from_path = get_oplog_path(merge_from)
    if not os.path.exists(to_path) or not os.path.exists(from_path):
        print(f"[ERROR] Missing oplog file(s) for {merge_into} or {merge_from}")
        return
    into_ops = load_ops(to_path)
    from_ops = load_ops(from_path)
    #latest SET dictionary 
    latest_to = build_latest_set_dict(into_ops)
    #latest SET dictionary 
    latest_from = build_latest_set_dict(from_ops)
    dest_handler = db_map[merge_into]
    for key, (from_ts, from_grade) in latest_from.items():
        student_id, course_id = key
        if key not in latest_to:
            dest_handler.SET(TABLE_NAME, student_id, course_id, from_grade)
            append_to_oplog(merge_into, f'SET(({student_id!r}, {course_id!r}), {from_grade!r})', from_ts)
            print(f"[MERGE] SET {key} → {merge_into} (new entry, ts={from_ts})")
        else:
            to_ts, _ = latest_to[key]
            if from_ts > to_ts:
                dest_handler.update_grade(TABLE_NAME, student_id, course_id, from_grade)
                append_to_oplog(merge_into, f'SET(({student_id!r}, {course_id!r}), {from_grade!r})',from_ts)
                print(f"[MERGE] SET {key} → {merge_into} (updated, ts={from_ts} > {to_ts})")
            else:
                # Destination is newer → skip
                print(f"[MERGE] Skipped {key})")



import re
def main():
    testcases_file = "test_cases.txt"
    
    with open(testcases_file, "r") as f:
        test_cases = f.readlines()
    test_cases = [line.strip() for line in test_cases if line.strip()]

    # Step 1: First pattern - extract till group3
    basic_pattern = r'^\s*(\d+)?\s*,?\s*([A-Z]+)\s*\.\s*([A-Z]+)\s*\((.*)\)\s*$'
    i=1
    for case in test_cases:
        print(f"Processing line {i}: {case}")
        i+=1
        match = re.match(basic_pattern, case)
        if not match:
            print(f"No match for line: {case}")
            continue
        timestamp = match.group(1)
        db = match.group(2)
        operation = match.group(3)
        group3 = match.group(4)
        if operation == "GET":
            # ( SID403 , CSE013 )
            inner_match = re.match(r'\s*([^\s,]+)\s*,\s*([^\s,]+)\s*', group3)
            if inner_match:
                student_id = inner_match.group(1)
                course_id = inner_match.group(2)
                result = db_map[db].GET(TABLE_NAME, student_id, course_id)
                append_to_oplog(db, f'GET(({student_id!r}, {course_id!r}))',int(timestamp))
                print(f"[GET] timestamp={timestamp}, db={db}, operation=GET, student_id={student_id}, course_id={course_id} ,result={result}")
            else:
                print(f"Invalid GET format: {group3}")

        elif operation == "SET":
            # (( SID101 , CSE026 ) , A )
            inner_match = re.match(r'\(\s*([^\s,]+)\s*,\s*([^\s,]+)\s*\)\s*,\s*([^\s,]+)', group3)
            if inner_match:
                student_id = inner_match.group(1)
                course_id = inner_match.group(2)
                grade = inner_match.group(3)
                db_map[db].update_grade(TABLE_NAME, student_id, course_id, grade)
                append_to_oplog(db, f'SET(({student_id!r}, {course_id!r}), {grade!r})',int(timestamp))
                print(f"[SET] timestamp={timestamp}, db={db}, operation=SET, student_id={student_id}, course_id={course_id}, grade={grade}")
            else:
                print(f"Invalid SET format: {group3}")

        elif operation == "MERGE":
            # ( SQL )
            inner_match = re.match(r'\s*([^\s,]+)\s*', group3)
            if inner_match:
                source_db = inner_match.group(1)
                #append_to_oplog(db, f'MERGE({source_db})')
                perform_merge(db, source_db)
                print(f"[MERGE] timestamp={timestamp}, db={db}, operation=MERGE, source_db={source_db}")
            else:
                print(f"Invalid MERGE format: {group3}")

        else:
            print(f"Unknown operation: {operation} in line: {case}")

if __name__ == "__main__":
    main()






