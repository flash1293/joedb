import pyzstd
from joedb import JoeDB


def test_smoke():
    # Initialize the database
    db = JoeDB()

    # Insert log entries (as JSON objects)
    # db.insert({"timestamp": "2024-10-19T14:00:00", "level": "INFO", "message": "Log message 1"})
    # db.insert({"timestamp": "2024-10-19T14:01:00", "level": "ERROR", "message": "Log message 2"})
    # db.insert({"timestamp": "2024-10-19T14:02:00", "level": "INFO", "message": "Log message 3"})
    db.insert({"message": "0123 hi XX", "timestamp": "2024-10-14T13:07:37.906Z"})
    db.insert({"message": "00456 hiXXX", "timestamp": "2024-10-14T15:07:37.906Z"})

    # Encode and store in binary format
    db.encode('logs.jdb')

    # Decode from binary format and reconstruct the log entries
    restored_logs = db.decode('logs.jdb')

    # Output the restored logs
    print(restored_logs)

    # remove the file
    # import os
    # os.remove('logs.jdb')
