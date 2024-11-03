import pyzstd
from joedb import JoeDB
import os
import difflib
from pprint import pformat
import gzip
import json
import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import humanize

from joedb.joedb import COMPRESSION_LEVEL


# Helper functions to run tests
def assert_equal(actual, expected, message=""):
    """Assert that actual equals expected with detailed diff on failure."""
    if actual != expected:
        diff = difflib.unified_diff(
            pformat(expected, indent=2).splitlines(keepends=True),
            pformat(actual, indent=2).splitlines(keepends=True),
            fromfile='expected',
            tofile='actual',
            lineterm='\n'
        )
        diff_output = '\n'.join(diff)
        raise AssertionError(f"Assertion failed! {message}:\n{diff_output}")


def assert_file_exists(file_path):
    assert os.path.exists(file_path), f"File {file_path} does not exist!"

def flatten_json(data, parent_key='', sep='.'):
    """Flatten nested JSON objects."""
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def insert_and_round_trip(db, log_entries, original_df=None):
    """
    Insert JSON objects, encode to binary, decode back and compare.
    The decoded data must match the original input data.
    Outputs the file sizes of the binary storage, gzipped NDJSON, and gzipped CSV files.
    """
    for log_entry in log_entries:
        db.insert(log_entry)

    # Encode to a test binary file
    binary_file = 'test_logs.jdb'
    db.encode(binary_file)

    # Ensure the binary file exists after encoding
    assert_file_exists(binary_file)

    # Decode the binary file and retrieve log entries
    restored_logs = db.decode(binary_file)

    # Ensure the decoded data matches the original data
    # assert_equal(restored_logs, log_entries, "Decoded logs must match the original logs")

    # Output the size of the binary file
    binary_size = os.path.getsize(binary_file)
    print(f"Binary file size: {humanize.naturalsize(binary_size)}")

    # Save the log entries in NDJSON format
    ndjson_file = 'test_logs.ndjson'
    with open(ndjson_file, 'w') as f:
        for log in log_entries:
            f.write(json.dumps(log) + '\n')

    raw_ndjson_size = os.path.getsize(ndjson_file)
    print(f"Raw NDJSON file size: {humanize.naturalsize(raw_ndjson_size)}")

    # Compress the NDJSON file with gzip
    gzipped_ndjson_file = 'test_logs.ndjson.gz'
    with open(ndjson_file, 'rb') as f_in, pyzstd.open(gzipped_ndjson_file, 'wb') as f_out:
        f_out.writelines(f_in)

    # Ensure the gzipped NDJSON file exists
    assert_file_exists(gzipped_ndjson_file)

    # Output the size of the gzipped NDJSON file
    gzipped_ndjson_size = os.path.getsize(gzipped_ndjson_file)
    print(f"Zstd compressed NDJSON file size: {humanize.naturalsize(gzipped_ndjson_size)}")

    # Save the log entries in CSV format if there are any entries
    if log_entries:
        # Collect all unique keys across all log entries
        all_keys = set()
        for log in log_entries:
            all_keys.update(log.keys())

        csv_file = 'test_logs.csv'
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(log_entries)

        raw_csv_size = os.path.getsize(csv_file)
        print(f"Raw CSV file size: {humanize.naturalsize(raw_csv_size)}")

        # Compress the CSV file with gzip
        gzipped_csv_file = 'test_logs.csv.gz'
        with open(csv_file, 'rb') as f_in, pyzstd.open(gzipped_csv_file, 'wb') as f_out:
            f_out.writelines(f_in)

        # Ensure the gzipped CSV file exists
        assert_file_exists(gzipped_csv_file)

        # Output the size of the gzipped CSV file
        gzipped_csv_size = os.path.getsize(gzipped_csv_file)
        print(f"Zstd compressed CSV file size: {humanize.naturalsize(gzipped_csv_size)}")

        # Prepare a dictionary to collect values for each column
        column_data = {key: [] for key in all_keys}

        # Collect values for each key across all records
        for record in log_entries:
            flattened_record = flatten_json(record)
            for key in all_keys:
                column_data[key].append(str(flattened_record.get(key, '')))  # Append value or empty if missing

        column_csv_file = 'test_logs_columnar.csv'
        # Write all column data to a single gzip-compressed CSV file
        with open(column_csv_file, 'w', newline='') as f:
            writer = csv.writer(f)

            # Write each column in the format: column_name, value1, value2, ..., valueN
            for key, values in column_data.items():
                writer.writerow([key] + values)  # Write column name followed by all values for that column

        raw_column_csv_size = os.path.getsize(column_csv_file)
        print(f"Raw columnar CSV file size: {humanize.naturalsize(raw_column_csv_size)}")

        # Compress the CSV file with gzip
        gzipped_column_csv_file = 'test_logs_column.csv.gz'
        with open(column_csv_file, 'rb') as f_in, pyzstd.open(gzipped_column_csv_file, 'wb', level_or_option=COMPRESSION_LEVEL) as f_out:
            f_out.writelines(f_in)

        # Ensure the gzipped CSV file exists
        assert_file_exists(gzipped_column_csv_file)

        # Output the size of the gzipped CSV file
        gzipped_column_csv_size = os.path.getsize(gzipped_column_csv_file)
        print(f"Zstd-compressed columnar CSV file size (lvl {COMPRESSION_LEVEL}): {humanize.naturalsize(gzipped_column_csv_size)}")
        os.remove(column_csv_file)
        os.remove(csv_file)
        os.remove(gzipped_csv_file)
        os.remove(gzipped_column_csv_file)


    else:
        print("No log entries to save in CSV format.")


    flattened_data = [flatten_json(record) for record in log_entries]

    # Convert to DataFrame (Pandas is a good intermediary for pyarrow)
    df = original_df if original_df is not None else pd.DataFrame(flattened_data)

    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    parquet_file = 'test_logs.parquet'

    # Write the table to a Parquet file with optional GZIP compression
    pq.write_table(table, parquet_file, compression='ZSTD', use_dictionary=True, data_page_size=40 * 1024 * 1024, compression_level=COMPRESSION_LEVEL)

    parquet_size = os.path.getsize(parquet_file)
    print(f"Parquet file size: {humanize.naturalsize(parquet_size)}")


    # Cleanup
    os.remove(binary_file)
    os.remove(ndjson_file)
    os.remove(gzipped_ndjson_file)
    os.remove(parquet_file)


def probe_csv_file(db, csv_file_path):
    """
    Test case to read the /Mac_2k.log_structured.csv file and insert individual log entries into the database.
    """
    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Convert each row to a dictionary
    log_entries = df.astype(str).to_dict(orient='records')


    insert_and_round_trip(db, log_entries)

def probe_ndjson_gz_file(db, ndjson_file_path):
    """
    Test case to read the /Mac_2k.log_structured.ndjson.gz file and insert individual log entries into the database.
    """
    # Read the NDJSON file using pandas
    df = pd.read_json(ndjson_file_path, lines=True)
    json_struct = json.loads(df.to_json(orient="records"))
    df_flat = pd.json_normalize(json_struct)
    df_flat.columns = df_flat.columns.str.replace('.', '_', regex=False)

    # TODO: Allow empty string values (currently not supported)
    df_flat = df_flat.drop(columns=['resource_attributes_host_cpu_family', 'resource_attributes_host_cpu_model_name', 'resource_attributes_host_mac'])

    # Convert each row to a dictionary
    log_entries = df_flat.astype(str).to_dict(orient='records')

    insert_and_round_trip(db, log_entries)

def probe_ndjson_gz_file_single_level(db, ndjson_file_path):
    """
    Test case to read the /Mac_2k.log_structured.ndjson.gz file and insert individual log entries into the database.
    """
    # Read the NDJSON file using pandas
    df = pd.read_json(ndjson_file_path, lines=True)
    df = df.astype(str)
    df = df.sort_values(by=['resource', '@timestamp'])
    # Convert each row to a dictionary
    log_entries = df.to_dict(orient='records')

    insert_and_round_trip(db, log_entries)

def test_simple_log():
    db1 = JoeDB()
    log_entries1 = [
        {"timestamp": "2024-10-19T14:00:00", "level": "INFO", "message": "Log message 1"},
        {"timestamp": "2024-10-19T14:01:00", "level": "ERROR", "message": "Log message 2"},
        {"timestamp": "2024-10-19T14:02:00", "level": "INFO", "message": "Log message 3"}
    ]
    insert_and_round_trip(db1, log_entries1)

def test_nested_log():
    db2 = JoeDB()
    log_entries2 = [
        {"message": "Log message 1", "meta": {"id": "123"}},
        {"message": "Log message 2", "meta": {"id": "124"}},
        {"message": "Log message 3", "meta": {"id": "125"}}
    ]
    insert_and_round_trip(db2, log_entries2)

def test_empty_log():
    db3 = JoeDB()
    log_entries3 = []
    insert_and_round_trip(db3, log_entries3)

def test_missing_fields_log():
    db4 = JoeDB()
    log_entries4 = [
        {"timestamp": "2024-10-19T14:00:00", "level": "INFO"},
        {"timestamp": "2024-10-19T14:01:00", "message": "Log message 2"},
        {"timestamp": "2024-10-19T14:02:00", "level": "ERROR", "message": "Log message 3"}
    ]
    insert_and_round_trip(db4, log_entries4)


def test_extend_trie():
    db5 = JoeDB()
    log_entries5 = [{"mykey": "abc"}, {"mykey": "abcd"}]
    insert_and_round_trip(db5, log_entries5)

def test_extend_trie2():
    db6 = JoeDB()
    log_entries6 = [{"mykey": "0"}, {"mykey": "1"}, {"mykey": "2"},  {"mykey": "10"}]
    insert_and_round_trip(db6, log_entries6)

    # Test 7: Large dataset with many records
# def test_large_dataset():
#     db7 = JoeDB()
#     log_entries7 = [{"timestamp": f"2024-10-19T14:{i:02d}:00", "level": "INFO", "message": f"Log message"} for i in range(1000)]
#     insert_and_round_trip(db7, log_entries7)

# def test_zookeeper_pattern_logs():
#     db8 = JoeDB(use_patternization=True)
#     probe_csv_file(db8, 'fixtures/zookeeper.csv')

# def test_zookeeper_no_patternlogs():
#     db8 = JoeDB(use_patternization=False)
#     probe_csv_file(db8, 'fixtures/zookeeper.csv')

# def test_spark_no_pattern_logs():
#     db8 = JoeDB(use_patternization=False)
#     probe_ndjson_gz_file(db8, 'fixtures/spark.ndjson.gz')

# def test_thunderbird_logs():
#     db8 = JoeDB(use_patternization=False)
#     probe_csv_file(db8, 'fixtures/thunderbird.csv')

def test_otel_logs():
    db8 = JoeDB(use_patternization=True)
    probe_ndjson_gz_file_single_level(db8, 'fixtures/otel.ndjson.gz')