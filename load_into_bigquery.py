# Nhập các thư viện cần thiết cho việc xử lý dữ liệu, giao tiếp với Google Cloud Storage và BigQuery
import re
import json
from datetime import datetime
from google.cloud import bigquery

# Hàm preprocess_file dùng để định dạng lại các cột ngày tháng trong file json trước khi tải lên BigQuery
def preprocess_file(CS, bucket_name, file_name):
    bucket = CS.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data_string = blob.download_as_text()
    lines = data_string.splitlines()

    modified_lines = []
    for line in lines:
        try:
            record = json.loads(line)
            if 'Date' in record and record['Date']:
                try:
                    # First, try parsing with 'YYYY-MM-DD'
                    datetime.strptime(record['Date'], '%Y-%m-%d')
                    # If successful, no need to reformat
                except ValueError:
                    # If the first attempt fails, try 'DD-MM-YYYY' and reformat
                    date_obj = datetime.strptime(record['Date'], '%d-%m-%Y')
                    record['Date'] = date_obj.strftime('%Y-%m-%d')
            modified_line = json.dumps(record)
            modified_lines.append(modified_line)
        except ValueError as e:
            print(f"Error parsing line. Error: {e}")

    modified_data_string = "\n".join(modified_lines)
    blob.upload_from_string(modified_data_string)
    print("Pre-processed the file to format the 'Date' column.")
    
# Hàm _check_if_table_exists kiểm tra sự tồn tại của bảng trên BigQuery, nếu không tồn tại sẽ tạo mới
def _check_if_table_exists(tableName, tableSchema, BQ, BQ_DATASET):
    table_id = BQ.dataset(BQ_DATASET).table(tableName)
    try:
        BQ.get_table(table_id)
    except Exception as e:
        print(f'Creating table: {tableName}. Exception: {e}')
        schema = create_schema_from_yaml(tableSchema)
        table = bigquery.Table(table_id, schema=schema)
        BQ.create_table(table)
        print(f"Created table {tableName}.")

# Hàm _load_table_from_uri tải dữ liệu từ uri (đường dẫn trong Storage) lên bảng BigQuery
def _load_table_from_uri(bucket_name, file_name, tableSchema, tableName, BQ, BQ_DATASET, job_config):
    uri = f'gs://{bucket_name}/{file_name}'
    table_id = BQ.dataset(BQ_DATASET).table(tableName)
    schema = create_schema_from_yaml(tableSchema)
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.max_bad_records = 10  # Allow for maximum 10 errors
    load_job = BQ.load_table_from_uri(uri, table_id, job_config=job_config) 
    load_job.result()

# Hàm create_schema_from_yaml tạo schema cho bảng dựa trên cấu hình từ file YAML
def create_schema_from_yaml(table_schema):
    schema = []
    for column in table_schema:
        schemaField = bigquery.SchemaField(column['name'], column['type'], column['mode'])
        schema.append(schemaField)
        if column['type'] == 'RECORD':
            schemaField._fields = create_schema_from_yaml(column['fields'])
    return schema

# Hàm streaming dùng để xử lý luồng dữ liệu từ Storage lên BigQuery, bao gồm các bước tiền xử lý và tải lên
def streaming(filename, bucket_name, BQ, BQ_DATASET, job_config, CS, config):
    try:
        preprocess_file(CS, bucket_name, filename)  # Pre-process the file to format the date column
        for table in config:
            tableName = table.get('name')
            if re.search(tableName, filename):
                tableSchema = table.get('schema')
                _check_if_table_exists(tableName, tableSchema, BQ, BQ_DATASET)
                tableFormat = table.get('format')
                if tableFormat == 'NEWLINE_DELIMITED_JSON':
                    _load_table_from_uri(bucket_name, filename, tableSchema, tableName, BQ, BQ_DATASET, job_config)
                break
    except Exception as e:
        print(f'Error streaming file. Cause: {e}')  