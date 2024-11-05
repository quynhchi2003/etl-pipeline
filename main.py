# Nhập các thư viện cần thiết cho việc xử lý dữ liệu, giao tiếp với Google Cloud Storage và BigQuery
import os
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
import yaml
import warnings
warnings.filterwarnings("ignore")

# Import data_transformation, upload_to_bucket và load_into_bigquery
import data_transformation
import upload_to_bucket
import load_into_bigquery

# I. Khai báo các biến cần thiết cho ETL Pipeline
# 1. Khai báo tên các bảng để transform
table_names = [
    # 'Combined_Laz_Chat',
    # 'Dim_Product',
    # 'Laz_Key_Metrics',
    # 'Laz_Mall_Key_Metrics',
    # 'Laz_Mall_Order',
    # 'Laz_Order',
    # 'Laz_Mall_Product',
    # 'Laz_Product',
    # 'Laz_Mall_Return_Refund',
    # 'Laz_Return_Refund',
    # 'Shopee_Chat',
    # 'Shopee_Key_Metrics',
    # 'Shopee_Order',
    # 'Shopee_Product',
    'Shopee_Return_Refund',
    # 'Shopee_Sales_Overview',
    # 'Shopee_Traffic',
    # 'TikTok_Key_Metrics',
    # 'TikTok_Order',
    # 'TikTok_Return_Refund',
    # 'Shopee_Details_Chat'
]

# 2. Khai báo các folders cần thiết và khoảng thời gian của data.
time_suffix = "Sep2024" # Biến này chỉ ra các data đang được xử lý thuộc khoảng thời gian nào
import_data_path =  r"C:\Users\Administrator\Desktop\Study\ULTIMATE AQUA\Input_data_Sep2024" # Đây là folder chứa data cần transform tại máy local
export_data_path = r"C:\Users\Administrator\Desktop\Study\ULTIMATE AQUA\Output_data_Sep2024" # Đấy là folder chứa những file .json sau khi đã transform

# 3. Khai báo các biến cấu hình cho dự án Google Cloud, dataset trong BigQuery và khởi tạo các client cho Storage và BigQuery 
# set key credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Administrator\Desktop\Study\ULTIMATE SUP\Big Query\Credentials_key\secret-cipher-426506-b1-e79780c16e29.json"
bucket_name = 'ultimate-aqua' # Đây là tên của GCS Bucket
PROJECT_ID = 'secret-cipher-426506-b1' # Đây là tên của GC Project
BQ_DATASET = 'ultimate_aqua_data' # Đây là tên của GC BiqQuery dataset
CS = storage.Client()
BQ = bigquery.Client()
job_config = bigquery.LoadJobConfig()

# 4. Khai báo dữ liệu của file schemas.yaml
# Đọc cấu hình schema từ file YAML để ánh xạ dữ liệu vào các bảng BigQuery
with open(r"C:\Users\Administrator\Desktop\Study\Git\ultimatesupdata\ETL Pipeline\schemas.yaml") as schema_file:
    config = yaml.load(schema_file, Loader=yaml.Loader)

# II. Thực hiện toàn bộ công đoạn của ETL Pipeline
for table_name in table_names:
    print(f"Processing file {table_name}_{time_suffix}")
    print("Current run time: ", datetime.now())
    
    # 1. Transform data
    try:
        func = getattr(data_transformation, table_name)
        func(import_data_path, export_data_path, time_suffix)
        print(f"Successfully transform {table_name}_{time_suffix}")
    except Exception as e:
        print(f'Error transforming file. Cause: {e}')

    # 2. Upload file to google storage bucket
    try:
        upload_to_bucket.upload_cs_file(bucket_name, f"{export_data_path}\{table_name}_{time_suffix}.json", f"{table_name}_{time_suffix}.json", CS)
        print(f"Successfully upload {table_name}_{time_suffix} to Bucket {bucket_name}")
    except Exception as e:
        print(f'Error uploading file. Cause: {e}')

    # 3. Load into Google BiqQuery
    try:
        load_into_bigquery.streaming(f"{table_name}_{time_suffix}.json", bucket_name, BQ, BQ_DATASET, job_config, CS, config)
        print(f"Finish loading {table_name}_{time_suffix} into Google BiqQuery {BQ_DATASET}")
        print("----------------------------------------------------------------\n")
    except Exception as e:
        print(f'Error loading file. Cause: {e}')
