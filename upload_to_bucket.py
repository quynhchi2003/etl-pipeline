# Import packages
from google.cloud import storage

# define function that uploads a file from the bucket
def upload_cs_file(bucket_name, source_file_name, destination_file_name, CS):
    bucket = CS.bucket(bucket_name)

    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)
    

