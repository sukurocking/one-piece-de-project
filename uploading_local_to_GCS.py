# Later we will use Terraform to create this bucket
from google.cloud import storage
project="dtc-de-course-388001"
bucket="onepiece-"+project
source_file_name = "./onepieceanime/one_piece.csv"
destination_file_name = "onepieceanime/input/one_piece.csv"
from google.cloud import storage

# Function to create bucket if not exists
def create_bucket_if_not_exists(bucket_name):
    storage_client = storage.Client()
    if not storage_client.lookup_bucket(bucket_name):
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")

create_bucket_if_not_exists(bucket)

# Function to transfer data to bucket
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

upload_blob(bucket, source_file_name, destination_file_name)
