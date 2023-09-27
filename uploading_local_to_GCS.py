# Later we will use Terraform to create this bucket
from google.cloud import storage
project="dtc-de-course-388001"
bucket="onepiece-"+project
source_file_name = "./onepieceanime/one_piece.csv"
destination_input_file = 'input/one_piece.csv'


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

# Uploading data to bucket
upload_blob(bucket, source_file_name, destination_input_file)


# Uploading script to bucket
script_source = './analysis_file.py'
destination_script_file =  'script/analysis_file.py'
upload_blob(bucket, script_source, destination_script_file)

