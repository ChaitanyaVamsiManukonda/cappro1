"""
Upload Bronze Layer CSVs to AWS S3 Data Lake

Reads local CSV files from data/bronze/ and uploads them to the S3 bucket
configured in your .env file using boto3.
"""

import os
import boto3
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET_URI = os.environ.get("S3_BUCKET_URI", "s3://cv-minicapstone-blob-bucket")

# Extract bucket name from S3 URI (e.g. s3://my-bucket -> my-bucket)
bucket_name = S3_BUCKET_URI.replace("s3://", "").split("/")[0]

print("=" * 60)
print("  Uploading Local Bronze Data to AWS S3")
print(f"  Target Bucket: {bucket_name}")
print(f"  Region:        {AWS_REGION}")
print("=" * 60)

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    print("ERROR: AWS credentials not found in environment or .env file.")
    sys.exit(1)

# Initialize the S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# Identify directories and files
project_root = os.path.dirname(os.path.abspath(__file__))
local_bronze_dir = os.path.join(project_root, "data", "bronze")
files_to_upload = ["users.csv", "products.csv", "weblogs.csv"]

# Run the upload
for file_name in files_to_upload:
    local_path = os.path.join(local_bronze_dir, file_name)
    s3_key = f"bronze/{file_name}"
    
    if os.path.exists(local_path):
        file_size = os.path.getsize(local_path)
        print(f"Uploading: {file_name} ({file_size / 1024:.2f} KB)")
        print(f"  -> s3://{bucket_name}/{s3_key}")
        try:
            s3_client.upload_file(local_path, bucket_name, s3_key)
            print(f"SUCCESS:   {file_name} uploaded successfully.")
        except Exception as e:
            print(f"FAILED:    Failed to upload {file_name}. Error: {e}")
    else:
        print(f"WARNING:   Local file not found at {local_path}. Skip.")

print("\nUpload process finished.")
print("=" * 60)
