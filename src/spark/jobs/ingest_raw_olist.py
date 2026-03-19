import os
import subprocess
import zipfile
import logging
import time
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

DATASET = "olistbr/brazilian-ecommerce"
DOWNLOAD_PATH = "/tmp/olist"

def get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not all([endpoint, access_key, secret_key]):
        raise Exception("Missing MinIO environment variables")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

def download_dataset():
    start = time.time()

    logger.info("Starting dataset download from Kaggle...")

    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    try:
        subprocess.run(
            [
                "kaggle",
                "datasets",
                "download",
                "-d",
                DATASET,
                "-p",
                DOWNLOAD_PATH
            ],
            check=True
        )

        elapsed = time.time() - start
        logger.info(f"Download completed in {elapsed:.2f}s")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error during dataset download: {e}")
        raise

def unzip_dataset():
    start = time.time()

    logger.info("Starting dataset extraction...")

    extracted_files = 0

    try:
        for file in os.listdir(DOWNLOAD_PATH):
            if file.endswith(".zip"):
                path = f"{DOWNLOAD_PATH}/{file}"

                logger.info(f"Extracting: {file}")

                with zipfile.ZipFile(path, "r") as zip_ref:
                    zip_ref.extractall(DOWNLOAD_PATH)
                    extracted_files += len(zip_ref.namelist())

        elapsed = time.time() - start

        logger.info(f"Extraction completed in {elapsed:.2f}s")
        logger.info(f"Total files extracted: {extracted_files}")

    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        raise

def clean_bucket_prefix(client, bucket, prefix):
    logger.info(f"Cleaning bucket '{bucket}' prefix '{prefix}'...")

    objects = client.list_objects(bucket, prefix=prefix, recursive=True)

    deleted = 0

    for obj in objects:
        logger.info(f"🗑 Removing: {obj.object_name}")
        client.remove_object(bucket, obj.object_name)
        deleted += 1

    logger.info(f"Removed {deleted} old files")

    return deleted

def validate_upload(client, bucket, prefix, expected_files):
    logger.info("Validating files in MinIO...")

    objects = client.list_objects(bucket, prefix=prefix, recursive=True)

    uploaded_files = [obj.object_name.split("/")[-1] for obj in objects]

    logger.info(f"Files found in bucket: {len(uploaded_files)}")

    missing = set(expected_files) - set(uploaded_files)

    if missing:
        logger.error(f"Missing files in MinIO: {missing}")
        raise Exception("Upload validation failed")

    logger.info("All files successfully uploaded and validated!")

def upload_to_minio():
    start = time.time()

    logger.info("☁️ Starting upload to MinIO...")

    client = get_minio_client()

    bucket = "raw"
    prefix = "olist/"

    try:
        if not client.bucket_exists(bucket):
            logger.info(f"Bucket '{bucket}' not found. Creating...")
            client.make_bucket(bucket)

        # 🔥 CLEAN BEFORE UPLOAD (overwrite strategy)
        deleted = clean_bucket_prefix(client, bucket, prefix)
        logger.info(f"Total old files removed: {deleted}")

        # Get CSV files
        files = [
            f for f in os.listdir(DOWNLOAD_PATH)
            if f.endswith(".csv")
        ]

        logger.info(f"Files to upload: {len(files)}")

        uploaded_files = 0

        for file in files:
            local_path = f"{DOWNLOAD_PATH}/{file}"
            object_name = f"{prefix}{file}"

            logger.info(f"⬆️ Uploading: {file}")

            client.fput_object(
                bucket,
                object_name,
                local_path
            )

            uploaded_files += 1

    
        validate_upload(client, bucket, prefix, files)

        elapsed = time.time() - start

        logger.info(f"Upload completed in {elapsed:.2f}s")
        logger.info(f"Total files uploaded: {uploaded_files}")

    except Exception as e:
        logger.error(f"Error during upload: {e}")
        raise

def run():
    pipeline_start = time.time()

    logger.info("Starting ingestion pipeline...")

    download_dataset()
    unzip_dataset()
    upload_to_minio()

    total_time = time.time() - pipeline_start

    logger.info("Pipeline finished successfully!")
    logger.info(f"Total pipeline time: {total_time:.2f}s")