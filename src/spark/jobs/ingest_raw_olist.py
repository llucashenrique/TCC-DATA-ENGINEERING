import os
import subprocess
import zipfile
from minio import Minio


DATASET = "olistbr/brazilian-ecommerce"
DOWNLOAD_PATH = "/tmp/olist"


def download_dataset():

    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

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


def unzip_dataset():

    for file in os.listdir(DOWNLOAD_PATH):

        if file.endswith(".zip"):

            path = f"{DOWNLOAD_PATH}/{file}"

            with zipfile.ZipFile(path, "r") as zip_ref:
                zip_ref.extractall(DOWNLOAD_PATH)


def upload_to_minio():

    client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="admin123",
        secure=False
    )

    bucket = "raw"

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    for file in os.listdir(DOWNLOAD_PATH):

        if file.endswith(".csv"):

            local_path = f"{DOWNLOAD_PATH}/{file}"

            object_name = f"olist/{file}"

            client.fput_object(
                bucket,
                object_name,
                local_path
            )


def run():

    download_dataset()

    unzip_dataset()

    upload_to_minio()