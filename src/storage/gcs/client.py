'''
GCS Client Module

This module provides a reusable interface to interact with Google Cloud Storage (GCS)

It handles:
- authentication (via Application Default Credentials)
- bucket connection
- file upload operations
'''
from google.cloud import storage
import logging

class GCSClient:
    def __init__(self, bucket_name: str):
        '''
        Initialize GCS client

        :param bucket_name: Name of the target GCS bucket
        '''
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

        logging.info(f"Client GCS initialisé pour le bucket: {bucket_name}")

    def upload_file(self, file, destination_blob_path: str):
        '''
        Upload a file directly to GCS

        :param file: File
        :param destination_blob_path: Path inside bucket
        '''
        blob = self.bucket.blob(destination_blob_path)
        blob.upload_from_file(file)

        logging.info(
            f"Fichier déposé sur gs://{self.bucket_name}/{destination_blob_path}"
        )