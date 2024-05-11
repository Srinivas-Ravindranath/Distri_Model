from pymongo import MongoClient
import gridfs
from io import StringIO
import logging
import os

from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

class MongoDB:

    def __init__(self):
        mongo_host_url = os.getenv("MONGO_HOST")
        self.url = mongo_host_url
        self.client = MongoClient(self.url)
        self.db = self.client["file_store"]
        self.fs = gridfs.GridFS(self.db)

    def file_in_gridfs(self, file_name: str):
        if self.fs.exists(filename=file_name):
            return True
        else:
            return False

    def add_file_to_gridfs(self, file_path: str):
        if self.file_in_gridfs(file_path):
            logger.info(f"{file_path} already exists in the database.")
        else:
            with open(file_path, 'rb') as file_to_upload:
                file_id = self.fs.put(file_to_upload, filename=file_path)
                logger.info(f"Uploaded {file_path} to GridFS with file id: {file_id}")

    def read_file_from_gridfs(self, file_name: str):
        if self.file_in_gridfs(file_name):
            grid_out = self.fs.find_one({"filename": file_name})
            data = grid_out.read().decode('utf-8')
            data_buffer = StringIO(data)
            logger.info(f'{file_name} has been read into memory.')
            return data_buffer
        else:
            logger.info(f'File {file_name} does not exist in the database.')
            return None
