from pymongo import MongoClient
import gridfs
from io import StringIO


class MongoDB:

    def __init__(self):
        self.url = 'mongodb://localhost:27017/'
        self.client = client = MongoClient(self.url)
        self.db = self.client["file_store"]
        self.fs = gridfs.GridFS(self.db)

    def file_in_gridfs(self, file_name: str):
        if self.fs.exists(filename=file_name):
            return True
        else:
            return False

    def add_file_to_gridfs(self, file_path: str):
        if self.file_in_gridfs(file_path):
            print(f"{file_path} already exists in the database.")
        else:
            with open(file_path, 'rb') as file_to_upload:
                file_id = self.fs.put(file_to_upload, filename=file_path)
                print(f"Uploaded {file_path} to GridFS with file id: {file_id}")

    # def read_file_from_gridfs(self, file_name: str):
    #     if self.file_in_gridfs(file_name):
    #         grid_out = self.fs.find_one({"filename": file_name})
    #         data = grid_out.read()
    #         with open(f'local_{file_name}', 'wb') as output_file:
    #             output_file.write(data)
    #         print(f'{file_name} has been downloaded and saved as local_{file_name}.')
    #     else:
    #         print(f'File {file_name} does not exist in the database.')

    def read_file_from_gridfs(self, file_name: str):
        if self.file_in_gridfs(file_name):
            grid_out = self.fs.find_one({"filename": file_name})
            data = grid_out.read().decode('utf-8')
            data_buffer = StringIO(data)
            print(f'{file_name} has been read into memory.')
            return data_buffer
        else:
            print(f'File {file_name} does not exist in the database.')
            return None
