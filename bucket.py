from dotenv import load_dotenv
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import os
import boto3


load_dotenv()


class Bucket:
    """ Managing to S3 bucket """

    def __init__(self, name: str):
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_aws_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = os.getenv("AWS_REGION_NAME")
        self.bucket_name = name

    def _connect_to_client(self):
        """ Connect to s3 bucket client """

        return boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.secret_aws_access_key,
            region_name='us-west-2'
        )

    def _connect_to_resource(self):
        """ Connect to s3 bucket resource """

        return boto3.resource(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.secret_aws_access_key,
            region_name='us-west-2'
        )

    def delete_old_markups(self, path_to_folder: str, till_date: int):
        folders = self._connect_to_client().list_objects(Bucket=self.bucket_name, Prefix=path_to_folder)
        folder_level = len(path_to_folder.split('/'))
        expire_date = datetime.now() - timedelta(days=till_date)
        timestamp_expire_date = round(datetime.timestamp(expire_date))
        temp = dict()
        if folders.get('Contents'):
            for item in folders.get('Contents'):
                timestamp_folder = item.get('Key').split('/')[folder_level:]
                if timestamp_folder[0] not in temp:
                    temp[timestamp_folder[0]] = []
                temp[timestamp_folder[0]].append(timestamp_folder[1])
            for temp_item_key, temp_item_value in temp.items():
                if temp_item_key <= str(timestamp_expire_date):
                    for file in temp_item_value:
                        self.delete_file(f"{path_to_folder}/{temp_item_key}/{file}")
                    print(f"{path_to_folder}/{temp_item_key}")
                    self.delete_file(f"{path_to_folder}/{temp_item_key}")

    def load_directories(self, path_to_folder: str):
        folders = self._connect_to_client().list_objects(Bucket=self.bucket_name, Prefix=path_to_folder)
        folder_level = len(path_to_folder.split('/'))
        folders_list = dict()
        latest_timestamp_folder = None
        if folders.get('Contents'):
            for item in folders.get('Contents'):
                timestamp_folder = item.get('Key').split('/')[folder_level:]
                if not latest_timestamp_folder or latest_timestamp_folder < timestamp_folder[0]:
                    latest_timestamp_folder = timestamp_folder[0]
                if timestamp_folder[0] not in folders_list:
                    folders_list[timestamp_folder[0]] = list()
                if len(timestamp_folder) > 1:
                    folders_list[timestamp_folder[0]].append(timestamp_folder[1])
        print(f"Latest timestamp folder: {latest_timestamp_folder}")
        return folders_list, latest_timestamp_folder

    def get_list_objects_folder(self, path: str):
        prefix = f"{path}"
        folder_list_object = list()
        folder = self._connect_to_client().list_objects(Bucket=self.bucket_name, Prefix=prefix)
        if folder.get('Contents'):
            for item in folder.get('Contents'):
                if item.get('Key'):
                    if item.get('Key') != prefix:
                        folder_list_object.append(item.get('Key'))
        return folder_list_object

    def get_file(self, file_path):
        try:
            return self._connect_to_resource().Object(self.bucket_name, file_path).get()['Body']
        except ClientError as boto_error:
            print(f"{boto_error}: {file_path}")
            return None

    def add_file(self, file_path, path_in_bucket):
        with open(file_path, 'rb') as data:
            self._connect_to_client().upload_fileobj(data, self.bucket_name, path_in_bucket)

    def copy_file(self, old_path,  new_path):
        """ Copy file in bucket """
        try:
            self._connect_to_resource().meta.client.copy({'Bucket': self.bucket_name, 'Key': old_path}, self.bucket_name, new_path)
        except ClientError as boto_error:
            print(boto_error)

    def delete_file(self, path):
        """ Delete file from bucket """
        self._connect_to_resource().Object(self.bucket_name, path).delete()

    def moving_file(self, old_path,  new_path):
        """ Moving file in bucket """
        self.copy_file(old_path, new_path)
        self.delete_file(old_path)

    def get_files_from_dir(self, folder_path, destination_path):
        """ Download files from bucket to local folder"""
        folders = self._connect_to_client().list_objects(Bucket=self.bucket_name, Prefix=folder_path)
        if folders.get('Contents'):
            for item in folders.get('Contents'):
                if item.get('Key'):
                    file_name = item.get('Key').split('/')[-1]
                    self._connect_to_client().download_file(self.bucket_name, item.get('Key'),
                                                            f"{destination_path}/{file_name}")

    def load_files_to_bucket(self, local_dir_path, bucket_path):
        """ Load all files from local directory to bucket"""
        for item in os.listdir(local_dir_path):
            self.add_file(f"{local_dir_path}/{item}", f"{bucket_path}/{item}")
