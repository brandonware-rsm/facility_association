from azure.storage.blob import BlobClient, BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import logging

class DataLakeContainer:

    def __init__(self, storage_account_name, connection_string, container_name):
        
        self.storage_account_name = storage_account_name
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)
        #self.logger = logging.getLogger('func_get_quickbooks_object.datalakecontainer')

    def upload_file_to_data_lake(self,blob_name,data):

        blob = BlobClient.from_connection_string(conn_str=self.connection_string, container_name=self.container_name,blob_name=blob_name)
        response = blob.upload_blob(data,overwrite=True)
        #self.logger.debug(f'{response}')
        
        return response

    def read_file_from_data_lake(self,blob_name):

        blob = BlobClient.from_connection_string(conn_str=self.connection_string, container_name=self.container_name,blob_name=blob_name)
        blob_download = blob.download_blob()
        data = blob_download.readall()
        #data = blob_download.readall().decode('utf-8')
        return data

    def list_blobs_in_directory(self,directory_path):

        blob_service_client = BlobServiceClient.from_connection_string(conn_str=self.connection_string)
        container_client = blob_service_client.get_container_client(self.container_name)
        blobs = container_client.list_blobs(name_starts_with=directory_path)
        blob_names = [blob.name for blob in blobs]

        return blob_names


    def delete_blob_from_data_lake(self,blob_name):

        blob = BlobClient.from_connection_string(conn_str=self.connection_string, container_name=self.container_name,blob_name=blob_name)
        blob.delete_blob()


    def move_blob_from_source_to_destination(self, source_blob_name, destination_blob_name):

        source_blob_client = BlobClient.from_connection_string(conn_str=self.connection_string, container_name=self.container_name,blob_name=source_blob_name)
        destination_blob_client = BlobClient.from_connection_string(conn_str=self.connection_string, container_name=self.container_name,blob_name=destination_blob_name)

        destination_blob_client.start_copy_from_url(source_blob_client.url)
        source_blob_client.delete_blob()
