from datalakecontainer import *
from processing import *
import json
import pandas as pd


# Getting mapping sheet
storage_account_name = 'dlsfadwhdev'
connection_string = 'DefaultEndpointsProtocol=https;AccountName=dlsfadwhdev;AccountKey=ZJ+p2fUqWsMa0c4Y3+6B9b2efwDclOS+ju4gLNfUsArKb+9bRJWrNudDoibteFrwDcCp5IzpH2Wi+ASt5wfwpA==;EndpointSuffix=core.windows.net'
container_name = 'fa-data-lake-dev'
my_data_lake = DataLakeContainer(storage_account_name, connection_string, container_name)
# print(my_data_lake.storage_account_name)

#Get the mapping sheet and convert from string to a list
premium_mapping = my_data_lake.read_file_from_data_lake('test/mapping/fa_claim_record_layout.json').decode('utf-8')
premium_mapping = json.loads(premium_mapping)
#print(type(premium_mapping))
# print(premium_mapping)

# Getting test file
storage_account_name = 'dlsfadwhdev'
connection_string = 'DefaultEndpointsProtocol=https;AccountName=dlsfadwhdev;AccountKey=ZJ+p2fUqWsMa0c4Y3+6B9b2efwDclOS+ju4gLNfUsArKb+9bRJWrNudDoibteFrwDcCp5IzpH2Wi+ASt5wfwpA==;EndpointSuffix=core.windows.net'
container_name = 'tempuipdata'
my_data_lake = DataLakeContainer(storage_account_name, connection_string, container_name)
# print(my_data_lake.storage_account_name)

file_name = 'PAUTO.D23060.V001.C91192'
test_file = my_data_lake.read_file_from_data_lake(file_name)
#print(test_file)

#Cleanse file into list
test1 = FileCleanse(test_file)
cleaned_data = test1.decode_file()
#print(cleaned_data)



#Delimit cleaned file
premium_delimit_test1 = FileDelimit(premium_mapping,cleaned_data)
master_list = premium_delimit_test1.parse_text()
df = premium_delimit_test1.generate_dataframe(master_list)
csv_data = premium_delimit_test1.generate_csv_data(df)


#push csv data into storage account


# Getting mapping sheet
storage_account_name = 'dlsfadwhdev'
connection_string = 'DefaultEndpointsProtocol=https;AccountName=dlsfadwhdev;AccountKey=ZJ+p2fUqWsMa0c4Y3+6B9b2efwDclOS+ju4gLNfUsArKb+9bRJWrNudDoibteFrwDcCp5IzpH2Wi+ASt5wfwpA==;EndpointSuffix=core.windows.net'
container_name = 'fa-data-lake-dev'
data_lake_landing = DataLakeContainer(storage_account_name, connection_string, container_name)
# print(my_data_lake.storage_account_name)
data_lake_landing.upload_file_to_data_lake(f'test/raw/{file_name}.csv',csv_data)



