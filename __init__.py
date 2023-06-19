from datalakecontainer import *
from processing import *
import json
import pandas as pd
from datetime import datetime

#Maybe two functions - one for claim, one for premium
#Loaded in two separate folders 
#Same process
#Both get placed into raw


def main():

    #Setting mapping sheet
    mapping_blob = 'test/mapping/fa_claim_record_layout.json'

    # Getting mapping sheet and convert from string to a list
    storage_account_name = 'dlsfadwhdev'
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=dlsfadwhdev;AccountKey=ZJ+p2fUqWsMa0c4Y3+6B9b2efwDclOS+ju4gLNfUsArKb+9bRJWrNudDoibteFrwDcCp5IzpH2Wi+ASt5wfwpA==;EndpointSuffix=core.windows.net'
    container_name = 'fa-data-lake-dev'
    my_data_lake = DataLakeContainer(storage_account_name, connection_string, container_name)
    # print(my_data_lake.storage_account_name)
    premium_mapping = my_data_lake.read_file_from_data_lake(mapping_blob).decode('utf-8')
    premium_mapping = json.loads(premium_mapping)
    # print(type(premium_mapping))
    # print(premium_mapping)

    # Getting test raw file
    file_name = 'PAUTO.D23060.V001.C91192'
    storage_account_name = 'dlsfadwhdev'
    connection_string = 'DefaultEndpointsProtocol=https;AccountName=dlsfadwhdev;AccountKey=ZJ+p2fUqWsMa0c4Y3+6B9b2efwDclOS+ju4gLNfUsArKb+9bRJWrNudDoibteFrwDcCp5IzpH2Wi+ASt5wfwpA==;EndpointSuffix=core.windows.net'
    container_name = 'tempuipdata'
    my_data_lake_raw_file = DataLakeContainer(storage_account_name, connection_string, container_name)
    # print(my_data_lake.storage_account_name)
    test_file = my_data_lake_raw_file.read_file_from_data_lake(file_name)
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
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    data_lake_landing.upload_file_to_data_lake(f'test/raw/with_pii/{file_name}_{timestamp}.csv',csv_data)

    #Take out (UIP) columns
    filtered_columns = [col for col in df.columns if '(UIP)' not in col]
    #print(filtered_columns)
    #print(df.columns)
    no_pii_df = df[filtered_columns]
    #print(no_pii_df)
    csv_data_no_pii = premium_delimit_test1.generate_csv_data(no_pii_df)
    data_lake_landing.upload_file_to_data_lake(f'test/raw/without_pii/{file_name}_{timestamp}.csv',csv_data_no_pii)


if __name__ == "__main__":
    main()
