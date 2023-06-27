import azure.functions as func
import logging
from datalakecontainer import *
from processing import *
import json
import pandas as pd
from datetime import datetime
from keyvault import *

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="fa-data-lake-dev/test/raw/landing/{name}",
                               connection="dlsfadwhdev_STORAGE") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    #Setting mapping sheet
    mapping_blob = 'test/mapping/fa_claim_record_layout.json'
    # Getting mapping sheet and convert from string to a list
    storage_account_name = 'dlsfadwhdev'
    my_vault = KeyVault('kv-fa-dwh-dev')
    connection_string = my_vault.get_secret('dl-connection-string')
    container_name = 'fa-data-lake-dev'
    my_data_lake = DataLakeContainer(storage_account_name, connection_string, container_name)
    premium_mapping = my_data_lake.read_file_from_data_lake(mapping_blob).decode('utf-8')
    premium_mapping = json.loads(premium_mapping)

    # Getting test raw file
    file_name = myblob.name.rsplit('/',1)[-1]
    container_name = 'tempuipdata'
    my_data_lake_raw_file = DataLakeContainer(storage_account_name, connection_string, container_name)
    test_file = my_data_lake_raw_file.read_file_from_data_lake(file_name)

    #Cleanse file into list
    test1 = FileCleanse(test_file)
    cleaned_data = test1.decode_file()

    #Delimit cleaned file
    premium_delimit_test1 = FileDelimit(premium_mapping,cleaned_data)
    master_list = premium_delimit_test1.parse_text()
    df = premium_delimit_test1.generate_dataframe(master_list)
    csv_data = premium_delimit_test1.generate_csv_data(df)

    #Push csv data into storage account with UIP
    container_name = 'fa-data-lake-dev'
    data_lake_landing = DataLakeContainer(storage_account_name, connection_string, container_name)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    data_lake_landing.upload_file_to_data_lake(f'test/raw/with_pii/{file_name}_{timestamp}.csv',csv_data)

    #Take out (UIP) columns and push into different directory
    filtered_columns = [col for col in df.columns if '(UIP)' not in col]
    no_pii_df = df[filtered_columns]
    csv_data_no_pii = premium_delimit_test1.generate_csv_data(no_pii_df)
    data_lake_landing.upload_file_to_data_lake(f'test/raw/without_pii/{file_name}_{timestamp}.csv',csv_data_no_pii)
