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
def process_raw_files(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    #file_name = 'PAUTO.D23037.V001.C90791'#'PAUTO.D23032.V001.C90791'#'PAUTO.D23032.V001.C91691'

    #premium_values 
    premium_values = ['1','2','3','4','J','K','L','M']
    #file name
    file_name = myblob.name.rsplit('/',1)[-1]
    file_path = 'test/raw/landing/'+ str(myblob.name.rsplit('/',1)[-1])#'PAUTO.D23032.V001.C90791' #premium#'PAUTO.D23118.V001.C91192'#claim  #'PAUTO.D23037.V001.C90791'

    # file_name = 'test/raw/landing/PAUTO.D23118.V001.C91192'
    # Getting test raw file
    # container_name = 'tempuipdata'
    container_name = 'fa-data-lake-dev'
    storage_account_name = 'dlsfadwhdev'
    my_vault = KeyVault('kv-fa-dwh-dev')
    connection_string = my_vault.get_secret('dl-connection-string')
    my_data_lake_raw_file = DataLakeContainer(storage_account_name, connection_string, container_name)
    raw_file = my_data_lake_raw_file.read_file_from_data_lake(file_path)
    #Cleanse file into list
    file1 = FileCleanse(raw_file)
    cleaned_data = file1.decode_file()
    #if the 16th charcter exists in the premium values, use premium mapping
    if str(cleaned_data[1][15]) in (premium_values): #if cleaned_data[0][15] == 1:
        parent_folder = 'premium_record'
        mapping_blob = 'test/mapping/fa_premium_record_layout.json'
    else:
        parent_folder = 'claim_record'
        mapping_blob = 'test/mapping/fa_claim_record_layout.json'
    # Getting mapping sheet and convert from string to a list
    container_name = 'fa-data-lake-dev'
    my_data_lake = DataLakeContainer(storage_account_name, connection_string, container_name)
    mapping_file = my_data_lake.read_file_from_data_lake(mapping_blob).decode('utf-8')
    mapping_file = json.loads(mapping_file)
    #Delimit cleaned file
    premium_delimit_test1 = FileDelimit(mapping_file,cleaned_data)
    master_list = premium_delimit_test1.parse_text()
    df = premium_delimit_test1.generate_dataframe(master_list)
    csv_data = premium_delimit_test1.generate_csv_data(df)
    #Push csv data into storage account with UIP
    container_name_pii = 'fa-data-lake-pii-dev'
    data_lake_landing_pii = DataLakeContainer(storage_account_name, connection_string, container_name_pii)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    data_lake_landing_pii.upload_file_to_data_lake(f'{parent_folder}/{file_name}_{timestamp}.csv',csv_data)
    #Take out (UIP) columns and push into different directory
    # container_name = 'fa-data-lake-dev'
    # data_lake_landing = DataLakeContainer(storage_account_name, connection_string, container_name)
    filtered_columns = [col for col in df.columns if '(UIP)' not in col]
    no_pii_df = df[filtered_columns]
    csv_data_no_pii = premium_delimit_test1.generate_csv_data(no_pii_df)
    my_data_lake.upload_file_to_data_lake(f'test/raw/{parent_folder}/{file_name}_{timestamp}.csv',csv_data_no_pii)
    return print('Success')

