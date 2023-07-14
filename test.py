import csv
import json
import pandas as pd
import azure.functions as func
import logging
from datalakecontainer import *
from processing import *
import json
import pandas as pd
from datetime import datetime
from keyvault import *


storage_account_name = 'dlsfadwhdev'
my_vault = KeyVault('kv-fa-dwh-dev')
connection_string = my_vault.get_secret('dl-connection-string')

#mapping_blob = 'raw/mapping/fa_premium_record_layout.json'

mapping_blob = 'raw/mapping/fa_premium_record_layout.json'
# Getting mapping sheet and convert from string to a list
container_name = 'fa-data-lake-dev'
my_data_lake = DataLakeContainer(storage_account_name, connection_string, container_name)
mapping_file = my_data_lake.read_file_from_data_lake(mapping_blob).decode('utf-8')
mapping_file = json.loads(mapping_file)

print(mapping_file)
# # df = pd.DataFrame(mapping_file)
# # df.to_csv('claims_cleaned.csv',index=False)
# #print(df)
# #Premium Layout
# # Set the Variables / input and output file names
# csv_file = 'C:\Repos\API_Testing\\premium_cleaned.csv'
# #print(csv_file)
# output_file = 'fa_premium_record_layout.json'

# # Read the csv file in
# data = pd.read_csv(csv_file,encoding='utf-8')
# #print(data)

# # Convert into a list of dictionaries
# dict_list= data.to_dict('records')

# #print(dict_list)

# # Push the list of dictionaries into a json file
# with open(output_file, 'w') as file:
#     json.dump(dict_list, file)



# #Claim Record
# # Set the Variables / input and output file names
# csv_file = 'C:\Repos\API_Testing\\claims_cleaned.csv'
# output_file = 'fa_claim_record_layout.json'

# # Read the csv file in
# data = pd.read_csv(csv_file,encoding='utf-8')
# #print(data)

# # Convert into a list of dictionaries
# dict_list= data.to_dict('records')

# #print(dict_list)

# # Push the list of dictionaries into a json file
# with open(output_file, 'w') as file:
#     json.dump(dict_list, file)

