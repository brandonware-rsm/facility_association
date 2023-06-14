import csv
import json
import pandas as pd

#Premium Layout
# Set the Variables / input and output file names
csv_file = 'fa_premium_record_layout.csv'
output_file = 'fa_premium_record_layout.json'

# Read the csv file in
data = pd.read_csv(csv_file,encoding='utf-8')
#print(data)

# Convert into a list of dictionaries
dict_list= data.to_dict('records')

#print(dict_list)

# Push the list of dictionaries into a json file
with open(output_file, 'w') as file:
    json.dump(dict_list, file)



#Claim Record
# Set the Variables / input and output file names
csv_file = 'fa_claim_record_layout.csv'
output_file = 'fa_claim_record_layout.json'

# Read the csv file in
data = pd.read_csv(csv_file,encoding='utf-8')
#print(data)

# Convert into a list of dictionaries
dict_list= data.to_dict('records')

#print(dict_list)

# Push the list of dictionaries into a json file
with open(output_file, 'w') as file:
    json.dump(dict_list, file)


