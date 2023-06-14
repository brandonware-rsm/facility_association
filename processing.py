import csv
import pandas as pd

class FileCleanse:
    def __init__(self, data):
        self.data = data
    def decode_file(self):
        coded_data = self.data.decode('utf-8')
        cleaned_data = coded_data.split('\r\n')
        return cleaned_data
  
# test1 = FileCleanse(test_file)
# cleaned_data = test1.decode_file()
# print(cleaned_data)

# coded_data = test_file.decode('utf-8')#.replace('\r\n','\n')
# cleaned_data = coded_data.split('\r\n')
# #df = pd.DataFrame(lines)
# print(cleaned_data[0])


class FileDelimit:
    def __init__(self,mapping, cleaned_data):
        self.mapping = mapping
        self.cleaned_data = cleaned_data
    def parse_text(self):
        master_list = []
        for row in self.cleaned_data:
            line = {}
            for item in self.mapping:
                table_name = item['Table']
                data_element = item['Data Element']
                start = item['From']
                end = item['To']

                value = row[start-1:end].strip()
                line[data_element] = value

            master_list.append(line)
        return master_list
    def generate_dataframe(self,master_list):
        df = pd.DataFrame(master_list)
        return df
    def generate_csv_data(self,df):
        csv_data = df.to_csv(index=False)
        return csv_data





    



