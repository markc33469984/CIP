#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

get_ipython().system('pip install wget')
import wget
import os
import pandas as pd

output_dir = '/home/student/Project/'
output_file = 'Covid_19_SI.csv'
url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
selected_columns = ["location","continent","date","total_cases","new_cases","stringency_index","population"]


def download_data(output_dir, output_file, url):
    '''
    Downloads the data from a given url
    :param output_dir: the directory path to which the data must be dowloaded
    :param output_file: the name of the file under which the data must be saved
    :param url: the url from which the data is read
    '''
    assert os.path.exists(output_dir)
    wget.download(url, out = os.path.join(output_dir,output_file))
    
def convert_to_csv(file_path, selected_columns,csv_name):
    '''
    Converts the extracted data to a .csv file after selecting only the columns required from the original dataset
    '''
    df = pd.read_csv(file_path)
    df_selected = df[selected_columns]
    df_selected.to_csv(csv_name, index = False)
    
def main():
    download_data(output_dir,output_file,url)
    convert_to_csv(os.path.join(output_dir,output_file), selected_columns,'StudentC_SourceF_covid19_src.csv')

if __name__ == "__main__":
    main()
    


# In[14]:


pd.read_csv("StudentC_SourceF_covid19_src.csv")


# In[ ]:




