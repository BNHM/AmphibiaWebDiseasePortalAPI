
import requests, zipfile, io
import json
import xlrd
import pandas as pd

# get the initial URL
url="https://api.geome-db.org/records/Sample/excel?networkId=1&q=_projects_:255+_select_:%5BEvent,Sample%5D"
r = requests.get(url)
excel_file_url = json.loads(r.content)['url']

# read the excel file url into a dataframe using pandas
df = pd.read_excel(excel_file_url,sheet_name='Samples')

# group by genus results
group = df.groupby('genus')['genus'].count()
group.to_json(r'data/genus.json')

# group by country results
group = df.groupby('country')['country'].count()
group.to_json(r'data/country.json')


