
import requests, zipfile, io, sys
import json
import xlrd
import pandas as pd
import urllib.request

def fetch_data():
    print("fetching data...")
    # populate proejcts array with a complete list of project IDs 
    # for the amphibianDiseaseTeam
    amphibianDiseaseTeamID = 45
    projects = []
    url="https://api.geome-db.org/projects/stats?"
    r = requests.get(url)
    for project in json.loads(r.content):
        projectConfigurationID = project["projectConfiguration"]["id"]
        if (projectConfigurationID == amphibianDiseaseTeamID):
            projects.append(project["projectId"])
    projectsString = "["+ ','.join(str(e) for e in projects) + "]"

    # get the initial URL
    url="https://api.geome-db.org/records/Sample/excel?networkId=1&q=_projects_:" + projectsString +"+_select_:%5BEvent,Sample,Diagnostics%5D"
    r = requests.get(url)

    excel_file_url = json.loads(r.content)['url']
    urllib.request.urlretrieve(excel_file_url, filename)


def process_data():
    print("processing data...")
    # read the excel file url into a dataframe using pandas
    SamplesDF = pd.read_excel(filename,sheet_name='Samples')
    #SamplesDF.to_excel("data/temp_samples.xlsx")
    EventsDF = pd.read_excel(filename,sheet_name='Events')
    #EventsDF.to_excel("data/temp_events.xlsx")
    DiagnosticsDF = pd.read_excel(filename,sheet_name='Diagnostics')
    #DiagnosticsDF.to_excel("data/temp_diagnostics.xlsx")

    print("grouping results ...")
    group = SamplesDF.groupby('genus')['genus'].count()
    group.to_json(r'data/genus.json')

    group = EventsDF.groupby('country')['country'].count()
    group.to_json(r'data/country.json')
    
    group = EventsDF.groupby('yearCollected')['yearCollected'].count()
    group.to_json(r'data/yearCollected.json')

    group = DiagnosticsDF.groupby('diseaseDetected')['diseaseDetected'].count()
    group.to_json(r'data/diseaseDetected.json')

filename = 'data/temp_output.xlsx'
fetch_data()
process_data()
