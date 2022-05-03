
import requests, zipfile, io, sys
import json
import xlrd
import numpy as np
import pandas as pd
import urllib.request
from io import TextIOWrapper
from gzip import GzipFile
import ssl

# hold scientificName objects which 
class scientificNames:
    def __init__(self, name, family, order, verbatim):  
        self.name = name  
        self.projects = list()
        self.family = family
        self.order = order
        self.verbatim = verbatim
    def add_project(self, projectCounter):
        self.projects.append(projectCounter) 
        
class projectCounter:
    def __init__(self, projectId, count):  
        self.projectId = projectId
        self.count = count

# fetch data from GEOME that matches the Amphibian Disease TEAM and put into an easily queriable format.
def fetch_data():
    print("fetching data...")
    # populate proejcts array with a complete list of project IDs 
    # for the amphibianDiseaseTeam
    amphibianDiseaseTeamID = 45    
    df = pd.DataFrame(columns = columns)
     
    # this will fetch a list of ALL projects from GEOME        
    url="https://api.geome-db.org/projects/stats?"
    r = requests.get(url)
    for project in json.loads(r.content):
        projectConfigurationID = project["projectConfiguration"]["id"]
        # filter for just projects matching the teamID
        if (projectConfigurationID == amphibianDiseaseTeamID and project["public"]):
        # condition for testing a single project
        #if (project["projectId"] == 249):
            
            url="https://api.geome-db.org/records/Event/excel?networkId=1&q=_projects_:" + str(project["projectId"]) + "+_select_:%5BSample,Diagnostics%5D"
            r = requests.get(url)

            if (r.status_code == 204):
                print ('no data found for project = ' + str(project["projectId"]))
            #elif (str(project["projectId"]) =="221"):
            else:
                print("processing data for project = " + str(project["projectId"]))

                temp_file = 'data/project' + str(project["projectId"]) + ".xlsx"                
                
                print(url)

                excel_file_url = json.loads(r.content)['url']
                
                print(excel_file_url)
                #disable ssl for next request, server returning invalid cert
                # on URL link
                ssl._create_default_https_context = ssl._create_unverified_context
                urllib.request.urlretrieve(excel_file_url, temp_file)
                           
                thisDF = pd.read_excel(temp_file,sheet_name='Samples',na_filter=False, engine='xlrd')                                
    
                thisDF = thisDF.reindex(columns=columns)
                
                thisDF = thisDF.replace(np.nan, '', regex=True) 

                thisDF = thisDF.astype(str)
                
                # normalize true/false to all upper case
                thisDF['diseaseDetected'] = thisDF['diseaseDetected'].str.upper() 
                thisDF['fatal'] = thisDF['fatal'].str.upper() 
                thisDF['diseaseTested'] = thisDF['diseaseTested'].str.capitalize() 

                thisDF['sampleType'] = thisDF['sampleType'].replace('external Swab','swabbing')
                thisDF['sampleType'] = thisDF['sampleType'].replace('external swab','swabbing')
                thisDF['sampleType'] = thisDF['sampleType'].replace('Swabbing','swabbing')
                thisDF['sampleType'] = thisDF['sampleType'].replace('Other','other')
                
                # process names
                thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('sp.','')
                thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('sp','')
                thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('cf','') 
                thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('cf.','')
                thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('sp.2','')                            
                thisDF['genus'] = thisDF['genus'].str.replace(r'sp\..*', 'sp.')
                thisDF['genus'] = thisDF['genus'].str.replace(r'sp .*', 'sp.')
                thisDF['scientificName'] = thisDF['genus'] + " " + thisDF['specificEpithet']
                thisDF['scientificName'] = thisDF['scientificName'].str.strip()
                thisDF['scientificName'] = thisDF['scientificName'].str.capitalize()
                # set the verbatim name before running taxonomize
                thisDF['verbatimScientificName'] = thisDF['scientificName']                 
                 
                thisDF = taxonomize(thisDF)
                
                thisDF['projectURL'] = str("https://geome-db.org/workbench/project-overview?projectId=") + thisDF['projectId'].astype(str)                
                    
                df = df.append(thisDF,sort=False)
     
    print("writing final data...")            
    # write to an excel file, used for later processing
    #df.to_excel(processed_filename,index=False)    
    df.to_csv(processed_csv_filename,index=False)    
    # Create a compressed output file so people can view a limited set of columns for the complete dataset
    df = df.reset_index() 
    df.index.name = 'index'
    SamplesDFOutput = df.reindex(columns=columns)
    api.write("|"+processed_csv_filename_zipped+"|Zipped version of all core metadata fields for every public project|\n")
    #SamplesDFOutput.to_csv(processed_csv_filename_zipped, index=False)                                            
    to_gzip_csv_no_timestamp(SamplesDFOutput,processed_csv_filename_zipped)
  
def test_data_writing():
    in_file = 'data/project221.xlsx'
    out_file = 'data/temp.xlsx'
    thisDF = pd.read_excel(in_file,sheet_name='Samples',na_filter=False, engine='xlrd')                                
    thisDF = thisDF.reindex(columns=columns)    
    thisDF = thisDF.replace(np.nan, '', regex=True) 
    thisDF = thisDF.astype(str)
    
    thisDF['diseaseDetected'] = thisDF['diseaseDetected'].str.upper()                 
    thisDF['fatal'] = thisDF['fatal'].str.upper()                 
    # remove unknown specificEpithet's
    thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('sp.','')
    thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('sp','')
    thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('cf','') 
    thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('cf.','')
    thisDF['specificEpithet'] = thisDF['specificEpithet'].replace('sp.2','')            
            
           
    thisDF['scientificName'] = thisDF['genus'] + " " + thisDF['specificEpithet']
    thisDF['scientificName'] = thisDF['scientificName'].str.strip()

    thisDF['scientificName'] = thisDF['scientificName'].str.capitalize()  
                
    thisDF = taxonomize(thisDF)
    thisDF.to_excel(out_file,index=False, engine='xlrd')    

    
def to_gzip_csv_no_timestamp(df, f, *kwargs):
    # Write pandas DataFrame to a .csv.gz file, without a timestamp in the archive
    # header, using GzipFile and TextIOWrapper.
    #
    # Args:
    #     df: pandas DataFrame.
    #     f: Filename string ending in .csv (not .csv.gz).
    #     *kwargs: Other arguments passed to to_csv().
    #
    # Returns:
    #     Nothing.
    with TextIOWrapper(GzipFile(f, 'w', mtime=0), encoding='utf-8') as fd:
        df.to_csv(fd, *kwargs)

# function to grab latest amphibiaweb taxonomy
def fetchAmphibianTaxonomy():
    # fetch from URL
    url="https://amphibiaweb.org/amphib_names.json"
    r = requests.get(url)
    return json.loads(r.content)

    # Opening JSON file.  Temporary!
    #f = open('amphib_names.json',) 
    #return json.load(f) 

# update taxonomic fields using amphibiaweb taxonomy
# updates scientificName using synonyms 
# updates family and order using higher level taxonomy by searching on genus name
def taxonomize(df):
    taxonomy = fetchAmphibianTaxonomy()
    synDict = {}
    familyDict = {}
    orderDict = {}
    for species in taxonomy:                
        familyDict[species['genus']] = species['family']
        orderDict[species['genus']] = species['order']
        if (species['synonymies'] != ''):
            synonym =  species['synonymies'].split(",") 
            for s in synonym:                  
                synDict[s] = species['genus'] + " " + species['species']
    
    df['scientificName'].replace(synDict, inplace=True)
    df['genus'] = df['scientificName'].str.split(" ").str[0]
    df['specificEpithet'] = df['scientificName'].str.split(" ").str[1]
    df['family'] = df['genus'].map(familyDict)
    df['order'] = df['genus'].map(orderDict)

    return df

# function to write tuples to json from pandas group by
# using two group by statements.
def json_tuple_writer(group,name,filename,definition):
    api.write("|"+filename+"|"+definition+"|\n")
    jsonstr = '[\n'
    namevalue = ''
    for rownum,(indx,val) in enumerate(group.iteritems()):                
        
        thisnamevalue = str(indx[0])
        
        if (namevalue != thisnamevalue):
            jsonstr+="\t{"
            jsonstr+="\""+name+"\":\""+thisnamevalue+"\","
            jsonstr+="\""+str(indx[1]).replace('"',"") +"\":"+str(val)  
            jsonstr+="},\n"                   
        else:
            jsonstr = jsonstr.rstrip("},\n")
            jsonstr+=",\""+str(indx[1]).replace('"',"") +"\":"+str(val)  
            jsonstr+="},\n"                           
        
        namevalue = thisnamevalue                
        
    jsonstr = jsonstr.rstrip(',\n')

    jsonstr += '\n]'
    with open(filename,'w') as f:
        f.write(jsonstr)
        
        
# function to write tuples to json from pandas group by
# using two group by statements.
def json_tuple_writer_scientificName_projectId(group,name):
    projectId = ''
    thisprojectId = ''
    jsonstr = ''
    firsttime = True
    for rownum,(indx,val) in enumerate(group.iteritems()):  
        #print(str(indx[0]),str(indx[1]), str(val))              
        thisprojectId = str(indx[0])
        if (projectId != thisprojectId):
            # End of file
            if firsttime == False:                
                jsonstr = jsonstr.rstrip(',\n')
                jsonstr += "\n]"            
                with open('data/scientificName_projectId_' + projectId + ".json",'w') as f:
                    f.write(jsonstr)                      
            # Beginning of file
            jsonstr = "[\n"
            jsonstr += ("\t{\"scientificName\":\"" + str(indx[1]).replace('"',"") + "\",\"value\":"+str(val) +"},\n" )

            api.write("|data/scientificName_projectId_"+thisprojectId +".json|unique scientificName count for project "+thisprojectId+"|\n")                
        else:                                    
            jsonstr += ("\t{\"scientificName\":\"" + str(indx[1]).replace('"',"") + "\",\"value\":"+str(val) +"},\n" )

            
        projectId = thisprojectId

        
        firsttime = False            
    
    # write the last one
    jsonstr = jsonstr.rstrip(',\n')
    jsonstr += "\n]"
    with open('data/scientificName_projectId_' + thisprojectId +".json",'w') as f:
                f.write(jsonstr)        
         
# Create a file for each scientificName listing the projects that it occurs in.
def json_tuple_writer_scientificName_listing(group,name,df):
    scientificName = ''
    thisscientificName = ''
    jsonstr = ''
    firsttime = True
    scientificNameList = list()
    s = scientificNames('','','','')
    
    # loop all grouped names & projects and populate list of objects
    # from these we will construct JSONS downstream
    for rownum,(indx,val) in enumerate(group.iteritems()):          
        thisscientificName = str(indx[0])
        thisfamily = str(indx[1])
        thisorder = str(indx[2])
        projectId = str(indx[3])
        thisverbatimScientificName = str(indx[4])

        count = str(val)                              
        if (scientificName != thisscientificName): 
            if firsttime:
                s = scientificNames(thisscientificName,thisfamily,thisorder,thisverbatimScientificName)             
                s.add_project(projectCounter(projectId,count)) 
            else:    
                scientificNameList.append(s)
                s = scientificNames(thisscientificName,thisfamily,thisorder,thisverbatimScientificName)       
                s.add_project(projectCounter(projectId,count))                                                       
        else:
            s.add_project(projectCounter(projectId,count))         
        scientificName = thisscientificName    
        firsttime = False    

    # construct JSON output
    jsonstr = ("[\n")
    for sciName in scientificNameList:                
        jsonstr += ("\t{\"scientificName\" : \"" + sciName.name.replace('"',"") + "\" , ")
        jsonstr += ("\"order\" : \"" + sciName.order + "\" , ")
        jsonstr += ("\"family\" : \"" + sciName.family + "\", ")
        jsonstr += ("\"verbatimScientificName\" : \"" + sciName.verbatim + "\", ")


        jsonstr += ("\"associatedProjects\" : [" )
        for project in sciName.projects:
            jsonstr += ("{\"projectId\" : \"" + project.projectId + "\" , \"count\" : " + project.count  + "},")
        jsonstr = (jsonstr.rstrip(','))        
        jsonstr += ("]},\n")
    jsonstr = (jsonstr.rstrip(',\n'))        
    jsonstr += ("\n]")
                
    with open('data/scientificName_listing.json','w') as f:
        f.write(jsonstr) 
    api.write("|scientificName_listing.json|All scientific names and the projects that they appear in|\n")
        
# function to write JSON from pandas groupby
def json_writer(group,name,filename,definition):
    api.write("|"+filename+"|"+definition+"|\n")
    
    jsonstr = '[\n'
    for (rownum,val) in enumerate(group.iteritems()):                        
        jsonstr+="\t{"
        jsonstr+="\""+name+"\":\""+str(val[0]).replace('"',"")+"\","            
        jsonstr+="\"value\":"+str(val[1])  
        jsonstr+="},\n"                   
        
        
    jsonstr = jsonstr.rstrip(',\n')
    jsonstr += '\n]'
    with open(filename,'w') as f:
        f.write(jsonstr)

    
def run_grouped_data(df,name):
    
    bd = df.diseaseTested.str.contains('Bd')
    bd = df[bd]  
    
    bsal = df.diseaseTested.str.contains('Bsal')
    bsal = df[bsal]  
    
    # groupby, filter on Bd,Bsal,Both for name
    group = df.groupby(name)[name].size()    
    json_writer(group,name,'data/'+name+'_Both.json','Bd and Bsal counts grouped by '+name)      
    
    group = bd.groupby(name)[name].size()
    json_writer(group,name,'data/'+name+'_Bd.json','Bd counts grouped by '+name)  
    
    group = bsal.groupby(name)[name].size()
    json_writer(group,name,'data/'+name+'_Bsal.json','Bsal counts grouped by '+name)  

    # groupby, filter on Bd,Bsal,Both for name+diseaseDetected
    group = df.groupby([name,'diseaseDetected']).size()
    json_tuple_writer(group,name,'data/'+name+'_diseaseDetected_Both.json','Bd and Bsal counts grouped by presence-abscense and by '+name)
    
    group = bd.groupby([name,'diseaseDetected']).size()
    json_tuple_writer(group,name,'data/'+name+'_diseaseDetected_Bd.json','Bd counts grouped by presence-abscense and by '+name)
    
    group = bsal.groupby([name,'diseaseDetected']).size()
    json_tuple_writer(group,name,'data/'+name+'_diseaseDetected_Bsal.json','Bsal counts grouped by presence-abscense and by '+name)
    
    # groupby, filter on Bd,Bsal,Both for name+diseaseTested         
    group = df.groupby([name,'diseaseTested']).size()
    json_tuple_writer(group,name,'data/'+name+'_Both_stacked.json','Bd and Bsal counts for a stacked chart, grouped by '+name)
    

            
def group_data():  
    print("reading processed data ...")
    #df = pd.read_excel(processed_filename, engine='xlrd')
    df = pd.read_csv(processed_csv_filename)
    
    print("grouping results ...")  
    # genus, country, yearCollected results
    run_grouped_data(df,'genus')
    run_grouped_data(df,'scientificName')
    run_grouped_data(df,'country')
    run_grouped_data(df,'yearCollected')

    # summary tables for diseaseDetected and diseaseTested
    bd = df.diseaseTested.str.contains('Bd')
    bd = df[bd]  
    
    bsal = df.diseaseTested.str.contains('Bsal')
    bsal = df[bsal]  
    

    # diseaseDetected
    group = df.groupby('diseaseDetected')['diseaseDetected'].size()
    json_writer(group,'diseaseDetected','data/diseaseDetected_Both.json','Bd and Bsal counts grouped by presence-absence')
    
    group = bd.groupby('diseaseDetected')['diseaseDetected'].size()
    json_writer(group,'diseaseDetected','data/diseaseDetected_Bd.json','Bd counts grouped by presence-absence')
    
    group = bsal.groupby('diseaseDetected')['diseaseDetected'].size()
    json_writer(group,'diseaseDetected','data/diseaseDetected_Bsal.json','Bsal counts grouped by presence-absence')
        
    # diseaseTested
    group = df.groupby('diseaseTested')['diseaseTested'].size()
    json_writer(group,'diseaseTested','data/diseaseTested_Both.json','Bd and Bsal counts')    
    
    # scientificName by projectId
    group = df.groupby(['projectId','scientificName']).size()
    json_tuple_writer_scientificName_projectId(group,'projectId')
    
    # scientificName listing
    group = df.groupby(['scientificName','family','order','projectId','verbatimScientificName']).size()
    json_tuple_writer_scientificName_listing(group,'scientificName',df)


api = open("api.md","w")
api.write("# API\n\n")
api.write("Amphibian Disease Portal API Documentation.  The following files are updated every evening and can be called directly by pointing to their raw form in this github repository.\n")
api.write("|filename|definition|\n")
api.write("|----|---|\n")

# global variables
#columns = ['materialSampleID','diseaseTested','diseaseDetected','order','family','genus','specificEpithet','country','decimalLatitude','decimalLongitude','yearCollected','projectId']
columns = [
'materialSampleID',
'diseaseTested',
'diseaseDetected',
'principalInvestigator',
'country',
'decimalLatitude',
'decimalLongitude',
'locality',
'yearCollected',
'coordinateUncertaintyInMeters',
'collectorList',
'basisOfRecord',
'order',
'family',
'genus',
'specificEpithet',
'sampleType',
'fatal',
'Default Group',
'continentOcean',
'stateProvince',
'municipality',
'county',
'locationRemarks',
'verbatimEventDate',
'monthCollected',
'dayCollected',
'horizontalDatum',
'georeferenceProtocol',
'minimumElevationInMeters',
'maximumElevationInMeters',
'minimumDepthInMeters',
'maximumDepthInMeters',
'locationID',
'habitat',
'eventRemarks',
'Record and Owner Details',
'occurrenceID',
'institutionCode',
'collectionCode',
'catalogNumber',
'otherCatalogNumbers',
'fieldNumber',
'associatedReferences',
'occurrenceRemarks',
'Taxonomy and Life History',
'infraspecificEpithet',
'taxonRemarks',
'lifeStage',
'establishmentMeans',
'sex',
'Protocol and Storage Details',
'individualCount',
'Measurements',
'weightUnits',
'weight',
'lengthUnits',
'length',
'Diagnostics and Traits',
'diseaseLineage',
'genotypeMethod',
'testMethod',
'diseaseTestedPositiveCount',
'specimenDisposition',
'quantityDetected',
'dilutionFactor',
'cycleTimeFirstDetection',
'zeScore',
'diagnosticLab',
'projectId',
'Sample_bcid'
]

processed_csv_filename = 'data/amphibian_disease_data_processed.csv'
processed_csv_filename_zipped = 'data/amphibian_disease_data_processed.csv.gz'

#test_data_writing()
fetch_data()
group_data()

api.close()
