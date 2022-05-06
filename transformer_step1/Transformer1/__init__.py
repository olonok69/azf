from typing import List
import logging
import json
import pandas as pd
import os
import sys
import pyodbc 
from sqlalchemy import create_engine
import urllib
import azure.functions as func
import uuid

# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
# SQL server Connection paramethers
server = "sql-digital-sandbox.database.windows.net"
database = "data-ingestion" 
username = "superuser" 
password = ""
matches_table = "Animal_Data_matches"
reporting_table = "Animal_data_reporting"



dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)
# breeds_code Dataset
breeds_file = os.path.join(dir_path, 'data', 'cv_breed_codes.csv')
breeds=pd.read_csv(breeds_file)

def main(events: List[func.EventHubEvent]):
    for event in events:
        logging.info('Python EventHub trigger processed an event: %s',
                        event.get_body().decode('utf-8'))

def map_prob(prob:float)-> bool:
    """
    If there is a match , so probability above 0 , return True, otherwise False
    """
    if prob > 0:
        return True
    else:
        return False

# Map to breed category
def map_to_breed_category(breed_code:str) -> (bool, bool, bool):
    """
    Return 3 fields if there is a breed Code Match. dataframe use breeds
    breed = breed type String
    cat_beef = Boolean Beef
    cat_dairy = Boolean Dairy
    """
    breed, cat_beef, cat_dairy = "", False, False
    if breed_code==None:
        breed_code=""

    r = breeds[breeds['Code'].astype('string') == breed_code.upper()]

    if len(r):
        x = r.iloc[0]
        breed = x.Breed
        cat_beef = bool(x.Beef)
        cat_dairy = bool(x.Dairy)

    return breed, cat_beef, cat_dairy

def map_to_sexed(naab:float)-> bool:
    """
    return True/False if sire_naab start with a number included in dictionary
    https://www.naab-css.org/naab-marketing-codes
    """
    naab=str(naab)
    Sexed = {523, 629, 529, 594, 501, 530, 531, 532, 533, 507, 514, 614, 777, 511, 611, 509, 629, 694, 602, 603, 604, 605}
 
    return bool(len({s for s in Sexed if naab.startswith(str(s))}))


def transform1_json(blob_to_text):
    """
    Receive a Json File after sire_mapping processing
    1 covert into a dataframe sire_predicted_matches column (contains all matches)
    2 select the record with higher probability
    3 convert this record to a json object
    4 create a new key in blob_to_text sire_match and assing json from step 3
    return object and dataframe of step 1
    """

    event_es=json.dumps(blob_to_text)

    data = json.loads(event_es)

    # transform Matches object to a datafrane
    df = pd.DataFrame(data['sire_predicted_matches'])
    # convert to Json Dataframe, orient records
    json_string=df.sort_values('match_prob', ascending=False)[:1].to_json(orient='records')
    # string to json, ready to return
    json_obj = json.loads(json_string)

    blob_to_text['sire_match']=json_obj
    
    return blob_to_text, df


def transform2_json(blob_to_text):
    """
    1 Receive the json blob_to_text and convert to dataframe
    2 explode the dataframe Data structure into dfull dataframe
    3 explode the sire_match data structure into dfull dataframe
    4 concat using axis 1 dfull and dfull2
    5 drop sire_match
    6 rename columns
    """

    data1 = json.loads(json.dumps(blob_to_text))
    dfull = pd.json_normalize(data1)
    dfull2 = pd.json_normalize(data1['sire_match'])
    df_final=pd.concat([dfull, dfull2], axis=1)
    df_final=df_final.drop('sire_match', axis=1)
    df_final.columns=['FarmId', 'EventType', 'sire_predicted_matches', 'herdID',
       'animalName', 'number', 'earTag', 'birthDate',
       'isBornOnFarm', 'gender', 'animalType', 'parity',
       'damEarTag', 'damName', 'sireEarTag', 'sireName',
       'transponder', 'transponder2', 'sire_key', 'sire_match_prob',
       'sire_bovine_id', 'sire_country', 'sire_breed', 'sire_id_number', 'sire_stud_code', 'sire_breed_code',
       'sire_naab', 'sire_full_naab', 'sire_short_name', 'sire_registered_name',
       'sire_country_alpha_two', 'sire_country_alpha_three', 'sire_country_numeric']
    
    return df_final


def transform3_df(df_final, identyfier):
    """
    1 apply map_prob function
    2 apply map_to_category function
    3 apply map_to_sexed function
    4 crate a column with the identifier generated for this record
    5 drop sire_predicted_matches
    
    """

    df_final['bovine_matched']=df_final['sire_match_prob'].apply(map_prob)
    df_final['breed_mapped'], df_final['cat_beef'], df_final['cat_dairy']= zip(*df_final['sire_breed_code'].apply(map_to_category))
    df_final['sexed']=df_final['sire_naab'].apply(map_to_sexed)
    df_final['id']=identyfier
    df_final.drop('sire_predicted_matches', axis=1, inplace=True)

    return df_final



def mycreate_engine(server,database,username, password):
    """
    Create a Sqlalchemi connection
    """
    quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    return engine
    
def append_Animal_Data_matches(df,server,database,username, password, identyfier, matches_table):
    """
    Update Table Animal_Data_matches with all maches comming from sire_mapping step into column sire_predicted_matches.
    here we also create a column id with the identifier generated for this record
    """
    engine=mycreate_engine(server,database,username, password)
    df['id']=identyfier
    df.to_sql(f"{matches_table}", engine, index=False, if_exists="append",schema="dbo")
    return


def append_Animal_Data_reporting(df,server,database,username, password, identyfier, matches_table):
    """
    Update Table Animal_data_reporting 
    """
    engine=mycreate_engine(server,database,username, password)
    df.to_sql(f"{matches_table}", engine, index=False, if_exists="append",schema="dbo")
    return

















""" cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
#df.to_sql('Animal_Data_matches', schema='dbo', con = engine, chunksize=200, method='multi', index=False, if_exists='replace')
result = engine.execute('SELECT * FROM [dbo].[Animal_Data_matches]')
print(result.fetchall())
 """

event_e={'FarmId': '10833', 'EventType': 'AnimalData', 
'Data': {'herdID': 10833, 'animalName': '243 Flathead', 'number': '66', 'earTag': 'IT 017992099009', 
'birthDate': '2018-11-12', 'isBornOnFarm': True, 'gender': '0', 'animalType': 0, 'parity': 0, 'damEarTag': 'IT 017991524743', 
'damName': '28 Dover', 'sireEarTag': 'IT 028990346857', 'sireName': 'Gegania Flathead-P', 'transponder': '7444000', 'transponder2': '7444000'},
 'sire_predicted_matches': [{'key': 408293, 'match_prob': 1.0, 'bovine_id': 2070502, 'country': 'ITA', 'breed': 'HO', 'id_number': '028990346857', 
 'stud_code': 29.0, 'breed_code': 'HO', 'naab': 18331.0, 'full_naab': '29HO18331', 'short_name': 'FLATHEAD-P', 'registered_name': 'GEGANIA FLATHEAD-P', 
 'country_alpha_two': 'IT', 'country_alpha_three': 'ITA', 'country_numeric': 380.0}, {'key': 408294, 'match_prob': 0.75, 'bovine_id': 2070502, 'country': 
 'USA', 'breed': 'HO', 'id_number': '028990346857', 'stud_code': 29.0, 'breed_code': 'HO', 'naab': 18331.0, 'full_naab': '29HO18331', 'short_name': 'FLATHEAD-P', 'registered_name': 
 'GEGANIA FLATHEAD-P', 'country_alpha_two': 'US', 'country_alpha_three': 'USA', 'country_numeric': 840.0}]}


json1, data_matches=transform1_json(event_e)
identyfier=uuid.uuid4().hex
print(identyfier)
# insert data in animal data table
append_Animal_Data_matches(data_matches,server,database,username, password, identyfier, matches_table)

quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
result = engine.execute('SELECT distinct(id) FROM [dbo].[Animal_Data_matches]')
print(result.fetchall())

dataf=transform2_json(json1)
dataf2=transform3_df(dataf, identyfier)
# Append to animal data Reporting
append_Animal_Data_reporting(dataf2,server,database,username, password, identyfier, reporting_table)
result = engine.execute('SELECT distinct(id) FROM [dbo].[Animal_data_reporting]')
print(result.fetchall())


print(dataf2.columns)