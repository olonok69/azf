import logging
import sys
from sys import path
import os
import json
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)

import azure.functions as func
from kapp_index import kapp_index

# define global variables
multiplier=325
additive=450

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    main function handlelling the request send to the entry point in a json Payload
    """
    logging.info('Python HTTP trigger function processed a request.')

    # read request
    payload=req.get_json()
    
    if payload:
        # read country
        country=payload['country'].upper()
        # setup Threshold for MS
        if country.upper()=='UK':
            MS_Threshold=1.18
        else:
            MS_Threshold=105

        # create kaap_index object       
        okapp=kapp_index('dummy',country, MS_Threshold, multiplier, additive)
        # read the trait sample from request payload , key--> Trait
        mydata=okapp.read_traits_json(payload)

        # read statistics file from conf directory
        path=os.path.join(dir_path, 'conf', 'all_stats.json')
        okapp.read_stat_json(path)
        # add statistics to the Trait Sample        
        data=okapp.data_from_stats(mydata)
        # Calculate Index
        kp=okapp.kapp_calculation()

    if kp:
        # return kaap  index value # agree if moving this into returning a JSON
        return func.HttpResponse(f"Kaap Index value, {kp}. Location: {country}.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

def load_conf(dir_path):

    path=dir_path+"\conf\\all_stats.json"
    print(path)
    with open(path, 'r') as fp:
        stats = json.load(fp)

    return stats