import logging
import io
import azure.functions as func
import pandas as pd
from bs4 import BeautifulSoup

def main(req: func.HttpRequest, context:func.Context) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
    for input_file in req.files.values():
        content = input_file.stream.read()
        conten_pd = io.BytesIO(content)
        soup = BeautifulSoup(conten_pd, 'xml')

        names = soup.find_all('name')
        for name in names:
            print(name.text)
        #test = pd.read_csv(conten_pd)
        #print(test.head())
        #filename = req.get_body().decode()
        file_path = f'{context.function_directory}/data/test1.csv'
        file = pd.read_csv(file_path)
        filename = file[:2].to_json()
        

    if filename:
        return func.HttpResponse(f"Hello, {filename}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
