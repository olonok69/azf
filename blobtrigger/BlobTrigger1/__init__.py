import logging
import io
import azure.functions as func
import pandas as pd
from bs4 import BeautifulSoup


def main(myblob: func.InputStream, context:func.Context):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    content = io.BytesIO(myblob.read())
    soup = BeautifulSoup(content, 'xml')

    names = soup.find_all('name')
    for name in names:
        print(name.text)

    # data = pd.read_csv(content)

    # print(data.head())

    file_path = f'{context.function_directory}/data/test1.csv'
    file = pd.read_csv(file_path)
    filename = file[:2].to_json()
    print(filename)
