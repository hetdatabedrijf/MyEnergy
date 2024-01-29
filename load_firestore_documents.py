from google.cloud import storage
from google.cloud import bigquery
import os
import csv
from datetime import date
import firebase_admin
from firebase_admin import credentials, firestore
import delete_firestore_documents

def upload_firestore_BEWEEGDETAILWEEK():

    # verwijderen van alle records uit BEWEEGDETAILWEEK VOKE
    delete_firestore_documents.delete_BEWEEGDETAILWEEK()
    print( "alle records uit BEWEEGDETAILWEEK verwijderd")

    if not firebase_admin._apps:
        cred = credentials.Certificate("/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/livecoach-b8e74-firebase-adminsdk-b1gsq-885accd829.json")
        app = firebase_admin.initialize_app(cred)

    store = firestore.client()

    # Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    # initiation SOURCEBESTAND
    sourcebasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"
    sourcebasisdirvolledig = sourcebasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEGDETAILWEEK/"
    sourcebestand = sourcebasisdirvolledig + "ACTIVITEIT_BEWEEGDETAILWEEK_" + todaytekst + ".csv"
    print(sourcebestand)
    # sourcebestand = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/01_RAW/RAWSTAGINGSPORT_BEWEEGDETAIL/ACTIVITEIT_BEWEEGDETAIL_20240107.csv"

    collection_name = "BEWEEGDETAILWEEK"

    def batch_data(iterable, n=1):
        l = len(iterable)
        for ndx in range(0, l, n):
            yield iterable[ndx:min(ndx + n, l)]

    data = []
    headers = []
    with open(sourcebestand) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                for header in row:
                    headers.append(header)
                line_count += 1
            else:
                obj = {}
                for idx, item in enumerate(row):
                    obj[headers[idx]] = item
                data.append(obj)
                line_count += 1
        print(f'Processed {line_count} lines.')

    for batch_data in batch_data(data, 499):
        batch = store.batch()
        for data_item in batch_data:
            doc_ref = store.collection(collection_name).document()
            batch.set(doc_ref, data_item)
        batch.commit()

    print('Done')


if __name__ == "__main__":
    upload_firestore_BEWEEGDETAILWEEK()

# -----------
# Source code website
# https://medium.com/@cbrannen/importing-data-into-firestore-using-python-dce2d6d3cd51

