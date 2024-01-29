from google.cloud import storage
from google.cloud import bigquery
import os
import csv
import firebase_admin
from firebase_admin import credentials, firestore

if not firebase_admin._apps:
    cred = credentials.Certificate(
    "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/livecoach-b8e74-firebase-adminsdk-b1gsq-885accd829.json")
    firebase_admin.initialize_app(cred)

def delete_BEWEEGDETAILWEEK():

    db = firestore.client()
    print(db)

    doc_ref = db.collection(u'BEWEEGDETAILWEEK')
    print(doc_ref)

    docs = doc_ref.limit(1000).stream()
    #docs = doc_ref.order_by(u'DAG').stream()

    deleted = 0

    for doc in docs:
        deleted = deleted + 1
        print(f'Deleting doc {doc.id} => {doc.to_dict()}')
        doc.reference.delete()

    if deleted >= 1000:
        return delete_collection()

    print("aantal records deleted: " + str(deleted))

if __name__ == "__main__":
    delete_BEWEEGDETAILWEEK()
