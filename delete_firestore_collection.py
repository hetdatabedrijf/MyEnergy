from google.cloud import storage
from google.cloud import bigquery
import os
import csv
import firebase_admin
from firebase_admin import credentials, firestore


def delete_collection():

    cred = credentials.Certificate("/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/livecoach-b8e74-firebase-adminsdk-b1gsq-885accd829.json")
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    print(db)

    doc_ref = db.collection(u'BEWEEG')
    print(doc_ref)

    docs = doc_ref.limit(1000).stream()
    #docs = doc_ref.order_by(u'DAG').stream()

    deleted = 0

    for doc in docs:
        var_dag = u'{}'.format(doc.to_dict()['DAG'])
        # print(var_dag)

        search_2024 = var_dag.find("2024")

        # print(f'Deleting doc {doc.id} => {doc.to_dict()}')
        # doc.reference.delete()

        if search_2024 < 1:
            deleted = deleted + 1
            printresult = str(deleted) + " " + str(search_2024)
            print(printresult)
            print(f'Deleting doc {doc.id} => {doc.to_dict()}')
            doc.reference.delete()

    if deleted >= 1000:
        return delete_collection()

    print(deleted)

if __name__ == "__main__":
    delete_collection()
