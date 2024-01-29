import os
import json
import glob
import shutil
import csv
import re
import unittest

from datetime import date

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime

# initializations Firestore
cred = credentials.Certificate(
        "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/livecoach-b8e74-firebase-adminsdk-b1gsq-885accd829.json")
default_app = firebase_admin.initialize_app(cred)

def upload_beweegdetail():
    """Uploads a file to the bucket."""

    # Initialisation of the program variables

    # Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    targetbestanddir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/01_RAW/RAWSTAGINGSPORT_BEWEEGDETAIL/"
    targetbestand = targetbestanddir + "ACTIVITEIT_BEWEEGDETAIL_" + todaytekst + ".csv"

    print(targetbestand)

    varaantalstappen = 0
    varslaapkwaliteit = 0
    varslaapdiep = 0

    # Openen van het bestand

    db = firestore.client()

    # Reading the data

    doc_ref = db.collection(u'BEWEEGDETAIL')

    # order the data
    docs = doc_ref.order_by(u'DAGSORT').stream()
    # docs = doc_ref.where(u'ANDERE1', u'!=', u'test').order_by(u'ANDERE1').stream()

    f = open(targetbestand, 'w')

    with f:
        # Specificatie  van de header
        #               van de query result variabele
        #               wegschijven van de header

        # fnames = ['DAGID', 'WEEKDAG', 'SPORTTYPE', 'AFSTAND', 'TIJD', 'HM','SPORTACTIVITEITEN', 'WEEKMAAND', 'WEEKMAANDSORT', 'FIETS_AFSTAND', 'LOOP_AFSTAND', 'WANDEL_AFSTAND', 'WEEK', 'DATUM', 'WEEKID']
        fnames = ['SOURCE', 'DAG', 'DAGSORT', 'WEEK', 'AANTALSTAPPEN', 'SLAAPKWALITEIT', 'SLAAPDIEP', 'DAGCOMPLETED']

        writer = csv.DictWriter(f, fieldnames=fnames)
        writer.writeheader()

        # Specificatie  van de loop met rijen vanuit het queryresultaat
        #               printen van de query lijn
        #               wegschrijven van de query lijn

        for doc in docs:
            # print('{} => {} '.format(doc.id, doc.to_dict()))

            vardag = u'{}'.format(doc.to_dict()['DAG'])
            vardagsort = u'{}'.format(doc.to_dict()['DAGSORT'])
            varweek = u'{}'.format(doc.to_dict()['WEEK'])
            varaantalstappen = u'{}'.format(doc.to_dict()['STAPPENAANTAL'])
            varslaapkwaliteit = u'{}'.format(doc.to_dict()['SLAAPKWALITEIT'])
            varslaapdiep = u'{}'.format(doc.to_dict()['SLAAPDIEP'])
            vardagcompleted = u'{}'.format(doc.to_dict()['DAGCOMPLETED'])

            var_dag_date = datetime.strptime(vardag, '%d%m%Y')
            datum = var_dag_date.strftime("%B %d, %Y")
            # print(datum)
            weekpython = (var_dag_date.strftime("%y%W"))
            weekpython_week = (str(weekpython)[2:4])
            weekid = 'W' + weekpython_week
            # print (weekid)
            week = weekid + var_dag_date.strftime("%y")
            # print(week)
            weekmaand_maand = (var_dag_date.strftime("%b"))
            weekmaand_maand_str = str(weekmaand_maand)
            weekmaand_maand_str_upper = weekmaand_maand_str.upper()
            weekmaand_jaar = (var_dag_date.strftime("%y"))
            weekmaand = weekmaand_maand_str_upper + weekmaand_jaar
            # print(weekmaand)
            weekmaandsort = (var_dag_date.strftime("%y%m"))
            # print(weekmaandsort)  # 2022-09-23    -> 2209
            weekdag = (var_dag_date.strftime("%A"))
            # print(weekdag)  # 2022-09-23    -> Friday

            # print(var_andere1_check)
            # print(var_andere1_check.find('test'))

            print(
                "FS_WEEKDETAIL" + "," +
                vardag + "," +
                vardagsort + "," +
                varweek + "," +
                varaantalstappen + "," +
                varslaapkwaliteit + "," +
                varslaapdiep + "," +
                vardagcompleted + ",")

            print(
                "calculated fields " + "," +
                vardag + "," +
                vardagsort + "," +
                varweek + "," +
                weekdag + "," +
                weekmaand + "," +
                weekmaandsort + "," +
                week + "," +
                datum + "," +
                weekid + ",")

            writer.writerow({'SOURCE': "FS_WEEKDETAIL",
                             'DAG': vardag,
                             'DAGSORT': vardagsort,
                             'WEEK': varweek,
                             'AANTALSTAPPEN': varaantalstappen,
                             'SLAAPKWALITEIT': varslaapkwaliteit,
                             'SLAAPDIEP': varslaapdiep,
                             'DAGCOMPLETED': vardagcompleted
                             })

    print("csv bestand vanuit firestore met basistransacties klaar .... Ended ")
    print("----------------------------------------------------------")


if __name__ == "__main__":
    upload_beweegdetail()

# --------------------------------Documentation--------
# Initialiseren van firebase firebase_admin
#  pip3 install firebase-admin
