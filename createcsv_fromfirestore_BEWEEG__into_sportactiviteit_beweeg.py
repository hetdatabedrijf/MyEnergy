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


def upload_firestoredata():
    """Uploads a file to the bucket."""

# Initialisation of the program variables

# Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    targetbestanddir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/01_RAW/RAWSTAGINGSPORT_BEWEEG/"
    targetbestand = targetbestanddir + "SPORTACTIVITEIT_BEWEEG_" + todaytekst + ".csv"

    print(targetbestand)

    var_afstand_fiets_str = ""
    var_afstand_loop_str  = ""
    var_afstand_wandel_str = ""
    var_afstand_wandel = 0
    var_afstand_fiets = 0
    var_afstand_loop = 0

# Openen van het bestand

# initializations Firestore
    if not firebase_admin._apps:
        cred = credentials.Certificate("/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/livecoach-b8e74-firebase-adminsdk-b1gsq-885accd829.json")
        default_app = firebase_admin.initialize_app(cred)

    db = firestore.client()

# Reading the data

    doc_ref = db.collection(u'BEWEEG')

    # order the data
    docs = doc_ref.order_by(u'DAG').stream()
    # docs = doc_ref.where(u'ANDERE1', u'!=', u'test').order_by(u'ANDERE1').stream()

    f = open(targetbestand, 'w')

    with f:

        # Specificatie  van de header
        #               van de query result variabele
        #               wegschijven van de header

        fnames = ['DAGID', 'WEEKDAG', 'SPORTTYPE', 'AFSTAND', 'TIJD', 'HM','SPORTACTIVITEITEN', 'WEEKMAAND', 'WEEKMAANDSORT', 'FIETS_AFSTAND', 'LOOP_AFSTAND', 'WANDEL_AFSTAND', 'WEEK', 'DATUM', 'WEEKID']

        writer = csv.DictWriter(f, fieldnames=fnames)
        writer.writeheader()

        # Specificatie  van de loop met rijen vanuit het queryresultaat
        #               printen van de query lijn
        #               wegschrijven van de query lijn

        for doc in docs:
            # print('{} => {} '.format(doc.id, doc.to_dict()))

            var_dag = u'{}'.format(doc.to_dict()['DAG'])
            var_sporttype = u'{}'.format(doc.to_dict()['SPORTTYPE'])
            var_description = u'{}'.format(doc.to_dict()['DESCRIPTION'])
            var_afstand_str = u'{}'.format(doc.to_dict()['AFSTAND'])
            var_tijd_str = u'{}'.format(doc.to_dict()['TIJD'])
            var_andere1_str = u'{}'.format(doc.to_dict()['ANDERE1'])
            var_andere1_check = u'{}'.format(doc.to_dict()['ANDERE1'])

        # var_dag_date = datetime.now()
        # print(var_dag_date)

            var_dag_date = datetime.strptime(var_dag, '%d%m%Y')
        # print(var_dag_date)

        # weekmaandsortjaar = var_dag_date.year
        # weekmaandsortmaand = var_dag_date.month
        # print(weekmaandsortmaand)   #2022-09-23    -> 9
        # print(weekmaandsortjaar)    #2022-09-23    -> 2022

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
            weekmaand_maand_str_upper =  weekmaand_maand_str.upper()
            weekmaand_jaar = (var_dag_date.strftime("%y"))
            weekmaand = weekmaand_maand_str_upper + weekmaand_jaar
        # print(weekmaand)

            weekmaandsort = (var_dag_date.strftime("%y%m"))
        # print(weekmaandsort)  # 2022-09-23    -> 2209

            weekdag = (var_dag_date.strftime("%A"))
        # print(weekdag)  # 2022-09-23    -> Friday

            # print(var_andere1_check)
            # print(var_andere1_check.find('test'))

            if var_andere1_check.find('test') == -1:

                var_afstand_str=(var_afstand_str.replace("..", "."))
                var_tijd_str = (var_tijd_str.replace("..", "."))
                var_andere1_str = (var_andere1_str.replace("..", "."))
                var_tijd = int(var_tijd_str)
                var_andere1 = int(var_andere1_str)
                var_afstand = float(var_afstand_str)

                print(var_sporttype.find('WANDEL'))
                if var_sporttype.find('WANDEL') >= 0:
                    var_afstand_wandel_str = var_afstand_str
                    var_afstand_wandel = float(var_afstand)
                    var_afstand_loop = 0
                    var_afstand_fiets = 0
                    var_afstand_loop_str = "0"
                    var_afstand_fiets_str = "0"

                if var_sporttype.find('FIETS') >= 0:
                    var_afstand_fiets_str = var_afstand_str
                    var_afstand_fiets = float(var_afstand)
                    var_afstand_loop = 0
                    var_afstand_wandel = 0
                    var_afstand_loop_str = "0"
                    var_afstand_wandel_str = "0"

                if var_sporttype.find('LOOP') >= 0:
                    var_afstand_loop_str = var_afstand_str
                    var_afstand_loop = float(var_afstand)
                    var_afstand_fiets_str = "0"
                    var_afstand_fiets = 0
                    var_afstand_wandel_str = "0"
                    var_afstand_wandel = 0

                print(
                    "firestorebestand" + "," +
                    weekdag + "," +
                    var_sporttype + "," +
                    var_afstand_str + "," +
                    var_tijd_str + "," +
                    var_andere1_str + "," +
                    var_description + "," +
                    weekmaand + "," +
                    weekmaandsort + "," +
                    var_afstand_fiets_str + "," +
                    var_afstand_loop_str + "," +
                    var_afstand_wandel_str + "," +
                    week + "," +
                    datum + "," +
                    weekid + ",")

                writer.writerow({'DAGID': "firestorebestand",
                             'WEEKDAG': weekdag,
                             'SPORTTYPE': var_sporttype,
                             'AFSTAND': var_afstand,
                             'TIJD': var_tijd,
                             'HM': var_andere1,
                             'SPORTACTIVITEITEN': var_description,
                             'WEEKMAAND': weekmaand,
                             'WEEKMAANDSORT': weekmaandsort,
                             'FIETS_AFSTAND': float(var_afstand_fiets),
                             'LOOP_AFSTAND': float(var_afstand_loop),
                             'WANDEL_AFSTAND': float(var_afstand_wandel),
                             'WEEK': week,
                             'DATUM': datum,
                             'WEEKID': weekid})

    print("cv vanuit firestore met basistransacties klaar .... Ended ")
    print("----------------------------------------------------------")



if __name__ == "__main__":
    upload_firestoredata()



# --------------------------------Documentation--------
# Initialiseren van firebase firebase_admin
#  pip3 install firebase-admin