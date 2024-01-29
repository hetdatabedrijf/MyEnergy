# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

# import load_sportdata_prepare
from load_sportdata_prepare import *

import load_sportdatafirestore_prepare

# import load_dagdata_prepare
# import load_projectdata_prepare
# import load_slaapdata_prepare

import createcsv_frombigquerytable_SACALC_ACTIVITEIT
import createcsv_fromfirestore_BEWEEG__into_sportactiviteit_beweeg
import createcsv_frombigquerytable_SPORTACTIVITEIT_ALL
import createcsv_frombigquerytable_SPORTACTIVITEIT_ALL_to_BEWEEGSTATWEEK
import delete_firestore_collection
import load_dataGCP_firestore


import run_dml_BigQuery
import load_dataGCP_BigQuery
import load_dataGCP_CloudStorage

# toegevoegd op 7/01/24
import createcsv_fromfirestore_BEWEEGDETAIL
# toegevoegd op 7/01/24
import createcsv_BEWEEGDETAILWEEK
# toegevoegd op 7/01/24
import load_firestore_documents

# 25/01/2024 gevalideerd en basis voor ontwikkelingen 2024
# Downloaden van de gegevens sportactiviteit uit firestore BEWEEG
print("Procedure voor het downloaden SPORTACTIVITEITEN VANUIT Firestore naar SPORTACTIVITEIT_BEWEEG gestart ..... ")
createcsv_fromfirestore_BEWEEG__into_sportactiviteit_beweeg.upload_firestoredata()

print("Procedure voor het voorbereiden van SPORTACTIVITEITEN VANUIT NOTION naar CLOUD STORAGE gestart ..... ")
# load_sportdata_prepare.voorbereiden_files()
voorbereiden_files()

print("Procedure voor het voorbereiden van SPORTACTIVITEITEN VANUIT FIRESTORE naar CLOUD STORAGE gestart ..... ")
load_sportdatafirestore_prepare.voorbereiden_firestore_data()

# print("Procedure voor het voorbereiden van DAGACTIVITEITEN naar CLOUD STORAGE gestart ..... ")
# load_dagdata_prepare.voorbereiden_files()

# print("Procedure voor het voorbereiden van PROJECTACTIVITEITEN naar CLOUD STORAGE gestart ..... ")
# load_projectdata_prepare.voorbereiden_files()

# print("Procedure voor het voorbereiden van SLAAPACTIVITEITEN naar CLOUD STORAGE gestart ..... ")
# load_slaapdata_prepare.voorbereiden_files()

print("Procedure voor het opladen van ACTIVITEITEN in CLOUD STORAGE gestart ..... ")
load_dataGCP_CloudStorage.upload_blob()

print("Procedure voor het opladen van SPORTACTIVITEITEN in BIGQUERY gestart ..... ")

print(' ...... leegmaken van SPORTACTIVITEITEN_TMP in bigquery gestart ..... ')
run_dml_BigQuery.del_all_sportactiviteiten_tmp()

print(' ...... opladen van SPORTACTIVITEITEN_TMP in bigquery gestart ..... ')
load_dataGCP_BigQuery.upload_bigquerytable()

print(' ...... leegmaken van SPORTACTIVITEITEN in bigquery gestart ..... ')
run_dml_BigQuery.del_all_sportactiviteiten()

print(' ...... opvullen van SPORTACTIVITEITEN vanuit TMP in bigquery gestart ..... ')
run_dml_BigQuery.ins_all_into_sportactiviteiten()

print(' ...... Voorbereiden bestand vanuit bigquery met gegevens NOTION en FIRESTORE naar SPORTACTIVITEITEN_ALL  .... ')
createcsv_frombigquerytable_SPORTACTIVITEIT_ALL.query_bigquery()

print("Procedure voor het opladen van SPORTACTIVITEITEN_ALL in CLOUD STORAGE gestart ..... ")
load_dataGCP_CloudStorage.upload_blob_SPORTACTIVITEITEN_ALL()

print(' ... Voorbereiden file uit bigquery met gegevens NOTION FIRESTORE naar SPORTACTIVITEITEN_ALL_BEWEEGSTATWEEK . ')
createcsv_frombigquerytable_SPORTACTIVITEIT_ALL_to_BEWEEGSTATWEEK.query_bigquery()

print("Procedure voor het opladen van SPORTACTIVITEITEN_ALL BEWEEG STATISTIEK FIRESTORE in CLOUD STORAGE gestart ... ")
load_dataGCP_CloudStorage.upload_blob_SPORTACTIVITEITEN_ALL_BEWEEGSTATWEEK()

# print("Procedure voor het verwijderen van de collectie BEWEEGSTATWEEK in Firestore gestart ..... ")
# delete_firestore_collection.delete_collection()

# print("Procedure voor het OPLADEN van de collectie BEWEEGSTATWEEK in Firestore gestart .....met locale gegevens ")
# load_dataGCP_firestore.upload_firestore_BEWEEGSTATWEEK()

# Aanpassingen toegevoegd 07/01/23 - opladen gegevens firebase BEWEEGDETAIL
# het ophalen van de laatste versie van BEWEEGDETAIL uit firestore
createcsv_fromfirestore_BEWEEGDETAIL.upload_beweegdetail()
print("gegevens uit beweegdetail firestore opgeladen")

# Aanpassingen toegevoegd 07/01/23 - berekenen gegevens BEWEEGDETAIL PER WEEK
# wegschrijven resultaat in een bestand csv
createcsv_BEWEEGDETAILWEEK.upload_BEWEEGDETAILWEEK()
print("gegevens uit beweegdetail opgeslagen in csv")

# toegevoegd op 7/01/24
# leegmaken van firestore documenten in BEWEEGDETAILWEEK en
# opullen van BEWEEGDETAILWEEK vanuit csv bestand
load_firestore_documents.upload_firestore_BEWEEGDETAILWEEK()
print("gegevens opgeladen in BEWEEGDETAILWEEK")

createcsv_frombigquerytable_SACALC_ACTIVITEIT.query_bigquery()
print(' ...... printen van rapport en file vanuit  SPORTACTIVITEITEN via SACALC_ACTIVITEIT ..... ')
