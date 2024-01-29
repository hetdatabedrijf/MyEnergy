import pandas as pd
import os
import csv
from google.cloud import bigquery
from datetime import date
import pyexcel as pe


def upload_BEWEEGDETAILWEEK():
    # initiation json file firebase
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"] = "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/clear-region-298816-c4aac1f2fb19.json"

    # Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    # initiation SOURCEBESTAND
    sourcebasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"
    sourcebasisdirvolledig = sourcebasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEGDETAIL/"
    sourcebestand = sourcebasisdirvolledig + "ACTIVITEIT_BEWEEGDETAIL_" + todaytekst + ".csv"
    print(sourcebestand)
    # sourcebestand = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/01_RAW/RAWSTAGINGSPORT_BEWEEGDETAIL/ACTIVITEIT_BEWEEGDETAIL_20240107.csv"

    # initiation TARGETBESTAND
    targetbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"
    targetbasisdirvolledig = targetbasisdir
    targetbestanddir = targetbasisdirvolledig + "01_RAW/RAWSTAGINGSPORT_BEWEEGDETAILWEEK/"
    targetbestand = targetbestanddir + "ACTIVITEIT_BEWEEGDETAILWEEK_" + todaytekst + ".csv"
    print(targetbestand)

    detaildata = pd.read_csv(sourcebestand,
                             index_col="DAGSORT")
    df = pd.DataFrame(detaildata)
    print(detaildata)

    max_value_dag = df['DAG'].max()
    print(max_value_dag)

    result = df.groupby('WEEK')[['AANTALSTAPPEN', 'SLAAPKWALITEIT']].aggregate('sum')

    ## result = df.groupby('WEEK').agg({'AANTALSTAPPEN': ['sum', 'max', 'mean'], 'SLAAPKWALITEIT': ['sum', 'max', 'mean'], 'DAG': 'max'})

    ##result = df.groupby('WEEK').agg({'AANTALSTAPPEN': ['sum'], 'SLAAPKWALITEIT': ['sum'], 'DAG': 'max'})

    print(result)

    result.to_csv(targetbestand)

    ##sheet = pe.load(targetbestand)
    ##del sheet.row[1]  # first row starts at index 0
    ##sheet.save_as(targetbestand)



if __name__ == "__main__":
    upload_BEWEEGDETAILWEEK()

# source used for pandas
# https://realpython.com/python-csv/
