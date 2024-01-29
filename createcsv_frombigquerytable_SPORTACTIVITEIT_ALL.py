import os
import csv
from google.cloud import bigquery
from datetime import date

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/clear-region-298816-c4aac1f2fb19.json"

# Programma dat de gegevens van een bigquery ophaalt uit een google GPC instantie
#           dat de gegevens wegschrijft naar een CSV bestand op disk
#           dat de opgehaalde gegevens in een print rij wegschrijft

def query_bigquery():

    # Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"

    targetbestanddir = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEGSTAT/"
    targetbestand = targetbestanddir + "SPORTACTIVITEIT_ALL_" + todaytekst + ".csv"
    print(targetbestand)

    client = bigquery.Client()
    query_job = client.query(
        """
        SELECT
        *
        FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_ALL` 
        ORDER BY SPORTTYPE, WEEK ASC, DATUM ASC
        """
    )

    f = open(targetbestand, 'w')

    with f:

        # Specificatie  van de header
        #               van de query result variabele
        #               wegschijven van de header

        teller = 0

        results = query_job.result()  # Waits for job to complete.
        # fnames = ['row', 'sporttype', 'week', 'datum', 'afstand', 'afstand_YTD', 'fietstarget_YTD']
        # fnames = ['row', 'sporttype', 'week', 'datum', 'afstand', 'afstand_YTD', 'fietstarget_YTD']

        fnames = ['DAGID', 'WEEKDAG', 'SPORTTYPE', 'AFSTAND', 'TIJD', 'HM', 'SPORTACTIVITEITEN', 'WEEKMAAND', 'WEEKMAANDSORT', 'FIETS_AFSTAND', 'LOOP_AFSTAND', 'WANDEL_AFSTAND', 'WEEK', 'DATUM', 'WEEKID']

        writer = csv.DictWriter(f, fieldnames=fnames)
        writer.writeheader()

        # Specificatie  van de loop met rijen vanuit het queryresultaat
        #               printen van de query lijn
        #               wegschrijven van de query lijn


        for row in results:
            teller = teller + 1

            print(f" {row.DAGID} - ",
                  f" {row.WEEKDAG} - ",
                  f" {row.SPORTTYPE} - ",
                  f" {row.AFSTAND} - ",
                  f" {row.TIJD} - ",
                  f" {row.HM} - ",
                  f" {row.SPORTACTIVITEITEN} - ",
                  f" {row.WEEKMAAND} - ",
                  f" {row.WEEKMAANDSORT} - ",
                  f" {row.FIETS_AFSTAND} - ",
                  f" {row.LOOP_AFSTAND} - ",
                  f" {row.WANDEL_AFSTAND} - ",
                  f" {row.WEEK} - ",
                  f" {row.DATUM} - ",
                  f" {row.WEEKID} - ")

            writer.writerow({'DAGID': row.DAGID, 'WEEKDAG': row.WEEKDAG, 'SPORTTYPE': row.SPORTTYPE, 'AFSTAND': row.AFSTAND, 'TIJD': row.TIJD, 'HM': row.HM, 'SPORTACTIVITEITEN': row.SPORTACTIVITEITEN, 'WEEKMAAND': str(row.WEEKMAAND), 'WEEKMAANDSORT': row.WEEKMAANDSORT, 'FIETS_AFSTAND': row.FIETS_AFSTAND, 'LOOP_AFSTAND': row.LOOP_AFSTAND, 'WANDEL_AFSTAND': row.WANDEL_AFSTAND, 'WEEK': row.WEEK, 'DATUM': row.DATUM, 'WEEKID': row.WEEKID})

        print("aantal rijen in CSV file" + str(teller))

    # copy de laatste SPORTACTIVITEIT_ALL.CSV naar bestand SPORTACTIVITEIT_ALL.csv
    command = "cp " + targetbestand + " " + targetbestanddir + "SPORTACTIVITEIT_ALL.csv"
    print(command)
    os.system(command)




if __name__ == "__main__":
    query_bigquery()
