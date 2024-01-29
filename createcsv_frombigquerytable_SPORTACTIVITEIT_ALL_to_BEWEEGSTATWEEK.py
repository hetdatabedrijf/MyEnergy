import os
import csv
from google.cloud import bigquery
from datetime import date

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/clear-region-298816-c4aac1f2fb19.json"

# Programma dat de gegevens vanuit bigquery SPORTACTIVITEIT_ALL haal om in firestore collection BEWEEG_STAT_WEEK te laden
#           dat de gegevens wegschrijft naar een CSV bestand op disk
#           dat de opgehaalde gegevens in een print rij wegschrijft
#           dat de gegevens wegschrijft in firestore

def query_bigquery():

    # Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"

    targetbestanddir = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEGSTAT/"
    targetbestand = targetbestanddir + "SPORTACTIVITEIT_ALL_BEWEEGSTATWEEK" + todaytekst + ".csv"
    print(targetbestand)

    client = bigquery.Client()
    query_job = client.query(
        """
        SELECT WEEK, WEEKMAAND, SPORTTYPE, sum(afstand) as AFSTAND, sum(tijd) as TIJD, sum(hm) as HM, sum(wandel_afstand) as WANDEL_AFSTAND, sum(loop_afstand) as LOOP_AFSTAND, sum(fiets_afstand) as FIETS_AFSTAND, IF(sporttype  LIKE "%WANDEL_BEWEEG%", sum(hm), 0) AS STAPPEN
            FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_ALL`
            where WEEKMAAND like "%22%"
            group by WEEK, WEEKMAAND, SPORTTYPE
            order by WEEK, SPORTTYPE
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

        fnames = ['WEEK', 'WEEKMAAND', 'SPORTTYPE', 'AFSTAND', 'TIJD', 'HM', 'WANDEL_AFSTAND', 'LOOP_AFSTAND', 'FIETS_AFSTAND', 'STAPPEN']

        writer = csv.DictWriter(f, fieldnames=fnames)
        writer.writeheader()

        # Specificatie  van de loop met rijen vanuit het queryresultaat
        #               printen van de query lijn
        #               wegschrijven van de query lijn


        for row in results:
            teller = teller + 1

            print(f" {row.WEEK} - ",
                  f" {row.WEEKMAAND} - ",
                  f" {row.SPORTTYPE} - ",
                  f" {row.AFSTAND} - ",
                  f" {row.TIJD} - ",
                  f" {row.HM} - ",
                  f" {row.WANDEL_AFSTAND} - ",
                  f" {row.LOOP_AFSTAND} - ",
                  f" {row.FIETS_AFSTAND} - ",
                  f" {row.STAPPEN} - ")

            writer.writerow({'WEEK': row.WEEK, 'WEEKMAAND': row.WEEKMAAND, 'SPORTTYPE': row.SPORTTYPE, 'AFSTAND': row.AFSTAND, 'TIJD': row.TIJD, 'HM': row.HM, 'WANDEL_AFSTAND': row.WANDEL_AFSTAND, 'LOOP_AFSTAND': row.LOOP_AFSTAND, 'FIETS_AFSTAND': row.FIETS_AFSTAND, 'STAPPEN': row.STAPPEN})

        print("aantal rijen in CSV file" + str(teller))

    # copy de laatste SPORTACTIVITEIT_ALL.CSV naar bestand SPORTACTIVITEIT_ALL.csv
    command = "cp " + targetbestand + " " + targetbestanddir + "SPORTACTIVITEIT_ALL_BEWEEGSTATWEEK.csv"
    print(command)
    os.system(command)




if __name__ == "__main__":
    query_bigquery()
