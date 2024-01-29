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

    targetbestanddir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/03_REPORTS/"

    targetbestand = targetbestanddir + "pvbaantalfietsperweek_" + todaytekst + ".csv"
    print(targetbestand)

    client = bigquery.Client()
    query_job = client.query(
        """
        SELECT
        *
        FROM `clear-region-298816.SPORTACTIVITEITEN.SACALC_ACTIVITEIT` 
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
        fnames = ['row', 'sporttype', 'week', 'datum', 'afstand', 'afstand_YTD', 'fietstarget_YTD']

        writer = csv.DictWriter(f, fieldnames=fnames)
        writer.writeheader()

        # Specificatie  van de loop met rijen vanuit het queryresultaat
        #               printen van de query lijn
        #               wegschrijven van de query lijn


        for row in results:
            teller = teller + 1

            afstand_ytd_afgerond = round(row.AFSTAND_YTD, 2)

            print(teller,
                  f" {row.SPORTTYPE} - ",
                  f" {row.WEEK} - ",
                  f" {row.DATUM} - ",
                  f" {row.AFSTAND}  ",
                  f" {afstand_ytd_afgerond}  ",
                  f" {row.FIETSTARGET_YTD} ")

            writer.writerow({'row': teller, 'sporttype': row.SPORTTYPE, 'week': row.WEEK, 'datum': row.DATUM, 'afstand': row.AFSTAND, 'afstand_YTD': afstand_ytd_afgerond, 'fietstarget_YTD': row.FIETSTARGET_YTD})


if __name__ == "__main__":
    query_bigquery()

# query_job = client.query(
#         """
#         SELECT
#         *
#         FROM `clear-region-298816.SPORTACTIVITEITEN.SACALC_ACTIVITEIT`
#         UNION ALL
#         SELECT
#          *
#         FROM `clear-region-298816.SPORTACTIVITEITEN.SACALC_ACTIVITEIT_BEWEEG`
#         ORDER BY SPORTTYPE, WEEK ASC, DATUM ASC
#         """
#     )