from google.cloud import storage
from google.cloud import bigquery
import os

os.environ[
     "GOOGLE_APPLICATION_CREDENTIALS"] = "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/clear-region-298816-c4aac1f2fb19.json"

# Automatisch process upladen bestanden sport van GCP Cloudstation naar GCP Bigquery

def upload_bigquerytable():
    """Uploads a file to the bigquery dataset."""

    table_id = "clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_TMP"

    # Construct a BigQuery client object.
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    uri = "gs://clear-region-298816.appspot.com/RAWSTAGING/SPORTACTIVITEIT.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

    # -----------------------------------------------
    # opladen van de beweeg files in beweeg_tmp tabel

    table_id_beweeg = "clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_TMP"

    # Construct a BigQuery client object.
    client_beweeg = bigquery.Client()

    job_config_beweeg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    uri_beweeg = "gs://clear-region-298816.appspot.com/RAWSTAGING/SPORTACTIVITEIT_BEWEEG.csv"

    load_job_beweeg = client_beweeg.load_table_from_uri(
        uri_beweeg, table_id_beweeg, job_config=job_config_beweeg
    )  # Make an API request.

    load_job_beweeg.result()  # Waits for the job to complete.

    destination_table_beweeg = client_beweeg.get_table(table_id_beweeg)  # Make an API request.
    print("Loaded {} rows.".format(destination_table_beweeg.num_rows))

    # -----------------------------------------------
    # opladen van de beweeg hist files in beweeg_tmp tabel (alle transacties die vanuit firestore
    # zijn gehistoriceerd

    table_id_beweeg = "clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_HIST_TMP"

    # Construct a BigQuery client object.
    client_beweeg = bigquery.Client()

    job_config_beweeg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    uri_beweeg = "gs://clear-region-298816.appspot.com/RAWSTAGING/SPORTACTIVITEIT_BEWEEG_HIST.csv"

    load_job_beweeg = client_beweeg.load_table_from_uri(
        uri_beweeg, table_id_beweeg, job_config=job_config_beweeg
    )  # Make an API request.

    load_job_beweeg.result()  # Waits for the job to complete.

    destination_table_beweeg = client_beweeg.get_table(table_id_beweeg)  # Make an API request.
    print("Loaded {} rows.".format(destination_table_beweeg.num_rows))


if __name__ == "__main__":
    upload_bigquerytable()
