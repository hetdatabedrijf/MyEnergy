from google.cloud import storage
import os

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/clear-region-298816-c4aac1f2fb19.json"


# Automatisch process upladen bestanden sport naar GPC Cloudstation

def upload_blob():
    """Uploads a file to the bucket."""

    bucket_name = "clear-region-298816.appspot.com"

    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"

    source_file_name_sport = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT/SPORTACTIVITEIT.csv"
    source_file_name_sport_beweeg_hist = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEG_HIST/SPORTACTIVITEIT_BEWEEG_HIST.csv"

    destination_blob_name_sport = "RAWSTAGING/SPORTACTIVITEIT.csv"
    destination_blob_name_sport_beweeg_hist = "RAWSTAGING/SPORTACTIVITEIT_BEWEEG_HIST.csv"

    source_file_name_dag = rawstagingbasisdir + "01_RAW/RAWSTAGINGDAG/DAGACTIVITEIT.csv"
    destination_blob_name_dag = "RAWSTAGING/DAGACTIVITEIT.csv"

    source_file_name_project = rawstagingbasisdir + "01_RAW/RAWSTAGINGPROJECT/PROJECTACTIVITEIT.csv"
    destination_blob_name_project = "RAWSTAGING/PROJECTACTIVITEIT.csv"

    source_file_name_slaap = rawstagingbasisdir + "01_RAW/RAWSTAGINGSLAAP/SLAAPACTIVITEIT.csv"
    destination_blob_name_slaap = "RAWSTAGING/SLAAPACTIVITEIT.csv"


    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name_sport)
    blob.upload_from_filename(source_file_name_sport)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_sport, destination_blob_name_sport
        )
    )

    blob = bucket.blob(destination_blob_name_sport_beweeg_hist)
    blob.upload_from_filename(source_file_name_sport_beweeg_hist)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_sport, destination_blob_name_sport_beweeg_hist
        )
    )


    blob = bucket.blob(destination_blob_name_dag)
    blob.upload_from_filename(source_file_name_dag)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_dag, destination_blob_name_dag
        )
    )

    blob = bucket.blob(destination_blob_name_project)
    blob.upload_from_filename(source_file_name_project)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_project, destination_blob_name_project
        )
    )


    blob = bucket.blob(destination_blob_name_slaap)
    blob.upload_from_filename(source_file_name_slaap)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_slaap, destination_blob_name_slaap
        )
    )

#if __name__ == "__main__":
#    upload_blob()


def upload_blob_SPORTACTIVITEITEN_ALL():
    """Uploads a file to the bucket."""

    bucket_name = "clear-region-298816.appspot.com"


    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"

    source_file_name_sport_beweeg = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEGSTAT/SPORTACTIVITEIT_ALL.csv"
    destination_blob_name_sport_beweeg = "RAWSTAGING/SPORTACTIVITEIT_ALL.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name_sport_beweeg)
    blob.upload_from_filename(source_file_name_sport_beweeg)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_sport_beweeg, destination_blob_name_sport_beweeg
        )
    )

def upload_blob_SPORTACTIVITEIT_BEWEEG():
    """Uploads a file to the bucket."""

    bucket_name = "clear-region-298816.appspot.com"


    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"

    source_file_name_sport = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEG/SPORTACTIVITEIT_BEWEEG.csv"
    destination_blob_name_sport = "RAWSTAGING/SPORTACTIVITEIT_BEWEEG.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name_sport)
    blob.upload_from_filename(source_file_name_sport)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_sport, destination_blob_name_sport
        )
    )


def upload_blob_SPORTACTIVITEITEN_ALL_BEWEEGSTATWEEK():
    """Uploads a file to the bucket."""

    bucket_name = "clear-region-298816.appspot.com"


    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"

    source_file_name_sport = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEGSTAT/SPORTACTIVITEIT_ALL_BEWEEGSTATWEEK.csv"
    destination_blob_name_sport = "RAWSTAGING/SPORTACTIVITEIT_ALL_BEWEEGSTATWEEK.csv"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name_sport)
    blob.upload_from_filename(source_file_name_sport)

    print(
        "File {} uploaded to {}.".format(
            source_file_name_sport, destination_blob_name_sport
        )
    )

