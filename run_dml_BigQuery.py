from google.cloud import bigquery
import os


os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/users/paulvanbrabant/DATA_INSTALL/MEERENERGY2022_V2/json/clear-region-298816-c4aac1f2fb19.json"

def del_all_sportactiviteiten():

# ------------------------ DELETE VAN SPORATACTIVITEITEN -------------

    project = 'clear-region-298816'
    dataset_id = 'SPORTACTIVITEITEN'
    table_id = 'SPORTACTIVITEITEN'

    client = bigquery.Client(project=project)

    sql = """
        DELETE FROM SPORTACTIVITEITEN.SPORTACTIVITEITEN where true
        """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    print(
        'TABLE SPORTACTIVITEITEN DELETED'
    )

# ------------------------ DELETE VAN SPORATACTIVITEITEN_BEWEEG  -------------

    sql = """
            DELETE FROM SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG where true
            """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    print(
        'TABLE SPORTACTIVITEITEN_BEWEEG DELETED'
    )
# if __name__ == "__main__":
#     del_all_sportactiviteiten()

# ------------------------ DELETE VAN SPORATACTIVITEITEN_BEWEEG_HIST-------------

    sql = """
            DELETE FROM SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_HIST where true
            """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    print(
        'TABLE SPORTACTIVITEITEN_BEWEEG_HIST DELETED'
    )
# if __name__ == "__main__":
#     del_all_sportactiviteiten()


# ------------------------ DELETE VAN SPORATACTIVITEITEN_TEMP -------------

def del_all_sportactiviteiten_tmp():

    project = 'clear-region-298816'

    client = bigquery.Client(project=project)

    sql = """
        DELETE FROM SPORTACTIVITEITEN.SPORTACTIVITEITEN_TMP where true
        """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    print(
        'TABLE SPORTACTIVITEITEN_TMP DELETED'
    )

    project = 'clear-region-298816'

    client = bigquery.Client(project=project)

    # ------------------------ DELETE VAN SPORATACTIVITEITEN_BEWEEG_TEMP-------------

    sql = """
            DELETE FROM SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_TMP where true
            """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    print(
        'TABLE SPORTACTIVITEITEN_BEWEEG_TMP DELETED'
    )

    # ------------------------ DELETE VAN SPORATACTIVITEITEN_BEWEEG_HIST_TEMP -------------

    project = 'clear-region-298816'

    client = bigquery.Client(project=project)

    sql = """
                DELETE FROM SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_HIST_TMP where true
                """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    print(
        'TABLE SPORTACTIVITEITEN_BEWEEG_HIST_TMP DELETED'
    )
# if __name__ == "__main__":
#     del_all_sportactiviteiten_tmp()


# ------------------------ INSERT --  VAN SPORATACTIVITEITEN -----------

def ins_all_into_sportactiviteiten():

    project = 'clear-region-298816'

    client = bigquery.Client(project=project)

    sql = """
        INSERT SPORTACTIVITEITEN.SPORTACTIVITEITEN
        SELECT * FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_TMP` 
        """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    query_job = client.query(
        """
        SELECT
          *
        FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN` 
        """
    )

    teller = 0
    results = query_job.result()  # Waits for job to complete.
    for row in results:
        teller = teller + 1

    print(
        'SPORTACTIVITEITEN GEVULD',
        'AANTAL RECORDS:',
        teller
    )

    project = 'clear-region-298816'

    client = bigquery.Client(project=project)

    sql = """
            INSERT SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG
            SELECT * FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_TMP` 
            """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    query_job = client.query(
        """
        SELECT
          *
        FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG` 
        """
    )

    teller = 0
    results = query_job.result()  # Waits for job to complete.
    for row in results:
        teller = teller + 1

    print(
        'SPORTACTIVITEITEN_BEWEEG GEVULD',
        'AANTAL RECORDS:',
        teller
    )

    project = 'clear-region-298816'

    client = bigquery.Client(project=project)

    sql = """
                INSERT SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_HIST
                SELECT * FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_HIST_TMP` 
                """
    job = client.query(sql)  # API request.
    job.result()  # Waits for the query to finish.

    query_job = client.query(
        """
        SELECT
          *
        FROM `clear-region-298816.SPORTACTIVITEITEN.SPORTACTIVITEITEN_BEWEEG_HIST` 
        """
    )

    teller = 0
    results = query_job.result()  # Waits for job to complete.
    for row in results:
        teller = teller + 1

    print(
        'SPORTACTIVITEITEN_BEWEEG_HIST GEVULD',
        'AANTAL RECORDS:',
        teller
    )
# if __name__ == "__main__":
#    ins_all_into_sportactiviteiten()

