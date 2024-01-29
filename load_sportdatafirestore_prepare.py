import glob
import os
import shutil
import load_dataGCP_CloudStorage
from datetime import date
import check_for_file

# Het automatische voorbereiden van de gegevens uit beweeg app firestore

def voorbereiden_firestore_data():
# Automatisch process voorbereiden bestanden sport via staging zip en staging folders

# Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    # Constant definities die kunnen wijzigen
    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"
    rawstagingdir = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEG_HIST"
    rawstagingcsvfiles = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEG_HIST/*.csv"
    rawstagingtargetcsv = "/SPORTACTIVITEIT_BEWEEG_HIST_" + todaytekst + ".csv"


    # check en verwijder bestand SPORTACTIVITEIT_BEWEEG_HIST
    # -------------------------------------------------------
    check_for_file.check_files()

    # opvragen van de laatste SPORTACTIVITEIT.CSV in gespecifieerde dir
    list_of_csvfiles_beweeg_hist = glob.glob(rawstagingcsvfiles)
    latest_csvfile_beweeg_hist = max(list_of_csvfiles_beweeg_hist, key=os.path.getctime)

    print(latest_csvfile_beweeg_hist)

    # copy de laatste SPORTACTIVITEIT_BEWEEG_HIST.CSV naar bestand SPORTACTIVITEIT_BEWEEG_HIST.csv
    # --------------------------------------------------------------------------------------------

    command = "\\cp -n " + latest_csvfile_beweeg_hist + " " +  rawstagingdir + "/SPORTACTIVITEIT_BEWEEG_HIST.csv"
    print(command)
    os.system(command)


if __name__ == "__main__":
    voorbereiden_firestore_data()

def voorbereiden_firestore_data_sportactiviteit_beweeg():
    # Automatisch process voorbereiden bestanden sport via staging zip en staging folders

    # Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    # Constant definities die kunnen wijzigen
    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"
    rawstagingdir = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEG"
    rawstagingcsvfiles = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT_BEWEEG/*.csv"
    rawstagingtargetcsv = "/SPORTACTIVITEIT_BEWEEG_" + todaytekst + ".csv"

    # opvragen van de laatste SPORTACTIVITEIT.CSV in gespecifieerde dir
    list_of_csvfiles = glob.glob(rawstagingcsvfiles)
    latest_csvfile = max(list_of_csvfiles, key=os.path.getctime)

    print(latest_csvfile)

    # copy de laatste SPORTACTIVITEIT.CSV naar bestand SPORTACTIVITEIT.csv
    command = "cp " + latest_csvfile + " " +  rawstagingdir + "/SPORTACTIVITEIT_BEWEEG.csv"
    print(command)
    os.system(command)


if __name__ == "__main__":
    voorbereiden_firestore_data()

