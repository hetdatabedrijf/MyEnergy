import glob
import os
import shutil
import load_dataGCP_CloudStorage
from datetime import date


def voorbereiden_files():
    # Automatisch process voorbereiden bestanden sport via staging zip en staging folders

    # Instantieren van variabelen
    today = date.today()
    todaytekst = str(today)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)

    # Constant definities die kunnen wijzigen
    rawstagingbasisdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/"
    rawstagingzipdir =  rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORTZIP"

    rawstagingzipfiles = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORTZIP/*.zip"
    rawstagingdir = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT"
    rawstagingcsvfiles = rawstagingbasisdir + "01_RAW/RAWSTAGINGSPORT/*.csv"
    rawstagingtargetcsv = "/SPORTACTIVITEIT_" + todaytekst + ".csv"

    print("------------------- Start Rawstaging informatie ---------------")
    print(rawstagingzipdir)
    print(rawstagingzipfiles)
    print(rawstagingtargetcsv)
    print("------------------- Einde Rawstaging informatie ---------------")

    # opvragen van de laatste zipfile in gespecifieerde dir
    command = "cd " + rawstagingzipdir
    os.system(command)

    print("------------------- Overzicht List of Zipfiles ---------------")
    command = "ls " + rawstagingzipfiles
    print(command)
    os.system(command)
    print("------------------- End List of Zipfiles ---------------")

    list_of_files = glob.glob(rawstagingzipfiles)
    latest_zipfile = max(list_of_files, key=os.path.getctime)

    print("------------------- Meest recente Zipfiles ---------------")
    print(latest_zipfile)
    print("------------------- End Meest recente Zipfiles ---------------")

    # uitvoeren van de zip file vanuit de target locatie
    # unzip van de laatste zip file

    command = "unzip -o " + latest_zipfile + " -d " + rawstagingdir
    print("--------- Unzip commando ------------")
    print(command)
    os.system(command)

    command = "ls " + rawstagingdir
    print("--------- ls van rawstagingdir commando ------------")
    print(command)
    os.system(command)
    print("------------------- End van rawstagingdir commando ---------------")

    # opvragen van de laatste SPORTACTIVITEIT.CSV in gespecifieerde dir
    list_of_csvfiles = glob.glob(rawstagingcsvfiles)
    latest_csvfile = max(list_of_csvfiles, key=os.path.getctime)

    print(latest_csvfile)

    # rename van de laatste SPORTACTIVITEIT.CSV naar bestand met zelfde naam + datum
    command = "mv " + "\"" + latest_csvfile + "\" " + rawstagingdir + rawstagingtargetcsv
    print(command)
    os.system(command)
    # copy de laatste SPORTACTIVITEIT.CSV naar bestand SPORTACTIVITEIT.csv
    command = "cp " + rawstagingdir + rawstagingtargetcsv + " " +  rawstagingdir + "/SPORTACTIVITEIT.csv"
    print(command)
    os.system(command)

    print("load sport preppare data .... Ended ")
    print("----------------------------------------------------------")

if __name__ == "__main__":
    voorbereiden_files()
