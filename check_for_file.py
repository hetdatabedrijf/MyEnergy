import glob
import os
import shutil
import load_dataGCP_CloudStorage
from datetime import date


def check_files():
    #  Zoeken van een bestand in een directory

    # Instantieren van variabelen

    # Constant definities die kunnen wijzigen
    searchdir = "/Users/paulvanbrabant/DATA_DEVELOP/D_MEETMEERENERGIE/01_RAW/RAWSTAGINGSPORT_BEWEEG_HIST"
    searchfile = "/SPORTACTIVITEIT_BEWEEG_HIST.csv"


    print("------------------- Start searching ---------------")

    print("looking in  ... " + searchdir)
    print("looking for .... " + searchfile)


    print("------------------- result searching ---------------")

    list_files = glob.glob(searchdir + searchfile, recursive=True)

    print(list_files)
    print("file exist? '0'=No, >'0'=Yes :" + str(len(list_files)))

    if (len(list_files) > 0):
        print ("file found: " + searchdir + searchfile )
        command = "ls " + searchdir + searchfile
        print("delete file: " + searchdir + searchfile)
        command = "rm " + searchdir + searchfile
        os.system(command)


if __name__ == "__main__":
    check_files()
