# common functions to use in all python scripts

import os
import csv
from datetime import date

# het bepalen van tijdsvariabelen input / output

def data_todaytekst(inputvar1):
    print(inputvar1)
    todaytekst = str(inputvar1)
    todaytekst = todaytekst.translate({ord('-'): None})
    print(todaytekst)
    return todaytekst
if __name__ == "__main__":
    data_todaytekst()

