
import commonfunction
from datetime import date

# ---------------------------------------------------------
# geeft de waarde terug van vandaag in het formaat 20240126
# inputwaarde
today = date.today()
# outputwaarde
todaytekst = commonfunction.data_todaytekst(today)
print(todaytekst)
# ----------------------------------------------------------
