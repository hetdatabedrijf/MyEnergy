import pandas as pd
# import numpy as np
import PyPDF2
import textract
import re
import os
from nltk.tokenize import word_tokenize
import nltk
from nltk.corpus import stopwords
import ssl


Listdirectory='/Users/paulvanbrabant/Library/CloudStorage/OneDrive-Gedeeldebibliotheken-IBSOLUTIONSBVBA/HETDATABEDRIJF - Documents/2023_MVP_SBI/Pvb/'
print(os.listdir(Listdirectory))

directory = Listdirectory
for filename2 in os.listdir(directory):

    ## filename = 'marketing_predicts_for_2023_ebook.pdf'
    filename = Listdirectory + filename2
    print(filename)

    pdfFileObj = open(filename, 'rb')
    pdfReader = PyPDF2.PdfReader(pdfFileObj)

    num_pages = len(pdfReader.pages)
    print(num_pages)

    count = 0
    text = ""

    while count < num_pages:
        pageObj = pdfReader.pages[count]
        count += 1
        text += pageObj.extract_text()

        if text != "":
            text = text

        else:
            ## text = textract.process('http://bit.ly/epo_keyword_extraction_document', method='tesseract', language='eng')
            text = 'Geen Text'


        text = text.encode('ascii', 'ignore').lower().decode('ascii')
        keywords = re.findall(r'[a-zA-Z]\w+', text)
        ## keywords = re.findall(' ([a-zA-Z]+) ', text, flags=re.IGNORECASE)
        ## keywords = re.findall(r'\w+cap', text, flags=re.IGNORECASE)  ## finding only word cap

        # tokens = word_tokenize(text)
        # stop_words = stopwords.words('english')
        # filtered_text = [word for word in tokens if not word.lower() in stop_words]
        # clean_text = [word.lower() for word in filtered_text if word.isalpha()]
        # print(clean_text)

        df = pd.DataFrame(list(set(keywords)), columns=['keywords'])


        def weightage(word, text, number_of_documents=1):
            word_list = re.findall(word, text)
            number_of_times_word_appeared = len(word_list)
            tf = number_of_times_word_appeared / float(len(text))
            idf = np.log((number_of_documents) / float(number_of_times_word_appeared))
            tf_idf = tf * idf
            return number_of_times_word_appeared, tf, idf, tf_idf


        df['number_of_times_word_appeared'] = df['keywords'].apply(lambda x: weightage(x, text)[0])
        df['tf'] = df['keywords'].apply(lambda x: weightage(x, text)[1])
        df['idf'] = df['keywords'].apply(lambda x: weightage(x, text)[2])
        df['tf_idf'] = df['keywords'].apply(lambda x: weightage(x, text)[3])

        df = df.sort_values('tf_idf', ascending=True)
        ## df.head(25)

        newdf = df.sort_values(by='number_of_times_word_appeared', ascending=False)
        print(newdf)

        ## keywordsearch = "capability"
        ## filtered_df = df.loc[df['keywords'].str.contains(keywordsearch)]
        ## print(filtered_df)