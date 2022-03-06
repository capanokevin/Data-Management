# -*- coding: utf-8 -*-
"""ScopusAPI.ipynb

# Import data in mongo db
"""

pip install pymongo[srv] --quiet

!pip install dnspython --quiet

# Prima di importare i moduli riavviare il runtime ...
import pymongo
from pymongo import MongoClient
import dns

# Usiamo pymongo come driver verso mongoDB 
client = pymongo.MongoClient("mongodb+srv://capanokevin:1234@cluster0.u1mid.mongodb.net/DataMan?retryWrites=true&w=majority")
db = client.DataMan
users = db.users

# COMANDI UTILI
# result = users.delete_many({}) #elimina tutti i record della collezione
# result = users.insert_one(documento)
# result = users.insert_many(dizionario di dizionari)

# Una volta messo il file json qui su server, lo mando diretto su mongo ... in realtà bisognerebbe mandarli uno a uno o in blocco ma qui possiamo
# permetterci questo ...
import json
data = []
with open('/content/sample_data/json.json') as f:
    for line in f:
        data.append(json.loads(line))

result = users.insert_many(data)

"""# Scopus API acquisition ... after a hundred attempts ..."""

pip install pybliometrics --quiet

# Qui devo mettere chiave API e successivamente anche Inst-Token
from pybliometrics.scopus import ScopusSearch
from pybliometrics.scopus import AuthorRetrieval

import pandas as pd

# Function that returns me the list of queries to use in ScopusSearch ... ottengo una lista con le query di tutti gli 
# articoli che ha scritto 

def get_query_list(post):
    query_list = []
    for idx, pub in enumerate(post['Publications']):
        publication = pub['Title'].split(' ')
        query = 'TITLE ( ' 
        for idx2,word in enumerate(publication):
            if idx2 == len(publication) - 1:
                query = query + word + ' ) '
            else:
                query = query + word + ' AND '
        query_list.append(query)
    return query_list

# Function that hopefully will guess the right spell of the name ... we'll see ...

def get_author_name(post):
    name = post['Name']
    surname = post['Surname']
    surname_name = surname + ', ' + name 
    return surname_name

# Function that catch the Scopus Author ID and query the database ...

def get_author_id(query_list, name):
    i = 1
    for query in query_list[0:i]:
        try:
            search = ScopusSearch(query)
            df = pd.DataFrame(pd.DataFrame(search.results))
            author_names = list(df['author_names'][0].split(';'))    
            author_IDs = list(df['author_ids'][0].split(';'))
            #print(author_names, author_IDs)
            index = author_names.index(name)          # devo ricavare la posizione dell'autore nella lista
            author_id = author_IDs[index]                    # uso la posizione per estrarre l'ID corretto
        except:
            i += 1      # significa che non trovo l'articolo su scopus, la query non dà risultati, quindi ne uso un'altra
                        # TODOs: aggiungere un flag sul post mongodb che dica non trovato su scopus
    return author_id

def get_affiliation_history(author):
    pubs = pd.DataFrame(author.get_documents(refresh=10))
    pubs['coverDate'] = pd.to_datetime(pubs['coverDate'])
    pubs['Year'] = None
    for idx, row in enumerate(pubs['coverDate']):
        pubs['Year'][idx] = row.year

cursor = users.find()
for post in cursor[0:1]:
    post_queries = get_query_list(post)    # ottengo la lista di query
    name = get_author_name(post)
    author_id = get_author_id(query_list = post_queries, name = name)       # ottengo l'ID dell'autore
    get_affiliation_history(author)

docs['author_names']

docs['coverDate'] = pd.to_datetime(docs['coverDate'])
docs['coverDate']

docs['coverDate'][0].year

docs['Year'] = None
for idx, row in enumerate(docs['coverDate']):
    docs['Year'][idx] = row.year
docs['Year']

docs[['eid','title', 'afid', 'affilname', 'author_names', 'author_ids', 'author_afids','Year', 'citedby_count', 'openaccess', 'fund_no', 'fund_sponsor']].head(2)

df = pd.DataFrame({}, columns = ['Affiliation', 'Year'])
row = []
for idx, row in enumerate(docs['title']):
    lista_autors_ids = list(docs['author_ids'][idx].split(';'))         # splitto gli IDs degli autori
    lista_affiliation_IDs = list(docs['author_afids'][idx].split(';'))  # splitto i rispettivi IDs delle affiliazioni
    indice_autore = lista_autors_ids.index(author_id)                 # cerco l'ordine dell'autore che mi interessa nella lista degli ID autori
    affiliation_id = lista_affiliation_IDs[indice_autore]             # grazie all'ordine dato dagli ID degli autori, estraggo quello della sua affiliazione 

    lista_aff_id = list(docs['afid'][idx].split(';'))                   # splitto gli ID delle affiliazioni presenti
    lista_aff_name = list(docs['affilname'][idx].split(';'))            # splitto le affiliazioni
    indice_aff = lista_aff_id.index(affiliation_id)                   # cerco tra gli ID delle affiliazioni quello che mi interessa
    aff = lista_aff_name[indice_aff]                                  # estraggo il nome dell'affiliazione che mi interessa
    
    df = df.append({'Affiliation': aff, 'Year': docs['Year'][idx]}, ignore_index=True )
    min = df.groupby('Affiliation').min()
    max = df.groupby('Affiliation').max()

df.groupby('Affiliation').min()

df

