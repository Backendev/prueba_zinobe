import requests,json,time, sqlite3,os
from datetime import datetime
import pandas as pd

#variables generales de app
dir_app = str(os.getcwd())+"\{}"
dir_app = str(str(dir_app).replace("\\",'\\\\'))
print(dir_app)
start = datetime.now()


#Funciones de Tiempo
def time_proces(x):
    end = datetime.now()
    process = end - x
    #time.sleep(1)
    return [process,end]
def time_proces2(x):
    end = datetime.now()
    process = end - x
    #time.sleep(1)
    return process

#primera peticion Api 
url = "https://restcountries-v1.p.rapidapi.com/all"

headers = {
    'x-rapidapi-host': "restcountries-v1.p.rapidapi.com",
    'x-rapidapi-key': "df47b2f50bmshc8f27c8be508784p12d8f0jsnaf8187ef55ff"
    }
response = requests.request("GET", url, headers=headers)
regions = response.text
regions = json.loads(regions)
df_regions = pd.DataFrame(regions)
df_unique_regions = df_regions['region'].unique()
df_unique_regions = [i for i in df_unique_regions if i != '']
print(str(type(df_unique_regions)))
print(df_unique_regions)
time_unique_regions = time_proces(start)
time_unique_regions_now = time_unique_regions[1]
time_unique_regions_delta = time_unique_regions[0]

#Segunda Peticion Api
url = "https://restcountries.eu/rest/v2/all"
response = requests.request("GET", url)
countries = response.text
countries = json.loads(countries)
df_countries = pd.DataFrame(countries)
df_countries_region = df_countries[df_countries.region.isin(df_unique_regions)].sample(frac=1).drop_duplicates(['region'], keep='first')
df_countries_region = df_countries_region[['name','region','languages']]
d_countries_region = dict(df_countries_region) 
d_countries_region = dict(d_countries_region['languages'])
print(str(d_countries_region) + " - "+str(type(d_countries_region)))
d_languages = [v[0]['iso639_1'] for k,v in d_countries_region.items()]
df_countries_region['languages'] = d_languages
df_countries_region
time_countries_region = time_proces(time_unique_regions_now)
time_countries_region_now = time_countries_region[1]
time_countries_region_delta = time_countries_region[0]

#tercera peticion union de dataFrame y lista de segunda peticion
l_requests_languages = list(d_languages)
name_lang = []
for i in l_requests_languages:
    url = f'https://restcountries.eu/rest/v2/lang/{i}'
    response = requests.request("GET", url)
    name_languages = response.text
    name_languages = json.loads(name_languages)
    name_languages = dict(name_languages[0]['languages'][0])
    name_languages = name_languages['name']
    time_languages_name = time_proces(time_countries_region_now)
    time_languages_name_now = time_languages_name[1]
    time_languages_name_delta = time_languages_name[0]
    l_names_langs = []
    l_names_langs = [name_languages,i,time_languages_name_delta,time_languages_name_now]
    name_lang.append(l_names_langs)
    print(name_languages)
names = pd.DataFrame(name_lang,columns=['Language Name','languages','time_languages_name_delta','time_languages_name_now'])
#union de Dataframes y reordenamiento de columnas
pd_results = pd.merge(names,df_countries_region,on=['languages'])
pd_results = pd_results.drop_duplicates(['name'], keep='first')
pd_results['results time'] =pd_results['time_languages_name_now'].apply(time_proces2)
pd_results['time_unique_regions'] =time_unique_regions_delta
pd_results['time_countries_region'] =time_countries_region_delta
pd_results
pd_results_reorden = pd_results
pd_result_reorden = pd_results.reindex(columns=['Language Name','languages','name','region','time_unique_regions','time_countries_region','time_languages_name_delta','time_languages_name_now','results time'])
pd_result_reorden.drop(['time_languages_name_now'], axis = 'columns', inplace=True)
pd_result_reorden['time_languages_name'] = pd_result_reorden['time_languages_name_delta'] 
pd_result_reorden.drop(['time_languages_name_delta'], axis = 'columns', inplace=True)
pd_result_reorden = pd_result_reorden.reindex(columns=['Language Name','languages','name','region','time_unique_regions','time_countries_region','time_languages_name','results time'])
pd_result_reorden['total_time'] = pd_result_reorden[["time_unique_regions", "time_countries_region", "time_languages_name","results time"]].apply(lambda x: x.sum(),axis=1)
pd_result_reorden['mean_time'] = pd_result_reorden[["time_unique_regions", "time_countries_region", "time_languages_name","results time"]].apply(lambda x: x.mean(),axis=1)
pd_result_reorden['min_time'] = pd_result_reorden[["time_unique_regions", "time_countries_region", "time_languages_name","results time"]].apply(lambda x: x.min(),axis=1)
pd_result_reorden['max_time'] = pd_result_reorden[["time_unique_regions", "time_countries_region", "time_languages_name","results time"]].apply(lambda x: x.max(),axis=1)


#Exportaciones
#Json
dir_json = dir_app.format("data.json")
pd_result_reorden.to_json(dir_json)
print(str(dir_json))
#Sqlite
con = sqlite3.connect(dir_app.format("db\\results.sqlite"))
reiniciar_tabla = "delete from languages"
cursor = con.cursor()
cursor.execute(reiniciar_tabla)
sentencia = "INSERT INTO languages VALUES(\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\')"
for rows in pd_result_reorden.iterrows():
    row = dict(rows[1])
    print(str(row['name'])+" - "+str(type(row)))
    cursor = con.cursor()
    sentencia_sql = sentencia.format(str(row['Language Name']),str(row['languages']),
                                     str(str(row['name']).replace("'","")),
                                     str(row['region']),
                                     str(row['time_unique_regions']),str(row['time_countries_region']),str(row['time_languages_name']),
                                     str(row['results time']),str(row['total_time']),
                                     str(row['mean_time']),str(row['min_time']),str(row['max_time']))
    print(sentencia_sql)
    cursor.execute(sentencia_sql)
    con.commit()
