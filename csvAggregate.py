import io
import csv
import sys
import json
import io

import petl as etl

table = etl.fromcsv('saved_homes_extended.csv')
zipLst = []

f = io.open('Zipcodes_Poly.geojson','r',encoding='utf-8-sig')
zdata = f.read()
zip_data = json.loads(zdata)

for feature in zip_data['features']:
    zipTable = etl.select(table, 'zip', lambda x: x == feature['properties']['CODE'])
    zipData = etl.data(zipTable)
    zipDataLst = list(zipData)
    zipDict = {'zip' : feature['properties']['CODE'], 'saved' : 0, 'lost' : 0, 'saved_fta' : 0, 'lost_fta' : 0, 'pending' : 0, 'pending_fta' : 0, 'vacant' : 0, 'nonowner' : 0, 'litig/bankr' : 0, 'shape' : feature['geometry']}
    for b in zipDataLst:
            if b[1] == 'Saved':
                zipDict['saved'] = zipDict['saved'] + 1
            elif b[1] == 'Lost':
                zipDict['lost'] = zipDict['lost'] + 1
            elif b[1] == 'Saved - FTA':
                zipDict['saved_fta'] = zipDict['saved_fta'] + 1
            elif b[1] == 'Lost - FTA':
                zipDict['lost_fta'] = zipDict['lost_fta'] + 1
            elif b[1] == 'Pending':
                zipDict['pending'] = zipDict['pending'] + 1
            elif b[1] == 'Pending - FTA':
                zipDict['pending_fta'] = zipDict['pending_fta'] + 1
            elif b[1] == 'Vacant':
                zipDict['vacant'] = zipDict['vacant'] + 1
            elif b[1] == 'Nonowner':
                zipDict['nonowner'] = zipDict['nonowner'] + 1
            elif b[1] == 'Litig/Bankr':
                zipDict['litig/bankr'] = zipDict['litig/bankr'] + 1
    if len(zipDataLst) != 0:
        zipLst.append(zipDict)

with open('zip_aggregate.csv','w') as file:
    dict_writer = csv.DictWriter(file,zipLst[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(zipLst)

f = io.open('tract.geojson','r',encoding='utf-8-sig')
tdata = f.read()
tract_data = json.loads(tdata)
tractLst = []

for feature in tract_data['features']:
    tractTable = etl.select(table, 'tract', lambda x: x == feature['properties']['NAMELSAD10'])
    tractData = etl.data(tractTable)
    tractDataLst = list(tractData)
    tractDict = {'tract' : feature['properties']['NAMELSAD10'], 'saved' : 0, 'lost' : 0, 'saved_fta' : 0, 'lost_fta' : 0, 'pending' : 0, 'pending_fta' : 0, 'vacant' : 0, 'nonowner' : 0, 'litig/bankr' : 0, 'shape' : feature['geometry']}
    for b in tractDataLst:
            if b[1] == 'Saved':
                tractDict['saved'] = tractDict['saved'] + 1
            elif b[1] == 'Lost':
                tractDict['lost'] = tractDict['lost'] + 1
            elif b[1] == 'Saved - FTA':
                tractDict['saved_fta'] = tractDict['saved_fta'] + 1
            elif b[1] == 'Lost - FTA':
                tractDict['lost_fta'] = tractDict['lost_fta'] + 1
            elif b[1] == 'Pending':
                tractDict['pending'] = tractDict['pending'] + 1
            elif b[1] == 'Pending - FTA':
                tractDict['pending_fta'] = tractDict['pending_fta'] + 1
            elif b[1] == 'Vacant':
                tractDict['vacant'] = tractDict['vacant'] + 1
            elif b[1] == 'Nonowner':
                tractDict['nonowner'] = tractDict['nonowner'] + 1
            elif b[1] == 'Litig/Bankr':
                tractDict['litig/bankr'] = tractDict['litig/bankr'] + 1
    tractLst.append(tractDict)

with open('tract_aggregate.csv','w') as file:
    dict_writer = csv.DictWriter(file,tractLst[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(tractLst)


yearLst = []
for i in range(2008,2032):
    yearTable = etl.select(table, 'maxdate', lambda x: x[0:4] == str(i))
    yearData = etl.data(yearTable)
    yearDataLst = list(yearData)
    yearDict = {'year' : i, 'saved' : 0, 'lost' : 0, 'saved_fta' : 0, 'lost_fta' : 0, 'pending' : 0, 'pending_fta' : 0, 'vacant' : 0, 'nonowner' : 0, 'litig/bankr' : 0}
    for b in yearDataLst:
        if b[1] == 'Saved':
            yearDict['saved'] = yearDict['saved'] + 1
        elif b[1] == 'Lost':
            yearDict['lost'] = yearDict['lost'] + 1
        elif b[1] == 'Saved - FTA':
            yearDict['saved_fta'] = yearDict['saved_fta'] + 1
        elif b[1] == 'Lost - FTA':
            yearDict['lost_fta'] = yearDict['lost_fta'] + 1
        elif b[1] == 'Pending':
            yearDict['pending'] = yearDict['pending'] + 1
        elif b[1] == 'Pending - FTA':
            yearDict['pending_fta'] = yearDict['pending_fta'] + 1
        elif b[1] == 'Vacant':
            yearDict['vacant'] = yearDict['vacant'] + 1
        elif b[1] == 'Nonowner':
            yearDict['nonowner'] = yearDict['nonowner'] + 1
        elif b[1] == 'Litig/Bankr':
            yearDict['litig/bankr'] = yearDict['litig/bankr'] + 1
    if len(yearDataLst) != 0:
        yearLst.append(yearDict)


with open('year_aggregate.csv','w') as file:
    dict_writer = csv.DictWriter(file,yearLst[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(yearLst)
