import json
import io
import csv
import sys

import petl as etl
import requests
from openpyxl import load_workbook
import shapely
from shapely.geometry import shape, Point

r = requests.get("http://docket.philalegal.org/data/foreclosure-addresses.xlsx")
print("Got request")
wb = load_workbook(io.BytesIO(r.content))
sh = wb.active
with open('saved_homes.csv', 'w') as f:  # open('test.csv', 'w', newline="") for python 3
    c = csv.writer(f)
    for k in sh.rows:
        allCells = []
        for cell in k:
            allCells.append(cell.value)
        c.writerow(allCells)

#og_csv = etl.fromcsv('saved_homes.csv')
f = io.open('tract.geojson','r',encoding='utf-8-sig')
#k = io.open('cb_2017_42_bg_500k.json','r',encoding='utf-8-sig')
tdata = f.read()
#bdata = k.read()
tract_data = json.loads(tdata)
#block_data = json.loads(bdata)
# lon = etl.values(og_csv, 'longitude')
# lat = etl.values(og_csv, 'latitude')
geo = {}
#bl_geo = {}

for feature in tract_data['features']:
    region = shape(feature['geometry'])
    name = feature['properties']['NAMELSAD10']
    geo[name] = region

#for feature in block_data['features']:
#    region = shape(feature['geometry'])
#    name = feature['properties']['NAME']
#    bl_geo[name] = region



def addRows(q_geo,fieldName,input,output):
    with open(input) as file:
        f = open(output,'w')
        reader = csv.reader(file)
        # skip headers
        # next(reader)
        writer = csv.writer(f)
        for row in reader:
            outrow = list(row)
            if outrow[0] == "indexno":
                outrow.append(fieldName)
            elif outrow[8] != "" and outrow[7] != "":
                point = Point(float(outrow[8]), float(outrow[7]))
                found = False

                for name, region in q_geo.items():
                    if point.within(region):
                        found = True
                        outrow.append(name)
                        break
                # if not found:
            writer.writerow(outrow)
        f.close()

addRows(geo,'tract','saved_homes.csv','saved_homes_extended.csv')
#addRows(bl_geo,'block_group','saved_homes_extended.csv','saved_homes_extended2.csv')


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
