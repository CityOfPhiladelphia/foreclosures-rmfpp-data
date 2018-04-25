import json
import csv
import sys
import io

import petl as etl
import requests
from openpyxl import load_workbook
import shapely
from shapely.geometry import shape, Point
print("Requesting docket...")
r = requests.get("http://docket.philalegal.org/data/foreclosure-addresses.xlsx")
print("Request complete!")
wb = load_workbook(io.BytesIO(r.content))
sh = wb.active
print("Writing docket to csv...")
with open('saved_homes.csv', 'w') as f:
    c = csv.writer(f)
    for k in sh.rows:
        all_cells = []
        for cell in k:
            all_cells.append(cell.value)
        c.writerow(all_cells)
print("File complete!")
print("Requesting carto files...")
t_data = requests.get("https://phl.carto.com/api/v2/sql?q=select%20*%20from%20census_tracts_2010&format=geojson")
tract_data = json.loads(t_data.content)
geo = {}

for feature in tract_data['features']:
    region = shape(feature['geometry'])
    name = feature['properties']['namelsad10']
    geo[name] = region

z_data = requests.get("https://phl.carto.com/api/v2/sql?q=select%20*%20from%20zip_codes&format=geojson")
zip_data = json.loads(z_data.content)
print("Requests complete!")

def addRows(q_geo,field_name,input,output):
    with open(input) as file:
        f = open(output,'w')
        reader = csv.reader(file)
        writer = csv.writer(f)
        for row in reader:
            outrow = list(row)
            if outrow[0] == "indexno":
                outrow.append(field_name)
            elif outrow[8] != "" and outrow[7] != "":
                point = Point(float(outrow[8]), float(outrow[7]))
                for name, region in q_geo.items():
                    if point.within(region):
                        outrow.append(name)
                        break
            writer.writerow(outrow)
        f.close()
print("Creating new csv...")
addRows(geo,'tract','saved_homes.csv','saved_homes_extended.csv')
print("New csv created.")
table = etl.fromcsv('saved_homes_extended.csv')
zip_list = []
tract_list = []
year_list = []
print("Aggregating by zip...")
for feature in zip_data['features']:
    zip_table = etl.select(table, 'zip', lambda x: x == feature['properties']['code'])
    zip_data = etl.data(zip_table)
    zip_data_list = list(zip_data)
    zip_dict = {'zip' : feature['properties']['code'], 'saved' : 0, 'lost' : 0, 'saved_fta' : 0, 'lost_fta' : 0, 'pending' : 0, 'pending_fta' : 0, 'vacant' : 0, 'nonowner' : 0, 'litig/bankr' : 0, 'litig/bankr_fta' : 0, 'shape' : feature['geometry']}
    for b in zip_data_list:
            if b[1] == 'Saved':
                zip_dict['saved'] = zip_dict['saved'] + 1
            elif b[1] == 'Lost':
                zip_dict['lost'] = zip_dict['lost'] + 1
            elif b[1] == 'Saved - FTA':
                zip_dict['saved_fta'] = zip_dict['saved_fta'] + 1
            elif b[1] == 'Lost - FTA':
                zip_dict['lost_fta'] = zip_dict['lost_fta'] + 1
            elif b[1] == 'Pending':
                zip_dict['pending'] = zip_dict['pending'] + 1
            elif b[1] == 'Pending - FTA':
                zip_dict['pending_fta'] = zip_dict['pending_fta'] + 1
            elif b[1] == 'Vacant':
                zip_dict['vacant'] = zip_dict['vacant'] + 1
            elif b[1] == 'Nonowner':
                zip_dict['nonowner'] = zip_dict['nonowner'] + 1
            elif b[1] == 'Litig/Bankr':
                zip_dict['litig/bankr'] = zip_dict['litig/bankr'] + 1
            elif b[1] == 'Litig/Bankr - FTA':
                zip_dict['litig/bankr_fta'] = zip_dict['litig/bankr_fta'] + 1
    zip_list.append(zip_dict)

with open('zip_aggregate.csv','w') as file:
    dict_writer = csv.DictWriter(file,zip_list[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(zip_list)
print("Aggregation complete!")
print("Aggregating by tract...")
for feature in tract_data['features']:
    tract_table = etl.select(table, 'tract', lambda x: x == feature['properties']['namelsad10'])
    tract_data = etl.data(tract_table)
    tract_data_list = list(tract_data)
    tract_dict = {'tract' : feature['properties']['namelsad10'], 'saved' : 0, 'lost' : 0, 'saved_fta' : 0, 'lost_fta' : 0, 'pending' : 0, 'pending_fta' : 0, 'vacant' : 0, 'nonowner' : 0, 'litig/bankr' : 0, 'litig/bankr_fta' : 0, 'shape' : feature['geometry']}
    for b in tract_data_list:
        if b[1] == 'Saved':
            tract_dict['saved'] = tract_dict['saved'] + 1
        elif b[1] == 'Lost':
            tract_dict['lost'] = tract_dict['lost'] + 1
        elif b[1] == 'Saved - FTA':
            tract_dict['saved_fta'] = tract_dict['saved_fta'] + 1
        elif b[1] == 'Lost - FTA':
            tract_dict['lost_fta'] = tract_dict['lost_fta'] + 1
        elif b[1] == 'Pending':
            tract_dict['pending'] = tract_dict['pending'] + 1
        elif b[1] == 'Pending - FTA':
            tract_dict['pending_fta'] = tract_dict['pending_fta'] + 1
        elif b[1] == 'Vacant':
            tract_dict['vacant'] = tract_dict['vacant'] + 1
        elif b[1] == 'Nonowner':
            tract_dict['nonowner'] = tract_dict['nonowner'] + 1
        elif b[1] == 'Litig/Bankr':
            tract_dict['litig/bankr'] = tract_dict['litig/bankr'] + 1
        elif b[1] == 'Litig/Bankr - FTA':
            zip_dict['litig/bankr_fta'] = zip_dict['litig/bankr_fta'] + 1
    tract_list.append(tract_dict)

with open('tract_aggregate.csv','w') as file:
    dict_writer = csv.DictWriter(file,tract_list[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(tract_list)
print("Aggregation complete!")
print("Aggregating by year...")
for i in range(2008,2032):
    year_table = etl.select(table, 'maxdate', lambda x: x[0:4] == str(i))
    year_data = etl.data(year_table)
    year_data_list = list(year_data)
    year_dict = {'year' : i, 'saved' : 0, 'lost' : 0, 'saved_fta' : 0, 'lost_fta' : 0, 'pending' : 0, 'pending_fta' : 0, 'vacant' : 0, 'nonowner' : 0, 'litig/bankr' : 0, 'litig/bankr_fta' : 0}
    for b in year_data_list:
        if b[1] == 'Saved':
            year_dict['saved'] = year_dict['saved'] + 1
        elif b[1] == 'Lost':
            year_dict['lost'] = year_dict['lost'] + 1
        elif b[1] == 'Saved - FTA':
            year_dict['saved_fta'] = year_dict['saved_fta'] + 1
        elif b[1] == 'Lost - FTA':
            year_dict['lost_fta'] = year_dict['lost_fta'] + 1
        elif b[1] == 'Pending':
            year_dict['pending'] = year_dict['pending'] + 1
        elif b[1] == 'Pending - FTA':
            year_dict['pending_fta'] = year_dict['pending_fta'] + 1
        elif b[1] == 'Vacant':
            year_dict['vacant'] = year_dict['vacant'] + 1
        elif b[1] == 'Nonowner':
            year_dict['nonowner'] = year_dict['nonowner'] + 1
        elif b[1] == 'Litig/Bankr':
            year_dict['litig/bankr'] = year_dict['litig/bankr'] + 1
        elif b[1] == 'Litig/Bankr - FTA':
            zip_dict['litig/bankr_fta'] = zip_dict['litig/bankr_fta'] + 1
    year_list.append(year_dict)
with open('year_aggregate.csv','w') as file:
    dict_writer = csv.DictWriter(file,year_list[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(year_list)
print("Aggregation complete!")
