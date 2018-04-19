import json
import io
import csv
import sys

import petl as etl
import shapely
from shapely.geometry import shape, Point

#og_csv = etl.fromcsv('saved_homes.csv')
f = io.open('tract.geojson','r',encoding='utf-8-sig')
k = io.open('cb_2017_42_bg_500k.json','r',encoding='utf-8-sig')
tdata = f.read()
bdata = k.read()
tract_data = json.loads(tdata)
block_data = json.loads(bdata)
# lon = etl.values(og_csv, 'longitude')
# lat = etl.values(og_csv, 'latitude')
geo = {}
bl_geo = {}

for feature in tract_data['features']:
    region = shape(feature['geometry'])
    name = feature['properties']['NAMELSAD10']
    geo[name] = region

for feature in block_data['features']:
    region = shape(feature['geometry'])
    name = feature['properties']['NAME']
    bl_geo[name] = region



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
addRows(bl_geo,'block_group','saved_homes_extended.csv','saved_homes_extended2.csv')
