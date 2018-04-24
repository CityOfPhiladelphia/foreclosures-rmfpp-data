import io
import csv

import requests
from openpyxl import load_workbook

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
