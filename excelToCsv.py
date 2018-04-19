import petl as etl

table = etl.fromxlsx('saved_homes.xlsx')
print("Done table creation")
etl.tocsv(table,'saved_homes.csv')
