import requests

r = requests.get("http://docket.philalegal.org/data/foreclosure-addresses.xlsx")
print("Got request")
f = open('saved_homes.xlsx','wb')
print("Made file")
f.write(r.content)
print("Done excel write")
