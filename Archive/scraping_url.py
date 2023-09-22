from bs4 import BeautifulSoup
import requests

my_url = "https://www.ratingraph.com/tv-shows/one-piece-ratings-17673/"

response = requests.get(my_url)
soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find('table')


rows = table.find_all('tr')
for row in rows:
    columns = row.find_all('td')
    print([col.text for col in columns])
