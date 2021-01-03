import csv
from bs4 import BeautifulSoup
import requests
from pyspark.shell import spark, sqlContext

# specify the url
quote_page = 'https://en.wikipedia.org/wiki/List_of_United_States_cities_by_population'
# query the website and return the html to the variable ‘page’
page = requests.get(quote_page).text

soup = BeautifulSoup(page, 'html.parser')
# print(soup.prettify())
# Table for 2019-estimate, 2016 land area KM, 2016 population density extracted (rank ascending)
tab = soup.find('div', {'class': 'mw-parser-output'})
tab = tab.find('table', {'class': 'wikitable sortable'})
tab = tab.find('tbody')
tab = tab.findAll('tr')
city_pop_us_tab = []
ans = []
# got completed table needed
for tr in tab:
    city_pop_us_tab = tr.text.strip()
    city_pop_us_tab = city_pop_us_tab.replace(u'\xa0', u' ')
    city_pop_us_tab = city_pop_us_tab.split('\n')
    ans.append(city_pop_us_tab)
    # print(city_pop_us_tab)
    # print('+++++++++++++++++')
ans.remove(ans[0])

rdd = spark.sparkContext.parallelize(ans)
# print(rdd.collect())
dict_dataframe = sqlContext.createDataFrame(rdd,
                                            ['rank', ' ', 'city', '', 'state', '', '2019estimate', '', '2010censor',
                                             '', 'change', '', '2016land1', '', '2016land2', '', '2016pop1', '',
                                             '2016pop2'])

dict_dataframe.show()
