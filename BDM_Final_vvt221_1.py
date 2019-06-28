
from pyspark import SparkContext
import sys
import os
import csv
sc = SparkContext()

drugs_1 = 'drug_illegal.txt'
drugs_2 = 'drug_sched2.txt'
cities_shp = '500cities_tracts.geojson'
TWEETS_FILE = sys.argv[1]
OUTPUT_FILE = 'output.csv'









def createIndex(shapefile):
  """This function performs the indexing of the censusTract """
  import rtree
  import fiona.crs
  import geopandas as gpd
  
  censusTracts500 = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
  index = rtree.Rtree()
  for idx,geometry in enumerate(censusTracts500.geometry):
    index.insert(idx,geometry.bounds)
  return (index, censusTracts500)

def findZone(p, index,censusTracts500 ):
  """ This function is used to return the correspnding census tract to which the point location belongs to."""
  match = index.intersection((p.x,p.y,p.x,p.y))
  for idx in match:
    if censusTracts500.geometry[idx].contains(p):
      return idx
  return None





def searchDrugTerminology(tweet):
  """This function returns only those tweets which have drug related jargons present in them. The relevant tweets are identified using a regular expressions string match."""
  import re
  drugs_1 = 'drug_illegal.txt'
  drugs_2 = 'drug_sched2.txt'
  with open(drugs_1,'r') as f:
    drugs1_list = f.readlines()
    drugs1_strip = [drugs1.rstrip("\n") for drugs1 in drugs1_list]
    drugs1_final = list(filter(lambda x: len(x)> 1, drugs1_strip))

  with open(drugs_2,'r') as f:
    drugs2_list = f.readlines()
    drugs2_strip = [drugs2.rstrip("\n") for drugs2 in drugs2_list]
    drugs2_final = list(filter(lambda x: len(x)> 1, drugs2_strip))

  total_drugs = drugs1_final + drugs2_final
  single_letter_drugs_regex = '|\sc\s|\se\s|\sh\s|\sx\s|\se\s'
  
  if re.match(r'\b|\b'.join(total_drugs) + single_letter_drugs_regex,tweet):
    return True
  else:
    return False


def filterSpecialSymbols(tweet):

  """This function is used to clean the tweets to remove @mentions , urls and lower case the strings to bringabout a sense of cleanliness"""
  import re
  
  mentions_filtered = re.sub(r'@[A-Za-z0-9]+','',tweet)
  urls_filtered = re.sub('https?://[A-Za-z0-9./]+','',mentions_filtered)
  
  tweets_lower = urls_filtered.lower()
  remove_extra_space = " ".join(tweets_lower.split())
  return (remove_extra_space)

def extractTweetWords(pid,rows):
  import csv
  reader = csv.reader(rows,delimiter='|')
  for fields in reader:
    tweet = str(fields[5])
    latitude = str(fields[1])
    longitude = str(fields[2])
    yield((longitude, latitude), tweet)


def processDrugRelatedTweets(records):
  import pyproj
  import shapely.geometry as geom
  
  
  proj = pyproj.Proj(init="epsg:2263", preserver_units=True)
  index, censusTracts500 = createIndex(cities_shp)
  drugTweetsCounts = {}
  
  for row in records:
    tweet_location = geom.Point(proj(float(row[0][1]), float(row[0][0])))
    censusTract = findZone(tweet_location, index,censusTracts500)
    
    if censusTract:
      yield((censusTracts500['plctract10'][censusTract],censusTracts500['plctrpop10'][censusTract]),row[1])


def func(x): 
    return x[0]




tweets = sc.textFile(TWEETS_FILE)
tweets_rdd = tweets.map(lambda x: x.split('|'))
tweets_rdd_split = tweets_rdd.map(lambda x: ((x[1],x[2]),filterSpecialSymbols(x[5])))

tweet_counts_ZoneLevel = tweets_rdd_split.mapPartitions(processDrugRelatedTweets)
filtered_tweets = tweet_counts_ZoneLevel.filter(lambda x: searchDrugTerminology(x[1]))
print(filtered_tweets.count())

filtered_tweets_key_counts = filtered_tweets.countByKey()
filter_0_population_ct = list(filter(lambda x: x[0][1] > 0, list(filtered_tweets_key_counts.items())))
cleaned_output = list(map(lambda x: (x[0][0],x[1] / x[0][1]), list(filter_0_population_ct)))
sort_by_city_counts = sorted(cleaned_output,key=func)



with open(OUTPUT_FILE,'w') as out:
  csv_out = csv.writer(out)
  csv_out.writerow(['plctract10','normalized_num_tweets'])
  for count in sort_by_city_counts:
    csv_out.writerow(count)
