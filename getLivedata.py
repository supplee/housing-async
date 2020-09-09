##
## getLiveData.py
## 		-get craigslist source HTML via STDIN (as a forked process from go) 
##		-save structured data locally.

# import get (similar to wget) to make and save an HTML request
# craigslist scraping tips courtesy of Riley Predum at Towards Data Science
from requests import get
from bs4 import BeautifulSoup
import json
import sys
import re
import random
import numpy as np
import pandas as pd
from argparse import ArgumentParser

myURL = "https://sfbay.craigslist.org/search/pen/apa?hasPic=1&availabilityMode=0&sale_date=all+dates"
debug = 0
moreDebug = 0

# Apartment data structure
class Apartment:
	def __init__(self, sourceData, src='html'):
		self.rawData = sourceData
		self.src = src

		# Initialize with default values
		self.url = 'missing'
		self.pid = -1
		self.title = 'missing'
		self.date = ''
		self.price = np.nan
		self.hood = 'missing'
		self.rooms = np.nan
		self.bathrooms = 0
		self.area = np.nan
		self.attr = ''
		self.lat = np.nan
		self.lon = np.nan

		if debug:
			print("\nNEW APARTMENT CREATED")

		if self.src == 'html':
			# Craigslist post ID, URL, and title
			postID = sourceData.find('a',class_='result-title hdrlnk')
			self.url = postID['href']
			self.pid = int(postID['data-id'])
			self.title = postID.text.strip()
			self.title = self.title.replace('!',' ')
			self.title = self.title.strip()

			# Post date
			postDate = sourceData.find('time',class_='result-date')['datetime']
			postDate,postTime = postDate.split(' ')
			self.date = postDate
			#if debug:
			#	print("Posted on",self.date)

			# Price ('y' in our data set)
			postPrice = sourceData.find('span',class_='result-price')
			postPrice = postPrice.text.strip()
			postPrice = postPrice.strip('$')
			postPrice = postPrice.replace(',','')
			self.price = int(postPrice)
			if debug:
				print("On ",self.date,"(",self.pid,"): $",self.price,"-",self.title)

			# Neighborhood

			postHood = self.rawData.find('span', attrs={'class':'result-hood'})
			try:
				postHood = postHood.text
			except:
				postHood = 'missing'
			postHood = postHood.replace('(',' ')
			postHood = postHood.replace(')',' ')
			postHood = postHood.strip()
			self.hood = postHood.title()
			
			if debug:
				print("Located in",self.hood)

			# Bedrooms and size in square feet
			postRooms = sourceData.find('span',class_='housing')
			try:
				postRooms = postRooms.text.strip()
				postRooms = postRooms.strip('-')
				idx = postRooms.find("br")

				sizeIdx = postRooms.find("ft")
				#if debug:
				#	print(idx,sizeIdx,postRooms)
				if sizeIdx > 0:
					postSize = postRooms[idx+1:sizeIdx]
					postSize = postSize.replace('-',' ')
					postSize = postSize.replace('r',' ')
					postSize = postSize.strip()
					
					self.area = int(postSize)
				else:
					postSize = -1
					self.area = -1

				if idx > 0:
					postRooms = postRooms[0:idx]
				else:
					postRooms = 0	


			except:
				postRooms = "0"
				postSize = -1
			try:
				self.rooms = int(postRooms)
				if debug:
					print(self.rooms, "bedrooms")
					try:
						print(self.area,"sq ft")
					except:
						print("no area data found")
			except:
				self.rooms = 0
				self.area = -1


# Get craigslist housing posts and return as an array of BeautifulSoup objects
def GetPostsFromSTDIN():
	posts = []
	
	
	# Get HTTP response from web server

	# BeautifulSoup path to next page ---> nextURL = html_soup.find('link',attrs={'rel':'next'})['href']
	rawResponse = sys.stdin.read()

	# Parse results as HTML
	try:
		html_soup = BeautifulSoup(rawResponse, 'html.parser')
	except:
		print("Unable to interpret STDIN as HTML!")
		

	# Get list of post objects
	newPosts = html_soup.find_all('li', class_='result-row')
	posts += newPosts
	if debug:
		print(type(posts))
		print(len(posts))
	


	if moreDebug:
		print(len(posts),"posts fetched")


	return posts


# Takes data from the main search results page and scrapes additional attributes by crawling through each individual post page
def GetInfoFromPost():
#	missingData = df.loc[df['attributes'] == '']
#	limitCounter = 0
#	if moreDebug:
#		numberMissing = len(missingData.index)
#		print(numberMissing,"RECORDS WITHOUT ATTRIBUTE DATA")
#
#	for i in missingData.index:
#		limitCounter += 1
#		if limitCounter <= limit:
#			thisURL = missingData.loc[i,'url']
	try:
		rawResponse = sys.stdin.read()
	except:
		print("Unable to get HTTP request from STDIN")

	try:
		html_soup = BeautifulSoup(rawResponse, 'html.parser')
	except:
		print("Unable to parse request as HTML for record")

	try:
		latData = html_soup.find('div',class_='viewposting')['data-latitude']
		lonData = html_soup.find('div',class_='viewposting')['data-longitude']
	except:
		latData = np.nan
		lonData = np.nan
	df.at[i,'latitude'] = latData
	df.at[i,'longitude'] = lonData

	# Fetch attributes and number of bathrooms, try again for area in sq ft if not found
	spans = html_soup.find_all('span',class_='shared-line-bubble')
	for span in spans:
		attrText = span.text
		try:
			bathroomData = re.findall("BR\s+/\s+(\d+\.?5?)",attrText)
			#idx = attrText.find("BR / ")
			if bathroomData != []:
				bathroomData = bathroomData.pop()
				#bathroomData = int(attrText[idx+5])
				df.at[i,'bathrooms'] = bathroomData

				#if moreDebug:
				#	print(bathroomData,end='')
		except:
			if debug:
				print("no bathroom data found")

		if df.at[i,'sqft'] < 0:
			attrText = attrText.strip()
			idx=attrText.find("ft2")
			if idx>0:
				areaData = attrText[0:idx]
				try:
					df.at[i,'sqft'] = int(areaData)
				except:
					df.at[i,'sqft'] = 0

	attributeString=''
	#pattrs=html_soup.find_all('p', class_='attrgroup')
	#for p in pattrs:
	#spans = p.findChildren('span',class_='', attr=s{'id': '', 'class': ''})
	spans = html_soup.find_all('span',class_='',attrs={'id':'', 'class':''})
	for span in spans:
		try:
			attr = span.text
			attr = attr.replace(' ','')
			attr = attr.lower()

			if len(attr) > 8:
				attributeString += attr[:8]+"|"
			elif len(attr)>1 and len(attr) < 9:
				attributeString += attr+"|"
		except:
			next

	if debug:
		print("Got apartment attributes:",attributeString)
	df.at[i,'attributes'] = attributeString

	return "dump of dataframe"
	#return df

# Convert BeautifulSoup results to apartment objects and then to a dataframe
def DumpObjectsToSTDOUT(apartments):
	if len(apartments) < 1:
		sys.stdout.write("error no posts found\n")
		return

	postsID = []
	postsURL = []
	postsDate = []
	postsTitle = []
	postsHood = []
	postsRooms = []
	postsBathrooms = []
	postsArea = []
	postsPrice = []
	postsAttr = []
	postsLat = []
	postsLon = []

	for a in apartments:
		# Create series from each scraped apartment listing
		postsID.append(a.pid)
		postsURL.append(a.url)
		postsDate.append(a.date)
		postsTitle.append(a.title)
		postsHood.append(a.hood)
		postsRooms.append(a.rooms)
		postsBathrooms.append(a.bathrooms)
		postsArea.append(a.area)
		postsPrice.append(a.price)
		postsAttr.append(a.attr)
		postsLat.append(a.lat)
		postsLon.append(a.lon)

	
	postsLat = pd.to_numeric(postsLat)
	postsLon = pd.to_numeric(postsLon)
	dfNew = pd.DataFrame({'pid': postsID,
		'date': postsDate,
		'neighborhood': postsHood,
		'title': postsTitle,
		'bedrooms': postsRooms,
		'bathrooms': postsBathrooms,
		'sqft': postsArea,
		'url': postsURL,
		'price': postsPrice,
		'attributes': postsAttr,
		'latitude': postsLat,
		'longitude': postsLon
		}, index=postsID)

	dfNew.drop_duplicates(subset=['pid'],inplace=True, keep='last')
	#parsed = dfNew.to_json(orient="index") <-- results in escaped slashes in URLs, which is not needed as they are already set in double quotes
	parsed = json.dumps(dfNew.to_dict('index'))
	sys.stdout.write(parsed)

	if debug:
		dfNew.to_csv("test.csv")

	return dfNew

# Save dataframe
def saveDataFrame(df,key='df',fileName='data.parquet'):
	try:
		df.to_parquet(fileName)
	except:
		print("Error saving",key,"to",fileName)
		exit(1)

def main():
	posts= []
	apartments = []

	posts = GetPostsFromSTDIN()
	
	for p in posts:
		apartments.append(Apartment(p))

	# Dump the newly created apartment objects to STDOUT (i.e. JSON objects)
	# The 'merge' mode will merge the new posts with previously saved posts into one table
	DumpObjectsToSTDOUT(apartments)
	exit(0)

if __name__ == "__main__":
	main()