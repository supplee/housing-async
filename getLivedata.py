!/usr/bin/python3

## getLiveData.py
## 		-get data for rent modeling from craigslist 
##		-store in a dataframe and save locally

# import get (similar to wget) to make and save an HTML request
# craigslist scraping tips courtesy of Riley Predum at Towards Data Science
from requests import get
from bs4 import BeautifulSoup
import sys
import re
import random
import numpy as np
import pandas as pd
from argparse import ArgumentParser

numberofListings = 1200 # 10 pages
myURL = "https://sfbay.craigslist.org/search/pen/apa?hasPic=1&availabilityMode=0&sale_date=all+dates"
debug = 0
moreDebug = 0
fileName = 'data.parquet'
writeMode = 'merge'

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
def getPostData(baseURL='',n=1200):
	posts = []
	postCounter = 0

	while postCounter < n:
		if baseURL == '':
			#baseURL = input('Enter a craigslist URL to get housing data from: ')
			baseURL = myURL

		# Get HTTP response from web server
		try:
			if postCounter == 0:
				rawResponse = get(baseURL) # baseURL of first result page with relevant filters applied
			else:
				baseURL = html_soup.find('link',attrs={'rel':'next'})['href']
				rawResponse = get(baseURL) # get next result bro!

			if moreDebug>0:
					print("S =",postCounter,"  (PAGE)",baseURL,"\n",rawResponse.text[:128])
		except:
			print("Connection failed!")
			exit(1)

		# Parse results as HTML
		try:
			html_soup = BeautifulSoup(rawResponse.text, 'html.parser')
		except:
			print("Unable to interpret URL as HTML!")
			exit(2)

		# Get list of post objects
		newPosts = html_soup.find_all('li', class_='result-row')
		posts += newPosts

		postCounter += len(newPosts)
		if debug:
			print(type(posts))
			print(len(posts))
	


	if moreDebug:
		print(len(posts),"posts fetched")


	return posts

# Load a parquet from disk
def getDataFromDisk(fileName='data.parquet'):
	df=pd.read_parquet(fileName)
	#store = pd.HDFStore(fileName,'r')
	#df = store['df']
	#store.close()
	return df

# Takes data from the main search results page and scrapes additional attributes by crawling through each individual post page
def getMissingInformation(df,limit=12000):
	missingData = df.loc[df['attributes'] == '']
	limitCounter = 0
	if moreDebug:
		numberMissing = len(missingData.index)
		print(numberMissing,"RECORDS WITHOUT ATTRIBUTE DATA")

	for i in missingData.index:
		limitCounter += 1
		if limitCounter <= limit:
			thisURL = missingData.loc[i,'url']
			try:
				rawResponse = get(thisURL)
			except:
				print("Unable to get HTTP request from",thisURL)
				exit(1)

			try:
				html_soup = BeautifulSoup(rawResponse.text, 'html.parser')
			except:
				print("Unable to parse request as HTML for record",p)
				exit(2)

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
		else:
			# We have scraped data from our set limit; skip the rest
			# of the data set until next time so that we do not DoS craigslist.org
			break

	if debug:
		print(missingData.head())
	if moreDebug:
		print("\n")	
	return df

def purgeMissingData(df):
	goodData = df.loc[df['attributes'] != '']
	return goodData

# Convert BeautifulSoup results to apartment objects and then to a dataframe
def saveNewListings(apartments, fileName='data.parquet',mode='merge'):
	if mode == 'overwrite':
		postsID = []
		postsDate = []
		postsURL = []
		postsTitle = []
		postsRooms = []
		postsBathrooms = []
		postsArea = []
		postsHood = []
		postsPrice = []
		postsAttr = []
		postsLat = []
		postsLon = []
	if mode == 'merge':
		dfSaved = getDataFromDisk(fileName)
		postsID = dfSaved['pid'].tolist()
		postsDate = dfSaved['date'].tolist()
		postsHood = dfSaved['neighborhood'].tolist()
		postsTitle = dfSaved['title'].tolist()
		postsRooms = dfSaved['bedrooms'].tolist()
		postsBathrooms = dfSaved['bathrooms'].tolist()
		postsArea = dfSaved['sqft'].tolist()
		postsURL = dfSaved['url'].tolist()
		postsPrice = dfSaved['price'].tolist()
		postsAttr = dfSaved['attributes'].tolist()
		postsLat = dfSaved['latitude'].tolist()
		postsLon = dfSaved['longitude'].tolist()
		


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
		})

	dfNew.drop_duplicates(subset=['pid'],inplace=True, keep='last')
	dfNew.to_parquet(fileName)

	if debug:
		# If debugging, also save as csv for easy import and viewing
		[name,extension] = fileName.split('.')
		csvName = name+".csv"
		dfNew.to_csv(csvName)

	return dfNew

# Save dataframe
def saveDataFrame(df,key='df',fileName='data.parquet'):
	try:
		df.to_parquet(fileName)
	except:
		print("Error saving",key,"to",fileName)
		exit(1)

def main():
	global numberofListings
	global myURL
	global debug
	global moreDebug
	global fileName
	global writeMode

	# Allow command line customization of the global variables
	parser = ArgumentParser(prog='getLiveData.py',epilog='(C) 2020 by Dr. William Supplee and licensed open-source under GPU General License v3')
	parser.add_argument("-n", "--number",
		default=1200, type=int, metavar="NUMBER_OF_LISTINGS",dest="numberofListings", help="number of new cl listings to scrape [default: %(default)s]")
	parser.add_argument("-u", "--url",
		default="https://sfbay.craigslist.org/search/pen/apa?hasPic=1&availabilityMode=0&sale_date=all+dates",
		type=str, metavar="URL",dest="url",help="URL of the first craigslist page to begin collecting data from [default: %(default)s]")
	parser.add_argument("-f", "--filename",
        metavar="FILE", dest="fileName", default="data.parquet", help="use %(metavar)s to load and store data on disk [default: %(default)s]")
	parser.add_argument("-m", "--mode",
                  default="merge",
                  dest='mode', type=str,
                  help="write mode: merge or overwrite "
                       "the database with new entries [default: %(default)s]")
	parser.add_argument("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="print some output every step of the way for debugging purposes")
	parser.add_argument("-d", "--debug", action="store_true", dest="moreDebug", default=False, help="show additional debug messages you won't benefit from")
	args = parser.parse_args()
	
	if args.moreDebug:
		moreDebug=1
	if args.verbose:
		debug=1
	fileName = args.fileName
	if args.numberofListings < 0:
		parser.print_help()
		print("You can't specify a negative number of posts, dude!")
		exit(1)
	else:
		numberofListings = args.numberofListings
	myURL = args.url
	writeMode = args.mode

	if debug:
		for arg in vars(args):
			print(arg,"=",getattr(args,arg))

	posts=getPostData(n=numberofListings) # Merge the first 'n' new posts to our database
	#posts=getPostData(n=1200)

	# Turn each new post into a data object, to be converted to data frame
	apartments = []
	for p in posts:
		apartments.append(Apartment(p))
		if debug:
			print("\ncreated from SOURCE DATA:")
			print(p)

	# Save listings as data frame and to disk
	# The 'merge' mode will merge the new posts with previously saved posts into one table
	saveNewListings(apartments, fileName, mode=writeMode)
	topLevel=getDataFromDisk(fileName)
	topLevel=getMissingInformation(topLevel,limit=120)	
	topLevel.to_parquet('data.parquet')

	if debug:
		topLevel.to_csv('debug.csv')


	exit(0)

if __name__ == "__main__":
	main()
