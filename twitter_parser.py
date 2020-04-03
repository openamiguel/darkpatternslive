##### --------------------------------------------------------------------------------------- #####
##### Implements a scraper to collect Twitter posts using the TwitterAPI library.       	  #####
##### Collects Tweets using various filters, which may be relevant to "dark patterns".	      #####
#####																						  #####
##### Author: Miguel Opena																	  #####
##### --------------------------------------------------------------------------------------- #####

from TwitterAPI import TwitterAPI

import pandas as pd

import logging

##### --------------------------------------------------------------------------------------- #####

""" File path to Reddit credentials """
TWITTER_CREDENTIALS_FPATH = "/Users/openamiguel/Documents/TWITTER_API_CREDENTIALS.txt"

""" File path to data storage folder """
OUTPUT_FOLDER = "/Users/openamiguel/Desktop/Twitter/"

""" The main short URL for all tweets """
MAIN_URL = "https://t.co/"

""" Maximum number of tweets to call in an API request """
MAX_TWEETS = 100

""" Gets some keys relevant to the data output """
KEYS = ['created_at', 'idstr', 'text', 'truncated']

""" Gets sub-keys of the metadata key """
KEYS_META = ['iso_language_code', 'result_type']

""" Full list of data columns """
COLUMN_LIST = KEYS + KEYS_META + ['url']

##### --------------------------------------------------------------------------------------- #####

def get_credentialed_parser(filepath: str=TWITTER_CREDENTIALS_FPATH): 
	""" Retrieves Twitter credentials from local file and returns parser object """
	TWITTER_CREDENTIALS_FILE = open(twitter_credentials_fpath, 'r')
	TWITTER_CREDENTIALS = TWITTER_CREDENTIALS_FILE.read().split("\n")
	# Access credentials in file
	TWITTER_APP_KEY = twitter_credentials[0]
	TWITTER_APP_KEY_SECRET = twitter_credentials[1]
	TWITTER_ACCESS_TOKEN = twitter_credentials[2]
	TWITTER_ACCESS_TOKEN_SECRET = twitter_credentials[3]
	# Plug credentials in
	twitter = TwitterAPI(TWITTER_APP_KEY, 
							TWITTER_APP_KEY_SECRET, 
							TWITTER_ACCESS_TOKEN, 
							TWITTER_ACCESS_TOKEN_SECRET)
	return twitter

##### --------------------------------------------------------------------------------------- #####

def get_entry(item, key) -> str:
	""" Return the entry of an item at a certain key """
	entry = str(item[k])
	entry = entry.replace('\n', '')
	entry = entry.replace('\t', '')

	return entry

##### --------------------------------------------------------------------------------------- #####

def process_tweet(tweet) -> dict: 
	""" Return a dictionary encoding on a tweet """
	rowdict = {key: '' for key in KEYS + KEYS_META}
	# Parses tweet for certain keys
	for key in KEYS: 
		if key in tweet: 
			rowdict[key] = get_entry(item, key)
	# Parses tweet for certain metadata-relevant keys
	if 'metadata' in item: 
		for key in KEYS_META: 
			if key in tweet: 
				rowdict[key] = get_entry(item['metadata'], key)
	# Adds URL to the data
	url = ''
	if 'text' in item: 
		url_idx = item['text'].rfind(MAIN_URL)
		url = item['text'][url_idx:]
	rowdict['url'] = url

	return rowdict

##### --------------------------------------------------------------------------------------- #####

def retrieve_tweets(queries: list, counts: list) -> pd.DataFrame: 
	""" Return a Pandas dataframe of the counts[i] most recent tweets matching query queries[i] """
	data_rowslist = []
	outfile = open(OUTPUT_FOLDER + "DATA_partial.txt", 'a')
	for query, cnt in zip(queries, counts): 
		request = api.request("search/tweets", {'q': query, 'count': cnt})
		for tweet in request: 
			rowdict = process_tweet(tweet)
			# Print submission ID
			infostr = f"Current ID: {rowdict['idstr']}"
			print(infostr)
			logging.debug(infostr)
			# Writes data to list and output file
			data_rowslist.append(rowdict)
			rowlist = [str(val) for val in rowdict.values()]
			outfile.write('\t'.join(rowlist) + '\n')
		# Print remaining quota
		quotastr = f"Quota remaining: {req.get_quota()}"
		print(quotastr)
		logging.info(quotastr)
	outfile.close()
	# Saves list to Pandas DataFrame
	data = pd.DataFrame(data_rowslist, columns=COLUMN_LIST)
	return data

##### --------------------------------------------------------------------------------------- #####

if __name__ == "__main__":
	""" Runs through provided parsing parameters """
	log_path = OUTPUT_FOLDER + "log.txt"
	logging.basicConfig(level=logging.DEBUG, filename=log_path, filemode="w+", \
						format="%(asctime)-15s %(levelname)-8s %(message)s")
	twitter = get_credentialed_parser()
	# Hard-coded queries and counts
	queries = ['#darkpattern', '#darkpatterns', '@darkpatterns']
	counts = [MAX_TWEETS] * len(queries)
	print("=================================================")
	logging.info("=================================================")
	# Add data
	data = retrieve_tweets(queries: list, counts: list)
	print("=================================================")
	logging.info("=================================================")
	# Save data to file
	data.to_csv(OUTPUT_FOLDER + "DATA.txt", sep='\t', index=False)

##### --------------------------------------------------------------------------------------- #####