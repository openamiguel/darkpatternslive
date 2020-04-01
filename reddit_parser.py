##### --------------------------------------------------------------------------------------- #####
##### Implements a scraper to collect Reddit posts using the Praw library.					#####
##### Collects posts from /r/assholedesign that may or may not pertain to "dark patterns".	#####
#####																						 #####
##### Author: Miguel Opena																	#####
##### --------------------------------------------------------------------------------------- #####

import praw
import urllib.request
import logging

# from textblob import TextBlob

import pandas as pd

from datetime import datetime
import time
import os.path

##### --------------------------------------------------------------------------------------- #####

""" File path to Reddit credentials """
REDDIT_CREDENTIALS_FPATH = "/Users/openamiguel/Documents/REDDIT_API_CREDENTIALS.txt"

""" File path to image storage folder """
OUTPUT_FOLDER = "/Users/openamiguel/Desktop/Reddit/"

""" Name of the subreddit /r/assholedesign """
SUBREDDIT_NAME = "assholedesign"

""" Permissible flairs to store data from subreddit """
ALLOWED_FLAIRS = [None, "Bad Unsubscribe Function", "Clickshaming", "Dark Pattern", \
				  "Bait and Switch", "Loaded Question"]

""" Standard identifiers of images """
IMAGE_TAGS = ["i.redd.it", ".png", ".jpg", ".gif"]

""" Standard identifiers of videos """
VIDEO_TAGS = ["v.redd.it", ".mp4", ".mov"]

""" List of columns in output data """
COLUMN_LIST = ["ID", "Title", "Title_Language", "Media_Type", "Media_URL", "Date_Time", "Flair", \
			   "Local_File_Name", "Is_OC", "Score", "Upvote_Ratio"]

""" Praw library upper bound on number of posts """
UPPER_BOUND = 1000

""" List of parameters to control parsing """
PARSING_PARAMS = [("all", UPPER_BOUND), ("year", UPPER_BOUND), ("month", UPPER_BOUND), \
				  ("week", UPPER_BOUND), ("day", UPPER_BOUND), ("hour", UPPER_BOUND)]
# PARSING_PARAMS = [("day", UPPER_BOUND)]

##### --------------------------------------------------------------------------------------- #####

def get_credentialed_parser(filepath: str=REDDIT_CREDENTIALS_FPATH): 
	""" Retrieves Reddit credentials from local file and returns parser object """
	REDDIT_CREDENTIALS_FILE = open(filepath, 'r')
	REDDIT_CREDENTIALS = REDDIT_CREDENTIALS_FILE.read().split('\n')
	# Access credentials in file
	REDDIT_PERSONAL_USE_SCRIPT = REDDIT_CREDENTIALS[0]
	REDDIT_SECRET_KEY = REDDIT_CREDENTIALS[1]
	REDDIT_APP_NAME = REDDIT_CREDENTIALS[2]
	REDDIT_USER_NAME = REDDIT_CREDENTIALS[3]
	REDDIT_LOGIN_PASSWORD = REDDIT_CREDENTIALS[4]
	# Plug credentials in
	reddit = praw.Reddit(client_id=REDDIT_PERSONAL_USE_SCRIPT, \
					 client_secret=REDDIT_SECRET_KEY, \
					 user_agent=REDDIT_APP_NAME, \
					 username=REDDIT_USER_NAME, \
					 password=REDDIT_LOGIN_PASSWORD)
	return reddit

##### --------------------------------------------------------------------------------------- #####

def get_time_from_unix(timestamp: int): 
	""" Return date & time given a Unix timestamp """
	return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

##### --------------------------------------------------------------------------------------- #####

def retrieve_image(submission_url, submission_localname): 
	""" Retrieve a file from the Internet and store """
	urllib.request.urlretrieve(submission_url, submission_localname)

##### --------------------------------------------------------------------------------------- #####

def process_submission(submission) -> dict: 
	""" Return a dictionary encoding data on a submission """
	# Removes all posts without URLs (i.e. posts without visual media)
	if submission.is_self: return {0: -1}
	# Removes all posts without the proper flairs
	if submission.link_flair_text not in ALLOWED_FLAIRS: return {0: -1}
	# Removes all posts with low score (might not be "asshole design")
	if submission.upvote_ratio < 0.6: return {0: -1}
	# Determines if the title is English
	# title_obj = TextBlob(submission.title)
	# title_lang = title_obj.detect_language()
	# if title_lang != "en": return {0: -1}
	# Parses date & time
	submission_datetime = get_time_from_unix(int(float(submission.created_utc)))
	# Decides if media is image or video
	submission_type = "Other"
	submission_url = submission.url
	if any([img_tag in submission_url for img_tag in IMAGE_TAGS]): submission_type = "Image"
	elif any([vid_tag in submission_url for vid_tag in VIDEO_TAGS]): submission_type = "Video"
	elif "imgur.com" in submission_url: submission_type = "Imgur_Non_Image"
	# Assigns the media a file location on local machine
	submission_localname = OUTPUT_FOLDER + submission.id + "." + submission_url.split('.')[-1]
	# Retrieves images to local machine
	if submission_type == "Image" and not os.path.isfile(submission_localname): 
		retrieve_image(submission_url, submission_localname)
	else: submission_localname = "NA"
	# Saves all the data
	rowdict = {}
	rowdict["ID"] = submission.id
	rowdict["Title"] = submission.title.replace('\t', '')
	rowdict["Title_Language"] = "en" # title_lang
	rowdict["Media_Type"] = submission_type
	rowdict["Media_URL"] = submission_url
	rowdict["Date_Time"] = submission_datetime
	flair = submission.link_flair_text
	if flair != None: flair = flair.replace('\t', '')
	rowdict["Flair"] = flair
	rowdict["Local_File_Name"] = '/'.join(submission_localname.split('/')[3:])
	rowdict["Is_OC"] = submission.is_original_content
	rowdict["Score"] = submission.score
	rowdict["Upvote_Ratio"] = submission.upvote_ratio

	return rowdict

##### --------------------------------------------------------------------------------------- #####

def retrieve_top(subreddit, timelabel:str="day", limit:int=UPPER_BOUND) -> pd.DataFrame:
	""" Return a DataFrame of the N top submissions over a given time horizon """
	top_submissions = subreddit.top(timelabel, limit=limit)
	data_rowslist = []
	outfile = open(OUTPUT_FOLDER + "DATA_partial.txt", 'a')
	for submission in top_submissions:
		# Process a submission with timer code
		starttime = time.time()
		rowdict = process_submission(submission)
		endtime = time.time()
		if 0 in rowdict.keys() and rowdict[0] == -1: continue
		# Print submission ID
		infostr = "Current ID: " + rowdict["ID"]
		timestr = f"Time elapsed: {(endtime - starttime) / 1000:.03f} sec"
		print(infostr + '\t' + timestr)
		logging.debug(infostr)
		logging.debug(timestr)
		# Writes data to list and output file
		data_rowslist.append(rowdict)
		rowlist = [str(val) for val in rowdict.values()]
		outfile.write('\t'.join(rowlist) + '\n')
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
	reddit = get_credentialed_parser()
	subreddit = reddit.subreddit('assholedesign')
	alldata = pd.DataFrame([], columns=COLUMN_LIST)
	for paramtup in PARSING_PARAMS: 
		timelabel = paramtup[0]
		limit = paramtup[1]
		trialstr = f"Retrieving up to the top {limit} posts across {timelabel}"
		print("=================================================")
		print(trialstr)
		logging.info("=================================================")
		logging.info(trialstr)
		# Add data and drop submissions with duplicate ID
		data = retrieve_top(subreddit, timelabel=timelabel, limit=limit)
		alldata = pd.concat([alldata, data], axis=0)
		alldata.drop_duplicates(subset="ID", inplace=True)
	# Save all data to a file
	print("=================================================")
	logging.info("=================================================")
	alldata.to_csv(OUTPUT_FOLDER + "DATA.txt", sep='\t', index=False)

##### --------------------------------------------------------------------------------------- #####