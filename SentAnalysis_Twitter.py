from textblob import TextBlob as tb
import tweepy
import pandas as pd
import numpy as np
import re as regex

class SentAnalysis_Twitter:



    def main():
        consumerKey = YOUR_KEY
        consumerSecret = YOUR_SECRECT
        search=QUERY
        resultDF = pd.Dataframe()
        fileName=FILE_NAME+PATH
        delimiter='|'
        
        client = twitterConn(consumerKey, consumerSecret)
        resultDF = tweetExtractor(client, search)
        createCSV(resultDF, fileName, delimiter)

def twitterConn(consumer_key, consumer_secret):

    auth = tweepy.OAuthHandler(consumer_key=consumer_key, consumer_secret=consumer_secret)
    api = tweepy.API(auth)

    return api

def tweetExtractor (api, search):
    results = []
    
    #%23NBC as #NBC
    #45.5191385,-73.6103499 is montreal geocode
    for tweet in tweepy.Cursor(api.search, q=search, result_type='recent', geocode='45.5191385,-73.6103499,10mi').items(50):
        results.append(tweet)
        tweet.text = regex.sub(r"http.?://[^\s]+[\s]?", " ", tweet.text) #Remove urls
        tweet.text = regex.sub('[^A-Za-z@#]+', " ", tweet.text)            #Leaves only alphanumeric characters
        tweet.text = str.lower(tweet.text)
        
    resultDS = pd.DataFrame()
    resultDS['userName'] = [tweet.user.name for tweet in results]
    resultDS['tweetText'] = [str(tb(tweet.text).correct()) for tweet in results]
    resultDS['tweetRetweetCt'] = [tweet.retweet_count for tweet in results]
    resultDS['tweetCreated'] = [tweet.created_at for tweet in results]
    resultDS['userLocation'] = [tweet.user.location for tweet in results]
    resultDS['geo'] = [tweet.geo for tweet in results]
    resultDS['place'] = [tweet.place for tweet in results]
    resultDS['coordinates'] = [tweet.coordinates for tweet in results]
    resultDS['userTimezone'] = [tweet.user.time_zone for tweet in results]
    resultDS['sentPolarity'] = [tb(tweet.text).correct().sentiment.polarity for tweet in results]
    resultDS['sentPolarity'] = np.where(resultDS['sentPolarity']<0, 'Negative',np.where(resultDS['sentPolarity']>0, 'Positive', 'Neutral'))
    return resultDS

def createCSV(resultDS, file_name, delimiter):
    resultDS.to_csv(file_name, sep=delimiter, header=True, index = False)
