'''
CrawlerListener class extends from StreamListener, an abstract class of 
tweepy library. 

StreamingCrawler is a wrapper for starting CrawlerListener. The major purpose
of the StreamingCrawer is to set up the filter (keywords that should be 
returned by tweeter).

3/6/2012: Chucheng - I remove all db codes and clean the codes (downloading)
'''
from __future__ import with_statement
import datetime
import time
import os
import sys
import signal
import unittest
from tweepy.api import API
from tweepy.models import Status
from tweepy.utils import import_simplejson
from tweepy.auth import BasicAuthHandler

import tweepy
import StreamFileSaverWrapper
import FileLog
json = import_simplejson()

import threading
#import streaming #TODO: I should use my streaming, but let's use 1.8 version now 3/6/2012
from streaming import StreamListener, Stream # my version of tweepy.StreamListener
#from tweepy.streaming import StreamListener, Stream

import traceback


#(The original streaming.py offered by tweepy will crashed after few days)
import TweetConfiguration

#gSave_to_DB" Set it to True if you would like to store to DB (old way)
#gSave_to_file" Set it to True if you would like to store to files as tsv
#gSave_raw_json: Set it to True if you would like to store raw json
(gSave_to_DB, gSave_raw_json, gSave_to_file) = TweetConfiguration.getStreamingCrawlerSetting()

class CrawlerListener (StreamListener): 
      
    num_received_tweets = 0 #global variable : count of received tweets
    
    
    def __init__(self, filter ):
        self.running = True #Set to FALSE will stop a listener
        self.on_data_running = False # this value is true if we are processing data
        self.pid = str(os.getpid()) 
        self.log_file = "StreamingCrawler.log"
        
        #database module
        if gSave_to_DB:
            #self.dbconn = DBConnection.DBConnection(log=self.log) 
            self.log("not support: storing to DB")
        
        #FileSaver
        self.fileconn = StreamFileSaverWrapper.StreamFileSaverWrapper(filter[0]) # ['http nyti ms']
        
        StreamListener.__init__(self) 
        self.filter = filter                

        now_str = datetime.datetime.now().strftime('%m%d%Y_%H%M%S')
        self.log_file = "CrawlerListener." + now_str + "." + "pid_{0}".format(self.pid) + ".log"
        FileLog.set_log_dir() #check the log dir, and create it if necessary
    
    def on_data(self, data):                
        '''Parse raw data from twitter and pass the status object to on_status()
        
        Call when raw data is passed from twitter.        
        If this function return False, it stop listening to the streamining.
        
        gSave_raw_json: if true, write json raw text to the ../json/
                        Set it to true only if you would like to debug.
                        
                           
        '''
        
        try:
            self.on_data_running = True
            self.log("Get raw data from Twitter", screen_only=True)
            
            if gSave_raw_json:
                ### save the json into disk ###
                parsed_data = tweepy.utils.import_simplejson().loads(data)
                
                if "id" not in parsed_data.keys():  #may return {"limit":{"track":73}} or {delete...}, ignore this data
                    return True #chucheng, this line is equal to check if 'delete'/;limit' in data
                    
                folder_name = parsed_data["id"]%1000
                
                
                try:
                    if not os.path.exists("../json/"+str(folder_name)):
                        os.makedirs("../json/"+str(folder_name))
                except OSError as ose:
                    self.log("OS ERROR")
                    pass
                
                filename = "../json/"+str(folder_name) + "/" + str(parsed_data["id"]) + ".json" 
                #print filename # for debug
                output = open(filename,"w")
                output.write(data)
                output.write('\n')
                output.close()
                ### done ###
                        
            # Chucheng 4/25/2011:
            #   We must override the method, because the original one might             
            #   return false, cause a stop of the listerner.
            #   In short, you cannot simply call:
            #       tweepy.StreamListener.on_data(self, data) 
            if 'in_reply_to_status_id' in data:
                status = Status.parse(self.api, json.loads(data))
                if self.on_status(status) is False: #Trigger on_status now!!
                    self.log('in_reply_to_status_id in data: on_status() returns False. (this line should never be reached)')
            else:
                pass #do nothing, the data we get is not what we need.
                    
            """ These lines should never be triggered in that we check :
                
            
            elif 'delete' in data:
                delete = json.loads(data)['delete']['status']
                if self.on_delete(delete['id'], delete['user_id']) is False:
                    self.log('delete in data: a delete notice arrives for a status')
            elif 'limit' in data:
                if self.on_limit(json.loads(data)['limit']['track']) is False:
                    self.log('limit in data: a limitation notice arrvies')       
            """
            
            self.on_data_running = False # This variable signal whether 
                                         # we are in the middle of processing data.
        
            if self.running == False: # see: StreamingCrawler.stop_listner()
                return False #stop the listener while catching a SIGTERM
            
        except Exception as e:
            self.on_data_running = False            
            self.log("Error:" + str(e), sys.exc_traceback)

        return True
        
    
    def on_status( self, status):
        '''The status object is a tweepy object.
        
        This function is basically trigger by on_data()
        call when a tweet coming after parsing by tweepy AND
        the data from trigger is NOT "delete" or "limit"
        '''
        
        self.num_received_tweets += 1  # count of received tweets
        #briefly print out the received tweet in the console. 
        #For more information, please refer to Status and User object at StatusObject.txt        
        self.log('-' * 20, screen_only=True)
        self.log(",".join([str(self.num_received_tweets), 
                           str(self.filter), 
                           str(status.id_str), 
                           str(status.user.id_str)]))
        #self.log("tweet text: " + str(status.text))
        
        #Save status object
        try:
            self.saveTweet(status)
            #self.log( "fake saveTweet") #for debug
        except Exception as e:
            self.log("saveTweet(status) throws the following exception:"+str(e), sys.exc_traceback)
            return True    
        return True
               
    def on_delete(self, status_id, user_id):
        self.log("on_delete: (status_id)" + str(status_id) + " (user_id)" + str(user_id))
        """Called when a delete notice arrives for a status"""
        return True

    def on_limit(self, track):
        self.log("on_limit: " + str(track))
        """Called when a limitation notice arrvies"""
        return True
    
    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        self.log("on_error: " + str(status_code))
        return True
    
    def on_timeout(self):
        self.log("connection time out")
        return True
    
    #output the tweets to DB or File
    def saveTweet(self, status):        
        #save tweets
        if gSave_to_DB:            
            self.log("not support: storing to DB")
            #self.log("write to db.")
            #self.dbconn.updateStreamingTweet(status, self.filter)        
        elif gSave_to_file:
            self.log("write to file.", screen_only=True)
            self.fileconn.updateStreamingTweet(status, self.filter[0]) #['http nyti ms']
        self.log("save tweet successfully.", screen_only=True)
        
        #save the user who send the tweet
        #print "$$$$$$$$ > user", status.user.__dict__
        if gSave_to_DB:
            self.log("not support: storing to DB")
#            self.log("write to db.")
#            self.dbconn.updateUser(status.user)
        elif gSave_to_file:
            self.log("write to file.", screen_only=True)
            self.fileconn.updateUser(status.user)
        self.log("save user successfully.", screen_only=True)
        
    
    def log(self, msg, tb=None, screen_only=False):
        if screen_only: #do not save to log file, only print it to screen
            FileLog.log(None, "StreamingCrawler:" + msg, exception_tb=tb)   
        else:
            FileLog.log(self.log_file, "StreamingCrawler:" + msg, exception_tb=tb)   
    
    

#This class is used to set up the filter and listener
class StreamingCrawler:
    def __init__(self, keywords, username, password):
        self.pid = str(os.getpid())
        ### Log ###
        self.log_file = "StreamingCrawler.log"
        FileLog.set_log_dir() #check the log dir, and create it if necessary
        
        ### Run ###
        self.key = keywords #keyword for filtering                
        self.listener = CrawlerListener(keywords)
        self.log("(" + self.pid + ") " +
                 "Start monitoring streaming... keywords: " + str(keywords))
        
        auth = BasicAuthHandler(username, password)
        self.stream = Stream(auth, self.listener, timeout=None, retry_count = sys.maxint); #retry_time = 10.0        

         
    #filter by keyword
    def keyword_filter(self):  
        try:
            self.stream.filter(track=self.key)
        except KeyboardInterrupt as kb:
            self.stop_listner() 
            raise kb
    
    def handle_SIGTERM(self, signum, frame): # handle terminate signal
        self.log("(" + self.pid + ") " +
                 "TERMINATED SIGNAL is triggerd... this process is going to terminate")
        self.stop_listner()
       
    def stop_listner(self):
        self.log("(" + self.pid + ") " +
                 "Tell self.listener to stop working... ")        
        self.listener.running = False # This line will cause on_data to return False
        
        while(self.listener.on_data_running):
            self.log("Wait... on_data_running is still true...")
            time.sleep(1)
            pass #in the middel of doing something
        self.log("(" + self.pid + ") " +
                 "Done: Stop monitoring streaming... keywords: " + str(self.key)) 
        
    def log(self, msg, tb=None):
        FileLog.log(self.log_file, msg, exception_tb=tb)       
    



if __name__ == "__main__":
    
    keywords_dict = {
        "wsj": ["http on wsj com"],
        "lat": ["http lat ms"],
        "nyti": ["http nyti ms"],
        "nydn": ["http nydn us"],
        "test": ["http"]
    }
    
    
    if len(sys.argv) == 4:
        # run customized parameters
        username = sys.argv[1]
        password = sys.argv[2]
        keywords_tag = sys.argv[3]
        keywords_str = keywords_dict.get(keywords_tag, None)
        if keywords_str == None:
            print "the input tag word [{0}] finds no match.".format(keywords_tag)
        #print("Streaming Crawler is running with these filter keywords: " + str(keywords_str))
                
    else:
        (username, password, _ , _) = TweetConfiguration.getProcessManagerSetting()
        print "monitoring streaming with username: " + username + " ; password: " + password
        #username = "your_user_id"
        #password = ""yourpassword""
        keywords_str = ["http nyti ms"]
        
        
    #print("Starting StreamingCrawler...")
    crawler1 = StreamingCrawler(keywords_str, username, password)   
    ### Register SIGTERM handler ###
    signal.signal(signal.SIGTERM, crawler1.handle_SIGTERM)
    crawler1.keyword_filter()
 
    
    '''
    accounts = [("your_user_id","yourpassword"),
               ]

    keywords = [["http on wsj com"],                
                ["http lat ms"],
                 ["http nyti ms"],
                 ["http nydn us"]]
    '''
    
