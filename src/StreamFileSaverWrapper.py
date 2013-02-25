import datetime
import os
import sys

import FileLog
import FileSaver

class StreamFileSaverWrapper:
    """Create [INSERT INTO ...] for StreamingCralwer
    
    This class is a wrapper for FileSaver. The main purpose is to "change" the
    filename of tsv file right after the midnight. While the code is firstly
    initialized, an filter_words
    
    NOTE: in test mode, we use datetime.datetime.now().strftime('%Y_%m_%d_%H')
    """
    
    def __init__(self, input_filter_words, verbose_log=False):
                 
        #Setup log 
        FileLog.set_log_dir() 
        self.log_filename = "StreamFileSaverWrapper.log"
        self.verbose_log = verbose_log #suppress all debug log
        
        #Setup filenames (for storing tsv)
        self.data_path = "../data/tsv/" 
        if self.data_path [-1] != os.sep: #make sure we have a path separtor charcter 
            self.data_path  += os.sep
        if not os.path.exists(self.data_path): #make sure path exist
            self.log("Creating dir: " + self.data_path)
            os.makedirs(self.data_path )          
        
        #self.check_all_old_processes_are_dead = False #Set to true to check "old processes" status            

        self.filter_words = input_filter_words        
        self.today_str = StreamFileSaverWrapper.static_get_date_string() #'2011_03_09'
        
        ### Setup Worker Processes ###
        #For storing the obsolete worker processes
        self.obsolete_process_dict  = None 
        #Create a dictionary that holds all the worker processes.        
        self.current_process_dict = self.build_new_worker_process_dict()                
        #start all worker processes
        self.start_all_worker(self.current_process_dict)      
        
        
    @staticmethod
    def static_get_date_string():
        """Return current date in YYYY_mm_dd"""
        return datetime.datetime.now().strftime('%Y_%m_%d_%H') #'2011_03_09' add _%M for mins
     
    @staticmethod
    def ensure_escape_for_mysql(content):
        result = str(content).replace("\r", "").replace("\n", '\\n').replace("\t", "\\t")
        #.replace("\\", "\\\\")
        return result            
        
    @staticmethod
    def static_determine_filename(filter_words, today_str, action):
        """return tweeter_stream_data.2011_03_09.<action>.http_nyti_ms.tsv
        """                
        str_filter_part = filter_words.replace(" ", "_")
        file_name = "tweeter_stream_data."
        file_name += (today_str + ".")
        file_name += (str(action) + ".")
        file_name += (str(filter_words).replace(" ", "_") )
        file_name += (".tsv")
        return file_name

    @staticmethod
    def static_process_status(data):
        """This static method will be passed to the FileSaver
        """ 
        (status_or_status_user, target_table, filter_words) = data
        if str(target_table).lower().strip() == 'tweets':
            status = status_or_status_user
            (status, target_table, filter_words) = data
            return StreamFileSaverWrapper.static_status_to_tweet_tsv(status, filter_words)
        elif str(target_table).lower().strip() == 'users':
            status_user = status_or_status_user
            return StreamFileSaverWrapper.static_status_to_user_tsv(status_user)
        else:
            return None        
        
    @staticmethod
    def static_status_to_tweet_tsv(status, filter_words):
        """Extract data related to Tweets Table, and return a tab splitted string.

        Returns:
        line -- a tab splitted string
        None -- some exception happens
        """
        retweeted = False
        origin_user_id = ''
        origin_tweet_id = ''
        source = ''
        source_url = ''
        retweet_count = 0
                            
         #if it is a retweet, update the retweet infomation
        if hasattr(status,'retweeted_status'): 
            origin_user_id = str(status.retweeted_status.user.id)
            origin_tweet_id = str(status.retweeted_status.id)
            retweeted = True        
                    
        if hasattr(status, 'source'):
            source = status.source
            
        if hasattr(status, 'source_url'):
            source_url = status.source_url        
        
        if str(status.retweet_count) == '100+':
            status.retweet_count = 101
        try:
            line = ("\t".join([
                str(status.id_str), #tweet_id
                str(status.user.id), #user_id
                StreamFileSaverWrapper.ensure_escape_for_mysql(status.text.encode('utf8')), #content
                str(status.created_at), #created_at
                str(retweeted), #retweeted
                str(int(status.retweet_count)), #retweeted_count
                str(origin_user_id), #origin_user_id
                str(origin_tweet_id), #origin_tweet_id
                str(source.encode('utf8')), # source
                str(source_url), #source_url
                str(filter_words), #filter
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') #insert_timestamp
                # leave it blank: HASH                
                ]))
            
            return line + "\n"       
        except Exception as e:        
            FileLog.log("FileSaver.log", "FileSaver(pid:{0}): "
                        .format(str(os.getpid())) + str(e), 
                        exception_tb=sys.exc_traceback)
        return None
    
    def build_new_worker_process_dict(self):
        new_worker_dict = {
            "tweets": FileSaver.FileSaver(self.data_path +
                                         StreamFileSaverWrapper.static_determine_filename(
                                             self.filter_words, self.today_str, "tweet"),
                                         StreamFileSaverWrapper.static_process_status
                                         ),
            "users": FileSaver.FileSaver(self.data_path +
                                         StreamFileSaverWrapper.static_determine_filename(
                                             self.filter_words, self.today_str, "user"),
                                         StreamFileSaverWrapper.static_process_status
                                         ) 
            # "static_process_status" is a delegate function for processing when "save" is called
        } 
        for (action, worker) in new_worker_dict.items():
            self.log("From now on, the [{0}] write to this file: {1}".format(
                str(action), str(worker.get_current_filename())))
        return new_worker_dict 
    
    def start_all_worker(self, worker_dict):
        """
        worker_dict = {"action", FileSaver Obj}
        """
        #start all worker processes
        for (action, file_saver) in worker_dict.items():
            file_saver.start()
            self.log("{0}: Save to this file: {1}".format(
                str(action), 
                file_saver.get_current_filename()))        
        
    
    def check_new_day(self):
        """Move the current processes to the old processes, and start new processes if needed"""
        new_date_str = StreamFileSaverWrapper.static_get_date_string()
        if new_date_str <> self.today_str: # a new day is coming
            self.log("A new day comes. Move the current worker processes to the old worker processes, and ask them to stop.")
            #dealing with obsolete process
            if self.obsolete_process_dict: #first time
                pass #do nothing
            else:
                #check status of obsolete processes
                if self.obsolete_process_dict:
                    for (action, worker_process) in self.obsolete_process_dict.items():
                        if worker_process.process_is_alive():
                            self.log("Error: The old process is still alive but we force delete it: " 
                                     + str(action) + " -- " 
                                     + str(worker_process.get_current_filename()))
            
            #Move current worker processes to "obsolete" dict            
            self.obsolete_process_dict = self.current_process_dict                
            self.log("Move current workers to the obsolete team...done")                                    
            
            #Dealing with new worker processes
            self.log("Create new workers.")
            self.today_str = new_date_str
            self.current_process_dict = self.build_new_worker_process_dict()
            self.start_all_worker(self.current_process_dict)
            
            #Send out a STOP signal to all obosolete workers
            self.log("Tell obosolete workers to stop and start to clean up their queues.")
            for (action, worker_process) in self.obsolete_process_dict.items():
                worker_process.stop() #worker_process is a FileSaver  
                
            return True
        else:    
            pass #do nothing, the date does not change
        return False
    
    def updateStreamingTweet(self, status, filter_words):          
        """ Save data into Tweets table.
        
        Copy from DB connection 
        """
        if status == None:
            self.log("Error: the status object is None. Skip the updateStreamingTweet()")
            return        

        try:
            #check the status of the current process (a "new" day)
            self.check_new_day() #new day arrived    
            #for debug
            if(self.verbose_log):
                self.show_all_workers()#TODO: remove it
            
            #self.log("Write(Tweet): " + sql)
            tweet_worker = self.current_process_dict.get("tweets")
            if tweet_worker:
                tweet_worker.save((status, 'tweets', filter_words)) #save status for the Tweets table
                #(status_or_status_user, target_table, filter_words)
            else:
                self.log("Error: there is no tweets action in current_process_dict.") 
            return True
        
        except Exception as e:        
            self.log(str(e), tb=sys.exc_traceback)
            #sent a stop signal to worker processes and wait.
            for (action, worker) in self.current_process_dict:
                self.log("Wait current worker to stop ... ({0})".format(str(action)))
                worker.waitstop()
        return False 


    @staticmethod
    def static_status_to_user_tsv(status_user):
        #insert a new user

        location = ""
        utc_offset = ""
        time_zone = ""
            
        if hasattr(status_user, 'time_zone'):
            if status_user.time_zone != None:
                time_zone = status_user.time_zone
            
        if hasattr(status_user, 'utc_offset'):
            if status_user.utc_offset != None:
                utc_offset = status_user.utc_offset
        
        if hasattr(status_user, 'location'):
            if status_user.location != None:
                location = status_user.location.encode('utf8')
            
        try:        
            line = "\t".join([
                       str(status_user.id_str), #user_id
                       StreamFileSaverWrapper.ensure_escape_for_mysql(str(status_user.screen_name)), #screen_id
                       str(status_user.statuses_count), #tweets_count
                       str(status_user.created_at), #created_at
                       str(status_user.followers_count), #followers_count
                       str(status_user.friends_count), #friends_count
                       StreamFileSaverWrapper.ensure_escape_for_mysql(str(location)), #location
                       str(status_user.listed_count), #listed_count
                       str(time_zone), #time_zone
                       str(utc_offset) #utc_offset
                       ]
            )
            return line + "\n"
        except Exception as e:        
            FileLog.log("FileSaver.log", "FileSaver(pid:{0}): "
                        .format(str(os.getpid())) + str(e), 
                        exception_tb=sys.exc_traceback)
        return None           
    
    def updateUser(self, status_user):  
        """Save data into Users table.
        
        Copy from DB connection .
        """
            
        try:                    
            #self.log("Write(User):" + sql)
            tweet_worker = self.current_process_dict.get("users")
            if tweet_worker:
                tweet_worker.save((status_user, 'users', None)) #save status for the Tweets table
            else:
                self.log("Error: there is no users action in current_process_dict.")            
            return True
        
        except Exception as e:        
            self.log(str(e), tb=sys.exc_traceback)
            #sent a stop signal to worker processes and wait.
            for (action, worker) in self.current_process_dict.items():
                self.log("Wait current worker to stop ... ({0})".format(str(action)))
                worker.waitstop()
        return False
    
    def show_all_workers(self):
        """This is just for debug"""
        self.log("#" * 20)
        self.log("Worker in the obsolete team (current pid: {0}) as follows:".format(str(os.getpid())))
        if not self.obsolete_process_dict:
            self.log("None")
        else:
            for (action, worker) in self.obsolete_process_dict.items():
                self.log("(pid:{0}): {1} --> {2}".format(
                    str(worker.get_pid()),
                    str(action),
                    str(worker.get_current_filename())))
        self.log("-" * 20)
        if not self.current_process_dict:
            self.log("None")
        else:
            for (action, worker) in self.current_process_dict.items():
                self.log("(pid:{0}): {1} --> {2}".format(
                    str(worker.get_pid()),
                    str(action),
                    str(worker.get_current_filename())))
        self.log("#" * 20)
        
    def log(self, message, tb=None):
        """Log error message and its traceback(optional)"""
        FileLog.log(self.log_filename, "StreamFileSaverWrapper: " + str(message), exception_tb=tb)
    
