"""This is a modified version by Chu-Cheng at 3/6/2012.
I disable the "stop" when an exception is araised.
"""

# Tweepy
# Copyright 2009-2010 Joshua Roesslein
# See LICENSE for details.

import httplib
from socket import timeout
from threading import Thread
from time import sleep
import urllib

from tweepy.models import Status
from tweepy.api import API
from tweepy.error import TweepError

from tweepy.utils import import_simplejson
json = import_simplejson()

STREAM_VERSION = 1

#chucheng:
import sys
import traceback
import FileLog
import os

class StreamListener(object):

    def __init__(self, api=None):
        self.api = api or API()
        self.log_file = "TweepyStreaming.log"
        self.pid = str(os.getpid())

    def on_data(self, data):
        """Called when raw data is received from connection.

        Override this method if you wish to manually handle
        the stream data. Return False to stop stream and close connection.
        """

        if 'in_reply_to_status_id' in data:
            status = Status.parse(self.api, json.loads(data))
            if self.on_status(status) is False:
                return False
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False

    def on_status(self, status):
        """Called when a new status arrives"""
        return

    def on_delete(self, status_id, user_id):
        """Called when a delete notice arrives for a status"""
        return

    def on_limit(self, track):
        """Called when a limitation notice arrvies"""
        return

    def on_error(self, status_code):
        """Called when a non-200 status code is returned"""
        return False

    def on_timeout(self):
        """Called when stream connection times out"""
        return
    
    def log(self, msg, tb=None): #for future debug, not called in this class yet
        FileLog.log(self.log_file, "(pid:{0})".format(self.pid) + str(msg), exception_tb=tb)   


class Stream(object):

    host = 'stream.twitter.com'

    def __init__(self, auth, listener, **options):
        self.auth = auth
        self.listener = listener
        self.running = False
        self.timeout = options.get("timeout", 300.0)
        self.retry_count = options.get("retry_count")
        self.retry_time = options.get("retry_time", 10.0)
        self.snooze_time = options.get("snooze_time",  5.0)
        self.buffer_size = options.get("buffer_size",  1500)
        if options.get("secure", True):
            self.scheme = "https"
        else:
            self.scheme = "http"

        self.api = API()
        self.headers = options.get("headers") or {}
        self.parameters = None
        self.body = None
        self.log_file = "TweepyStreaming.log"
        self.log("You are running Chu-Cheng's version of streaming.py") #log        

    def _run(self):
        # Authenticate
        url = "%s://%s%s" % (self.scheme, self.host, self.url)

        # Connect and process the stream
        error_counter = 0
        conn = None
        exception = None
        
        while self.running:
            
            if self.retry_count is not None and error_counter > self.retry_count:
                #Chucheng 3/16/2012 never break
                pass
                """
                # quit if error count greater than retry count
                break
                """
            try:
                if self.scheme == "http":
                    conn = httplib.HTTPConnection(self.host)
                else:
                    conn = httplib.HTTPSConnection(self.host)
                self.auth.apply_auth(url, 'POST', self.headers, self.parameters)
                conn.connect()
                conn.sock.settimeout(self.timeout)
                conn.request('POST', self.url, self.body, headers=self.headers)
                resp = conn.getresponse()
                if resp.status != 200:
                    if self.listener.on_error(resp.status) is False:
                        break
                    error_counter += 1
                    sleep(self.retry_time)
                else:
                    error_counter = 0
                    self._read_loop(resp)
            except timeout:
                if self.listener.on_timeout() == False:
                    break
                if self.running is False:
                    break
                conn.close()
                sleep(self.snooze_time)
            
            except Exception as e:
                # any other exception is fatal, so kill loop
                if self.retry_count > 999999: #special case
                    self.log("The self.retry count is larger than 999999," + 
                             " and thus we will never stop even we see an exception. ({0})"
                             .format(str(e)), tb=sys.exc_traceback)  #log the exception                      
                    self.log("Sleep {0} seconds...".format(str(self.retry_time)))
                    sleep(self.retry_time)
                    self.log("And now retrying...")
                    
                    pass #do nothing
                else:
                    break            

        # cleanup
        self.log("You should never be here...(chucheng), this means that self.running is going to be stopped.")    
        self.running = False
        if conn:
            conn.close()

        if exception:            
            self.log("We see an exception at the end of the while loop ({0})"
                     .format(str(exception)), tb=sys.exc_traceback)  #log the exception            
            #Chucheng, do not raise exception
            #raise            

    def _read_loop(self, resp):
        """disable 1.8
        while self.running:
            if resp.isclosed():
                break

            # read length
            data = ''
            while True:
                c = resp.read(1)
                if c == '\n':
                    break
                data += c
            data = data.strip()

            # read data and pass into listener
            if self.listener.on_data(data) is False:
                self.running = False
        """
        #1.7.1 by chucheng
        data = ''
        while self.running:
            if resp.isclosed():
                break

            # read length
            length = ''
            while True:
                try:
                    c = resp.read(1)
                except httplib.IncompleteRead as e:
                    self.log(str(e), tb=sys.exc_traceback) #for debug purpose
                    length = ''
                    break 
                if c == '\n':
                    break
                length += c
            length = length.strip()
            if length.isdigit():
                length = int(length)
            else:
                continue

            # read data and pass into listener
            try:
                data = resp.read(length)
            except httplib.IncompleteRead as e:                
                self.log(str(e), tb=sys.exc_traceback) #for debug purpose
                continue #data is imcomplete
            
            if self.listener.on_data(data) is False:
                self.running = False

    def _start(self, async):
        self.running = True
        if async:
            Thread(target=self._run).start()
        else:
            self._run()

    def userstream(self, count=None, async=False, secure=True):
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/2/user.json'
        self.host='userstream.twitter.com'
        if count:
            self.url += '&count=%s' % count
        self._start(async)

    def firehose(self, count=None, async=False):
        self.parameters = {'delimited': 'length'}
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/firehose.json?delimited=length' % STREAM_VERSION
        if count:
            self.url += '&count=%s' % count
        self._start(async)

    def retweet(self, async=False):
        self.parameters = {'delimited': 'length'}
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/retweet.json?delimited=length' % STREAM_VERSION
        self._start(async)

    def sample(self, count=None, async=False):
        self.parameters = {'delimited': 'length'}
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/sample.json?delimited=length' % STREAM_VERSION
        if count:
            self.url += '&count=%s' % count
        self._start(async)

    def filter(self, follow=None, track=None, async=False, locations=None, count = None):
        self.parameters = {}
        self.headers['Content-type'] = "application/x-www-form-urlencoded"
        if self.running:
            raise TweepError('Stream object already connected!')
        self.url = '/%i/statuses/filter.json?delimited=length' % STREAM_VERSION
        if follow:
            self.parameters['follow'] = ','.join(map(str, follow))
        if track:
            self.parameters['track'] = ','.join(map(str, track))
        if locations and len(locations) > 0:
            assert len(locations) % 4 == 0
            self.parameters['locations'] = ','.join(['%.2f' % l for l in locations])
        if count:
            self.parameters['count'] = count
        self.body = urllib.urlencode(self.parameters)
        self.parameters['delimited'] = 'length'
        self._start(async)

    def disconnect(self):
        if self.running is False:
            return
        self.running = False

    def log(self, msg, tb=None): #for debug purpose
        FileLog.log(self.log_file, str(msg), exception_tb=tb)    