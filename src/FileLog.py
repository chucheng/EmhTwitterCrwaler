''' Providing basic log features

Suggested way to use the following log function:
(1) In the constructor, run FileLog.set_log_dir()
(2) Create a member function self.log, and add FileLog.log(...) in it

In try catch, use: tb=sys.exc_traceback
'''
import os
import datetime
import sys
import traceback

default_log_path = "../log/"

def set_log_dir(new_log_path=None):  
    """Make sure the deafult log directory, ../log/, exists."""
    log_path = default_log_path
    if new_log_path:
        log_path = new_log_path
        
    if log_path[-1] != os.sep: #make sure we have a path separtor charcter 
        log_path += os.sep
    if not os.path.exists(log_path):
        os.makedirs(log_path)  

def log(log_file, msg, print_log=True, exception_tb=None, 
        log_path=default_log_path):
    """Print out a message and save it to a log file.
    
    If log_file is None or empty, the message will not be stored.
    By default, the log path is ../log/
    """
    now = datetime.datetime.now()
    now_str = now.strftime('%m/%d/%Y %H:%M:%S') #'03/09/2011 17:41:30'
    full_msg = now_str + " > " + str(msg)
    if exception_tb:
        tb_list = traceback.format_tb(exception_tb)        
        full_msg = full_msg + "\nTraceback:\n" + "\n".join(tb_list)
    
    if print_log:
        print full_msg
    if log_file:
        with open(log_path + log_file, "a") as log_file:
            log_file.write(full_msg +os.linesep)