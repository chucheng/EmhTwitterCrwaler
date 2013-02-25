import socket
''' Configuration '''

### Database ###
#host = 'larch.cs.ucla.edu' 
host = 'localhost'

if host == 'larch':
    host = 'localhost'
user = 'your_db_user_id'
password = 'your_db_password'
database = 'tweettrenddb'


### Debuging ###
verbose = False #for having a verbose output

### StreamingCrawler Default ###
def getStreamingCrawlerSetting():
    """return (gSave_to_DB, gSave_raw_json, gSave_to_File)
    """
    host = socket.gethostname()
    print "Configuration -- host name is: " + str(host)
    if host == 'larch':
        gSave_to_DB = False        
        gSave_raw_json = False
        print "Configuration: Save to files only."
        gSave_to_File = True
    else:
        gSave_to_DB = False
        gSave_raw_json = False
        print "Configuration: Save to files only."
        gSave_to_File = True
        
    return (gSave_to_DB, gSave_raw_json, gSave_to_File)

### DBConnection Default ###
def getDBConnectionSetting():
    """ return (hostname, user, password, database)
    """
    host = socket.gethostname()
    hostname = 'larch.cs.ucla.edu'
    #hostname = 'localhost'
    user = 'your_db_user_id'
    password = 'your_db_password'
    #database = 'tweettrenddb'
#    database = 'tweetnews'
    database = 'smalltweetnews'
    #database = 'testtweetnews'
    
    print "Configuration -- host is: " + str(host)

    if host == 'larch':
        host = 'localhost'
        
    print "DB settings: (hostname, user, password, database) -- " + str((hostname, user, password, database))
    return (hostname, user, password, database)

### ProcessManager ###
def getProcessManagerSetting():
    """ return (username, password, username, password)"""
    nyti_account = 'your_twitter_user_id' #default
    nyti_password = 'your_password'

    if(socket.gethostname()=='ec2'): #only for ec2
        nyti_account = 'your_twitter_user_id'
        nyti_password = 'your_password'
    wsj_account = ''#'your_twitter_user_id'
    wsj_password = ''#'your_password'
    
    return (nyti_account, nyti_password, wsj_account, wsj_password)
