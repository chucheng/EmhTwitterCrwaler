""" FileSaver: Save message to an file asynchronizedly.

"""
import os
import sys
import time
import FileLog
import multiprocessing

DEBUG_FLAG = True  # While this is true, it log almost everything (not recommend in a production env)

class FileSaver:
    """Save message to a file asynchronously.
    
    To use this file:
    
    fs = FileSaver("./test_chucheng.data", delegate_process) 
    # Assign a file name. Note that this will also create corresponding 
    # directories. delegate_process is a delegate function that will be triggered
    # before writing data to the file.
    # delegate_process -- input: any data, output: str
                                           
    fs.start() # The worker process trys to write to the file from now on
    
    fs.save(data) #you can save data even before the start
                  #this is nothing more than putting data into a queue; thus, it should be very fast.
    fs.stop() # This will sent out the stop signal to the worker process.
              # Note that the worker process will die only it complete the task.
    
    
    """
    
    def __init__(self, full_path_name, delegate_process=None):
        #Setup log 
        FileLog.set_log_dir() 
        self.log_filename = "FileSaver.log"
        self.write_to_basename = os.path.basename(full_path_name)   
        self.write_to_fullname = full_path_name
        
        #MutliProcessing Shared Memory
        self.queue = multiprocessing.Queue()          
        self.delegate_process = delegate_process
        if not self.delegate_process:
            self.log("No delegate process function is in use, Store raw data")
        
    def start(self):        
        self.running = multiprocessing.Value('i', 1) #1 start the running.
        self.proc = multiprocessing.Process(target=FileSaver.run, 
                                            args=(self.running, 
                                                  self.queue, 
                                                  self.write_to_fullname,
                                                  self.delegate_process)
                                            )
        self.proc.start()
        
        self.log("Start the worker process (for writing to the file)")          
    
    def stop(self):
        """This will sent out a stop signal to worker process.

        Note that: even the main process is terminated, the worker process will continue to work until it's done.
        """
        self.running.value = 0 #0 stop the running.            
        self.log("Receive a request of stopping the worker process (for writing to the file)")

    def get_current_filename(self):
        return self.write_to_basename
    
    def get_pid(self):
        return self.proc.pid
    
    def process_is_alive(self):
        return self.proc.is_alive()
        
    def waitstop(self):
        self.stop()
        self.proc.join()
        self.proc.terminate()
        
    def save(self, message):
        self.queue.put(message)
        
    def log(self, message, tb=None):
        """Log error message and its traceback(optional)"""
        FileLog.log(self.log_filename, "FileSaver({0}): ".format(self.write_to_basename) + str(message), exception_tb=tb)
        

    @staticmethod
    def run(shared_running, shared_msg_queue, full_path_name, delegate_process=None):
        """This is the main part of codes that save the file

        Keywords Arguments:
        running -- multiprocessing.Value (Shared Memory)
        shared_msg_queue -- the worker prcoess keep monitor this queue
        full_path_name -- the location that the worker save the data from the queue
        delegate_process -- process data before writing it into a file
        """                 
        flag_new_data = False
        f = None
        try:
            f = open(full_path_name, 'a')
#            print "shared_running.value = ", shared_running.value
            while shared_running.value == 1:                
                if shared_msg_queue.empty():
                    time.sleep(1) #sleep 1 sec if there's no data
                    #print "-->sleep 1 sec if there's no data." #for debug                                                                        
                else:
                    data = shared_msg_queue.get() #if queue is empty, it will hang
#                    print "lag...50sec" #for debug
#                    time.sleep(50) #for debug
#                    print "main loop: writing: " + str(data) #for debug
                    if delegate_process:
                        processed_data = str(delegate_process(data))
                        if not processed_data:
                            FileLog.log("FileSaver.log", "FileSaver({0}, pid:{1}): "
                                        .format(os.path.basename(full_path_name), str(os.getpid())) 
                                        + "Warning: Delegated process function return None. " 
                                        + "Skip writing to the file. " 
                                        + "The input to the process function: " + str(data))            
                        else:
                            f.write(processed_data)
                    else:
                        f.write(str(data)) # Write raw data
                    f.flush()
                    os.fsync(f.fileno())                                     
#            print "!!!!!!!!!STOP now !!!!!!!!"        
            #when shared_running = 0
            FileLog.log("FileSaver.log", "FileSaver({0}, pid:{1}): "
                        .format(os.path.basename(full_path_name), str(os.getpid())) 
                        + "A STOP signal is receieved, start to clean up the queue." 
                        + "Queue Size (approxmately): " + str(shared_msg_queue.qsize()))
            while not shared_msg_queue.empty():
#                print "lag...1sec" #for debug
#                time.sleep(1) #for debug
#                data = shared_msg_queue.get()
#                print "empty_queue: writing: " + str(data) #for debug
                if delegate_process:
                    data = shared_msg_queue.get()
                    processed_data = str(delegate_process(data))
                    if not processed_data:
                        FileLog.log("FileSaver.log", "FileSaver({0}, pid:{1}): "
                                    .format(os.path.basename(full_path_name), str(os.getpid())) 
                                    + "Warning: Delegated process function return None. " 
                                    + "Skip writing to the file. " 
                                    + "The input to the process function: " + str(data))            
                    else:
                        f.write(processed_data)
                else:
                    f.write(str(data)) # Write raw data
                f.flush()
                os.fsync(f.fileno())
       
            f.close()
            FileLog.log("FileSaver.log", "FileSaver({0}, pid:{1}): "
                        .format(os.path.basename(full_path_name), str(os.getpid())) 
                        + "Queue is empty. The file is closed. This process is going to be terminated now.")            
        except Exception as e:
            if(f and isinstance(f, file)):
                f.close()
            FileLog.log("FileSaver.log", "FileSaver({0}, pid:{1}): "
                            .format(os.path.basename(full_path_name), str(os.getpid())) 
                            + str(e), exception_tb=sys.exc_traceback)
        
    @staticmethod
    def ensure_dir(full_path_name):
        """ Given a full path filename, make sure the dir it locates exists"""        
        d = os.path.dirname(full_path_name)
        if not os.path.exists(d):
            os.makedirs(d)

            
def test1():
    #Testing cases:
    print "main process pid: " + str(os.getpid())
    
    #fs = FileSaver("./test_chucheng.data")    
    fs = FileSaver("./test_chucheng.data", test_delgate)    
    fs.save("Hello 1")
    #print "sent Hello 1"
    fs.start()
    fs.save("Hello 2")
    fs.save("Hello 3")
    print "--> call stop"
    fs.stop()
    print "--> call stopdone"
    #time.sleep(3)
    fs.save("Hello end 4")
    fs.save("Hello end 5")
    fs.save("Hello end 6")    
    print "main process sent out all info"
    #fs.waitstop()
    print "main process done"
    while True:
        print "main process...sleep"
        time.sleep(1)
    
    
def test2():
    #Testing cases:
    print "main process pid: " + str(os.getpid())
    
    fs = FileSaver("./tchucheng_1.data")    
    fs.save("Hello 1")
    #print "sent Hello 1"
    fs.start()
    fs.save("Hello 2")
    fs.save("Hello 3")
    #time.sleep(3)
    fs.save("Hello end 4")
    fs.save("Hello end 5")
    fs.save("Hello end 6")
    
    print "main process sent out all info"
    fs.stop()
    
    
    
    fs2 = FileSaver("./tchucheng_2.data")    

    fs2.save("Hello end 4")
    fs2.save("Hello end 5")
    fs2.save("Hello end 6")
    fs2.start()
    fs2.stop()
    
    print "fs2:", fs2.process_is_alive()
    time.sleep(4)
    print "fs2:", fs2.process_is_alive()
    print "--- main process done ---"
    
def test_delgate(data):
    return "$$$delegate$$$" + data

if __name__ == "__main__":
    test1()
    