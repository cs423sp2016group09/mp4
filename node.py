'''
Created on Apr 18, 2016

@author: group09
''' 

import socket
import threading

SIZE = 1024*1024*4 #4194304
num_jobs = 512

a=[0.0]*SIZE


def job_slice(left_index, right_index):
    return ((left_index,right_index), a[left_index:right_index])
jq_lock = threading.Lock()
job_queue = []

for i in range(num_jobs):
    job_size = (SIZE/num_jobs)
    left_index = i* job_size
    right_index = left_index + job_size
#     print (i, (left_index,right_index)) 
    job_queue.append(job_slice(left_index, right_index))
print job_queue
# 
# rec_socket = socket.socket()
# rec_socket.bind(("", 6000))
# rec_socket.listen(20)
# (send_sock,addr_info) = rec_socket.accept()
# print addr_info
# 


class WorkerThread(threading.thread):
    '''
    '''
        
    def __init__(self, params):
        '''
        Constructor
        '''
        self.running = True
        pass
    
    @staticmethod
    def run_job(job):
        (_,array) = job
        for i in range(len(array)):
            for _ in range(200):               
                array[i] += 1.111111

    def run(self):
        while (self.running):
            job = job_queue.pop()
            ((left_index, right_index),array) = job
            
            WorkerThread.run_job(job)
            a[left_index:right_index]=array
            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
          
# for i in range(1024*1024*4):
#     for j in range(200):
#         a[i] += 1.111111
#         