'''
Created on Apr 18, 2016

@author: group09
''' 

import socket
import threading
import argparse
import json 
import re
import select
import sys

SIZE = 1024*1024*4 #4194304
num_jobs = 512
# SIZE = 20
# num_jobs = 4

a=[0.0]*SIZE

parser = argparse.ArgumentParser()
parser.add_argument("-r", "--remote", help="Add this flag if this is the remote node", action="store_true")
# maybe later
parser.add_argument("remote_port", help="Port for client to connect to remote node, or listen port on remote node", type=int)
args = parser.parse_args()
    
def job_slice(left_index, right_index):
    return ((left_index,right_index), a[left_index:right_index])



jq_lock = threading.Lock()
job_queue = []
finished_queue = []
load_policy = 0.7
comm_sock = socket.socket()

def send_some_jobs(jobs):
    data = '#jobs#'
    data += json.dumps(jobs)
    comm_sock.send(data)
    
    
def send_state():
    data = '#state#'
    data += json.dumps((len(job_queue),load_policy))
    comm_sock.send(data)
    
def receive_data():
    data = ''
    packet = comm_sock.recv(512*1024*1024) # TODO, pick a smarter packet size
    while packet:
        data += packet
        packet = comm_sock.recv(512*1024*1024)
    regex = re.match(r"#([a-z]+)#(.*)", data)
    type = regex.group(1)
    message = regex.group(2)
    
    if type == "jobs":
        return receive_some_jobs(message)
    elif type == "state":
        return receive_state(message)
    else:
        pass

def receive_state(message):
    state = json.loads(message)
    print state
    return state

def receive_some_jobs(message):
    jobs = json.loads(message)
    return jobs

class KBThread(threading.Thread):

    def __init__(self):
        '''
         Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        pass
    
 
    def run(self):
        input = [sys.stdin]
        while self.running:
            # block until we receive packets or keyboard input
            inputready, outputready, exceptready = select.select(input, [], [])
            thing = raw_input("yo typesome")    
            print thing
        
        
        

















if not args.remote:
    print "I am local node"
    print "remote port %d" % (args.remote_port)
    
    comm_sock.connect(("", args.remote_port))
#     comm_sock.send("Hi!")
#     response = comm_sock.recv(1024)
#     print response
#     comm_sock.send("Hi!222")
    
    for i in range(num_jobs):
        job_size = (SIZE/num_jobs)
        left_index = i* job_size
        right_index = left_index + job_size
        # print (i, (left_index,right_index)) 
        job_queue.append(job_slice(left_index, right_index))
        # print job_queue
    
    half = num_jobs / 2
    send_jobs = job_queue[half:]
    job_queue = job_queue[:half]
    
    send_some_jobs(send_jobs)
else:
    print "I am remote node"
    print "listening on port %d" % (args.remote_port)
    
    comm_sock.bind(("", args.remote_port))
    comm_sock.listen(20)
    (client_sock, addr_info) = comm_sock.accept()
    comm_sock = client_sock # this is really stupid, don't ever do this IRL
    # print addr_info
#     receive = comm_sock.recv(1024)
#     print receive
#     comm_sock.send("YOOOOOOOOO")
#     receive = comm_sock.recv(1024)
#     print receive
    
    # bootstrap phase
    jobs = receive_data()
    
    print "I got %d jobs from the client" % (len(jobs))
    ((first_l, first_r), last_arr) = jobs[0]
    ((last_l, last_r), last_arr) = jobs[-1]
    print "First job is indices %d to %d\nLast job is indices %d to %d" % (first_l, first_r, last_l, last_r)
    #print next
    
monitor = KBThread()
monitor.start()



# class WorkerThread(threading.thread):
#     '''
#     '''
#         
#     def __init__(self, params):
#         '''
#         Constructor
#         '''
#         self.running = True
#         pass
#     
#     @staticmethod
#     def run_job(job):
#         (_,array) = job
#         for i in range(len(array)):
#             for _ in range(200):               
#                 array[i] += 1.111111
# 
#     def run(self):
#         while (self.running):
#             job = job_queue.pop()
#             ((left_index, right_index),array) = job
#             
#             WorkerThread.run_job(job)
#             a[left_index:right_index]=array
#             
#         
        

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
          
# for i in range(1024*1024*4):
#     for j in range(200):
#         a[i] += 1.111111
#         