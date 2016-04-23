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
import time
import signal
import messaging

def signal_handler(signal, frame):
    print 'You pressed Ctrl+C!'
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

SIZE = 1024 * 1024 * 4  # 4194304
num_jobs = 512
# SIZE = 20
# num_jobs = 4

a = [0.0] * SIZE

parser = argparse.ArgumentParser()
parser.add_argument("-r", "--remote", help="Add this flag if this is the remote node", action="store_true")
# maybe later
parser.add_argument("remote_port", help="Port for client to connect to remote node, or listen port on remote node", type=int)
args = parser.parse_args()


def job_slice(left_index, right_index):
    return ((left_index, right_index), a[left_index:right_index])


def send_some_jobs(sock, jobs):
    data = '#jobs#'
    data += json.dumps(jobs)

    messaging.send_msg(sock, data)
    # print "sent %d bytes" % len(data)



def receive_data(sock):
    # data = sock.recv(512)  # TODO, pick a smarter packet size

    # packr = re.match(r"#([a-z]+)#([0-9]+)#(.*)", data)
    # type = packr.group(1)
    # length = int(packr.group(2))

    # print "type: %s, length: %d" % (type, length)

    # i = 0
    # while len(data) < length:
    # print "get another packet %d" % i
    #     packet = sock.recv(512)
    #     data += packet
    #     i += 1

    # print "length received"
    # print len(data)

    # regex = re.match(r"#([a-z]+)#([0-9]+)#(.*)", data)
    data = messaging.recv_msg(sock)

    # print data

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
    print "receiving state"
    pats = re.match(r"(.*)#(.*)")
    other_length = int(pats.group(1))
    other_load_policy = float(pats.group(2))

    return (other_length, other_load_policy)


def receive_some_jobs(message):
    jobs = json.loads(message)
    return jobs

job_queue = []
finished_queue = []
job_queue_lock = threading.Lock()

work_event = threading.Event()
load_policy_enforcer_event = threading.Event()
load_policy_enforcer_event.set()

if not args.remote:
    print "I am local node"
    print "remote port %d" % (args.remote_port)

    comm_sock = socket.socket()
    comm_sock.connect(("", args.remote_port))
    # print "connected"
#     response = comm_sock.recv(1024)
#     print response
#     comm_sock.send("Hi!222")

    for i in range(num_jobs):
        job_size = (SIZE / num_jobs)
        left_index = i * job_size
        right_index = left_index + job_size
        # print (i, (left_index,right_index))
        job_queue.append(job_slice(left_index, right_index))
        # print job_queue

    half = num_jobs / 2
    send_jobs = job_queue[half:]
    job_queue = job_queue[:half]

    # print "sending"

    send_some_jobs(comm_sock, send_jobs)
    print "should have sent"
else:
    print "I am remote node"
    print "listening on port %d" % (args.remote_port)
    temp_socket = socket.socket()
    temp_socket.bind(("", args.remote_port))
    temp_socket.listen(20)
    # print "block until accept"
    (comm_sock, addr_info) = temp_socket.accept()
    # print "after accept"
    # bootstrap phase
    jobs = receive_data(comm_sock)
    job_queue.extend(jobs)
    # print "after recv"
    print "I got %d jobs from the client" % (len(jobs))
    ((first_l, first_r), last_arr) = jobs[0]
    ((last_l, last_r), last_arr) = jobs[-1]
    print "First job is indices %d to %d\nLast job is indices %d to %d" % (first_l, first_r, last_l, last_r)
    # print next

class StateManager(threading.Thread):

    def __init__(self, sock):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        self.sock = sock

    def run(self):
        while (self.running):
            time.sleep(5)
            # data = "#%f#%d#" % (load_policy, len(job_queue))
            # send_msg(self.sock, data)
            hardware_monitor.send_state(self.sock)

class AdaptorThread(threading.Thread):

    def __init__(self, sock):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        self.sock = sock

    def run(self):
        while (self.running):
            time.sleep(5)
            # data = "#%f#%d#" % (load_policy, len(job_queue))
            # send_msg(self.sock, data)

# sole job is to enforce the work time policy
class HardwareMonitor(threading.Thread):

    def __init__(self, load_policy):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        self.load_policy = 0.7
        self.update_work_interval()

    def send_state(self, sock):
        data = "#state#%d#%f" % (len(job_queue), self.load_policy)
        messaging.send_msg(sock, data)

    def set_load_policy(self, new_load_policy):
        if new_load_policy < 1 and new_load_policy > 0:
            self.load_policy = new_load_policy
            print "New load policy is set to: ", self.load_policy
        else:
            print "The load policy is must be in range (0,1)!!!!"
        self.update_work_interval()


    def update_work_interval(self):
        self.work_interval = self.load_policy * 100.0 / 1000.0

    def run(self):
        while (self.running):
            # time.sleep(work_interval)  # after 70 ms
            # load_policy_enforcer_event.clear()  # put the worker to sleep
            # time.sleep(0.1 - work_interval)  # after 30 ms
            # load_policy_enforcer_event.set()  # wake it up
            time.sleep(20)  # after 70 ms
            load_policy_enforcer_event.clear()  # put the worker to sleep
            print "putting worker to sleep"
            time.sleep(10)  # after 30 ms
            load_policy_enforcer_event.set()  # wake it up
            print "waking worker up"


class WorkerThread(threading.Thread):

    def __init__(self):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True

    @staticmethod
    def run_job(job):
        (_, array) = job
        for i in range(len(array)):
            for _ in range(200):
                load_policy_enforcer_event.wait()
                array[i] += 1.111111

    def run(self):
        while (self.running):

            if len(job_queue) == 0:
                print "done with jobs"
                work_event.wait()
            else:
                job_queue_lock.acquire()
                job = job_queue.pop()
                job_queue_lock.release()
                print "popped new job "
                ((left_index, right_index), array) = job

                # WorkerThread.run_job(job)
                (_, array) = job
                for i in range(len(array)):
                    for _ in range(200):
                        load_policy_enforcer_event.wait()
                        array[i] += 1.111111


                finished_queue.append(job)
                # a[left_index:right_index] = array

class TransferManager(threading.Thread):

    def __init__(self, sock):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        self.sock = sock
        self.wake = threading.Event()
        self.jobs_to_send = []

    def request_transfer(self, jobs):
        self.wake.set()
        self.jobs_to_send = jobs


    def run(self):
        while (self.running):
            self.wake.wait()
            # do some work
            send_some_jobs(self.sock, self.jobs_to_send)
            self.jobs_to_send = []
            self.wake.clear()


running = True
worker_thread = WorkerThread()
worker_thread.start()

adaptor_thread = AdaptorThread(comm_sock)
adaptor_thread.start()

hardware_monitor = HardwareMonitor(comm_sock)
hardware_monitor.start()

transfer_manager = TransferManager(comm_sock)
transfer_manager.start()

state_manager = StateManager(comm_sock)
state_manager.start()

read_fds = [sys.stdin, comm_sock]
while running:
    # block until we receive packets or keyboard read_fds
    (senders, _, _) = select.select(read_fds, [], [])
    for sender in senders:
        # received packets
        if sender == comm_sock:
            a = messaging.recv_msg(comm_sock)

            print "received message", a
        # handle keyboard input
        elif sender == sys.stdin:
            thing = sys.stdin.readline().rstrip('\n')
            new_load_policy = float(thing)
            print "New load policy will be set to ", new_load_policy
            hardware_monitor.set_load_policy(new_load_policy)
