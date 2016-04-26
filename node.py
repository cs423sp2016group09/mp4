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

# multiplier constant
# this is a blind guess
k = 4

# only do a balance transfer if there's a 5 job imbalance
LOAD_BALANCE_THRESHOLD = 5

parser = argparse.ArgumentParser()
parser.add_argument("-r", "--remote", help="Add this flag if this is the remote node", action="store_true")
# maybe later
parser.add_argument("remote_port", help="Port for client to connect to remote node, or listen port on remote node", type=int)
args = parser.parse_args()


def job_slice(left_index, right_index):
    return ((left_index, right_index), a[left_index:right_index])


def send_some_jobs(sock, jobs, finished = False):
    data = "#finished#" if finished else '#jobs#'
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
    elif type == "finished":
        return receive_finished_jobs(message)
    elif type == "state":
        return receive_state(message)
    else:
        pass


def receive_state(message):
    print "receiving state"
    pats = re.match(r"(.*)#(.*)#(.*)", message)
    other_length = int(pats.group(1))
    other_finished_length = int(pats.group(2))
    other_load_policy = float(pats.group(3))

    other_state = (other_length, other_finished_length, other_load_policy)
    state_manager.handle_received_state(other_state)
    return other_state

f = open("a", "w+")
def receive_some_jobs(message):
    try:
        jobs = json.loads(message)
        job_queue_lock.acquire()
        job_queue.extend(jobs)
        job_queue_lock.release()
        print "I got %d jobs from the client" % (len(jobs))
        ((first_l, first_r), last_arr) = jobs[0]
        ((last_l, last_r), last_arr) = jobs[-1]
        print "First job is indices %d to %d\nLast job is indices %d to %d" % (first_l, first_r, last_l, last_r)
        work_event.set()
        return jobs

    except Exception, e:
        f.write(message)
        f.write("\n")
        print "written to file"

def receive_finished_jobs(message):
    print "receiving finished jobs"
    try:
        jobs = json.loads(message)
        finished_queue_lock.acquire()
        finished_queue.extend(jobs)
        finished_queue_lock.release()
        print "Finished queue adds %d jobs from the client" % (len(jobs))
        ((first_l, first_r), last_arr) = jobs[0]
        ((last_l, last_r), last_arr) = jobs[-1]
        print "First job is indices %d to %d\nLast job is indices %d to %d" % (first_l, first_r, last_l, last_r)
        work_event.set()
        return jobs
    except Exception, e:
        f.write(message)
        f.write("\n")
        print "written to file"

job_queue = []
finished_queue = []
job_queue_lock = threading.Lock()
finished_queue_lock = threading.Lock()

def remove_n_jobs(n):
    # WARNING: NOT THREAD SAFE
    global job_queue
    to_transfer = job_queue[-n:]
    job_queue = job_queue[:-n]
    return to_transfer

def add_finished_job(job):
    global finished_queue
    finished_queue.append(job)

def remove_finished_jobs():
    global finished_queue
    new_finished_queue = finished_queue[:]
    finished_queue = []
    return new_finished_queue

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
    # print "after recv"

    # print next


class StateManager(threading.Thread):

    def __init__(self, sock):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        self.sock = sock

    def handle_received_state(self, (other_length, other_finished_length, other_load_policy)):
        # print "i am being asked to handle other state"
        # print "other_length", other_length, "other_lp", other_load_policy
        job_queue_lock.acquire()
        finished_queue_lock.acquire()

        my_length = len(job_queue)
        my_finished_length = len(finished_queue)
        job_queue_lock.release()
        finished_queue_lock.release()
        my_load_policy = hardware_monitor.load_policy
        bundle = (my_length, my_finished_length, my_load_policy, other_length, other_finished_length, other_load_policy)
        # send bundle to adapter
        adaptor_thread.request_calculation(bundle)

    def run(self):
        while (self.running):
            time.sleep(30)
            # data = "#%f#%d#" % (load_policy, len(job_queue))
            # send_msg(self.sock, data)
            hardware_monitor.send_state(self.sock)


class AdaptorThread(threading.Thread):

    def __init__(self, sock, remote, job_queue):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        self.sock = sock
        self.bundle = ()
        self.wake = threading.Event()
        self.remote = remote
        self.job_queue = job_queue

    def request_calculation(self, bundle):
        self.bundle = bundle
        self.wake.set()

    def run(self):
        while (self.running):
            self.wake.wait()
            # do some work

            (my_length, my_finished_length, my_load_policy, other_length, other_finished_length, other_load_policy) = self.bundle
            if (my_length == 0 and  my_finished_length == 0 and other_length == 0 and other_finished_length == 0):
                shutdown()
            print "i'm performing a calculation on ", my_length, my_load_policy, other_length, other_load_policy    

            if not self.remote:
                s = (k * other_load_policy * my_length - my_load_policy * other_length) / (k * other_load_policy + my_load_policy)
            else:
                s = (other_load_policy * my_length - k * my_load_policy * other_length) / (other_load_policy + k * my_load_policy)

            if (s > LOAD_BALANCE_THRESHOLD):
                print "i think we should transfer ", s, " jobs"
                job_queue_lock.acquire()

                count = int(s)

                to_transfer = remove_n_jobs(count)

                job_queue_lock.release()
                transfer_manager.request_transfer(to_transfer)

            self.jobs_to_send = ()
            self.wake.clear()
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
        job_queue_lock.acquire()
        finished_queue_lock.acquire()
        data = "#state#%d#%d#%f" % (len(job_queue),(len(finished_queue)), self.load_policy)
        job_queue_lock.release()
        finished_queue_lock.release()
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
            time.sleep(self.work_interval)  # after 70 ms
            load_policy_enforcer_event.clear()  # put the worker to sleep
            time.sleep(0.1 - self.work_interval)  # after 30 ms
            load_policy_enforcer_event.set()  # wake it up
            # time.sleep(20)  # after 70 ms
            # load_policy_enforcer_event.clear()  # put the worker to sleep
            # print "putting worker to sleep"
            # time.sleep(10)  # after 30 ms
            # load_policy_enforcer_event.set()  # wake it up
            # print "waking worker up"


class WorkerThread(threading.Thread):

    def __init__(self, transfer_manager):
        '''
        Constructor
        '''
        threading.Thread.__init__(self)
        self.running = True
        self.transfer_manager = transfer_manager

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
                work_event.clear()
                print "done with jobs"

                finished_queue_lock.acquire()
                new_finished_queue = remove_finished_jobs()
                finished_queue_lock.release()

                if len(new_finished_queue) > 0:
                    if (not args.remote): 
                        print "Applying ", len(new_finished_queue), " completed jobs"
                        for i in range(len(new_finished_queue)):
                            ((first_l, first_r), last_arr) = new_finished_queue[i]
                            a[first_l:first_r] = last_arr
                    else:
                        print "sending", len(new_finished_queue), " jobs"
                        self.transfer_manager.request_transfer(new_finished_queue, True)

                work_event.wait()
            else:
                job_queue_lock.acquire()
                job = job_queue.pop()
                job_queue_lock.release()
                ((left_index, right_index), array) = job
              	print "Popped new job with starting index: %d and end index: %d" %(left_index, right_index)
                # WorkerThread.run_job(job)
                (_, array) = job
                for i in range(len(array)):
                    for _ in range(200):
                        load_policy_enforcer_event.wait()
                        array[i] += 1.111111

                add_finished_job(job)
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

    def request_transfer(self, jobs, finished = False):
        self.wake.set()
        self.jobs_to_send = jobs
        self.finished = finished

    def run(self):
        while (self.running):
            self.wake.wait()
            # do some work
            send_some_jobs(self.sock, self.jobs_to_send, self.finished)
            self.jobs_to_send = []
            self.wake.clear()


running = True

adaptor_thread = AdaptorThread(comm_sock, args.remote, job_queue)
adaptor_thread.start()

hardware_monitor = HardwareMonitor(comm_sock)
hardware_monitor.start()

transfer_manager = TransferManager(comm_sock)
transfer_manager.start()

worker_thread = WorkerThread(transfer_manager)
worker_thread.start()


state_manager = StateManager(comm_sock)
state_manager.start()

read_fds = [sys.stdin, comm_sock]
def shutdown():
    global running
    running = False
    worker_thread.running = False
    adaptor_thread.running = False
    hardware_monitor.running = False
    transfer_manager.running = False
    state_manager.running = False
    work_event.set()


while running:
    # block until we receive packets or keyboard read_fds
    (senders, _, _) = select.select(read_fds, [], [])
    for sender in senders:
        # received packets
        if sender == comm_sock:
            receive_data(comm_sock)
        # handle keyboard input
        elif sender == sys.stdin:
            thing = sys.stdin.readline().rstrip('\n')
            new_load_policy = float(thing)
            print "New load policy will be set to ", new_load_policy
            hardware_monitor.set_load_policy(new_load_policy)
