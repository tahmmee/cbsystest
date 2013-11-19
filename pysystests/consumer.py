import datetime
import time
import json
import gevent
import random
import argparse


# couchbase
from couchbase.experimental import enable as enable_experimental
from couchbase.exceptions import NotFoundError, TemporaryFailError, TimeoutError
enable_experimental()
from gcouchbase.connection import GConnection
import testcfg as cfg

# rabbit
from librabbitmq import Connection
from rabbit_helper import RabbitHelper


# para
from gevent import Greenlet, queue
import threading
import multiprocessing
from multiprocessing import Process, Event,  queues

from gevent import monkey
monkey.patch_all()

#logging
import logging
logging.basicConfig(filename='consumer.log',level=logging.DEBUG)

# setup parser
parser = argparse.ArgumentParser(description='CB System Test KV Consumer')
parser.add_argument("--cluster", default = cfg.CB_CLUSTER_TAG, help="the CB_CLUSTER_TAG from testcfg will be used by default <default>")

# some global state
CB_CLUSTER_TAG = ""
CLIENTSPERPROCESS = 4
NUMPROCESSES = 5
PROCSSES = []

# broker handle
conn = Connection(host="localhost", userid="guest",
                  password="guest", virtual_host="default")
channel = conn.channel()



class SDKClient(threading.Thread):

    def __init__(self, name, task, e):
        threading.Thread.__init__(self)
        self.name = name
        self.i = 0
        self.op_factor = CLIENTSPERPROCESS * NUMPROCESSES
        self.ops_sec = task['ops_sec']
        self.bucket = task['bucket']
        self.password  = task['password']
        self.template = task['template']
        self.create_count = task['create_count']/self.op_factor
        self.update_count = task['update_count']/self.op_factor
        self.get_count = task['get_count']/self.op_factor
        self.del_count = task['del_count']/self.op_factor
        self.exp_count = task['exp_count']/self.op_factor
        self.consume_queue = task['consume_queue']
        self.ttl = task['ttl']
        self.miss_perc = task['miss_perc']
        self.active_hosts = task['active_hosts']
        self.batch_size = 1000
        self.memq = queue.Queue()
        self.hotset = []
        self.ccq = None
        self.hotkeys = []
        if 'cc_queues' in task['template']:
            self.ccq = str(task['template']['cc_queues'][0])  #only supporting 1 now
            RabbitHelper().declare(self.ccq)

        self.batch_size = 1000
        if self.batch_size > self.create_count:
            self.batch_size = self.create_count

        self.active_hosts = task['active_hosts']
        addr = task['active_hosts'][random.randint(0,len(self.active_hosts) - 1)].split(':')
        host = addr[0]
        port = 8091
        if len(addr) > 1:
            port = addr[1]
        self.cb = GConnection(bucket=self.bucket, password = self.password, host = host, port = port)


        self.e = e

    def run(self):

        while self.e.is_set() == False:

            start = datetime.datetime.now()


            # do an op cycle
            threads = self.do_cycle()
            gevent.joinall(threads)


            # wait till next cycle
            end = datetime.datetime.now()
            wait = 1 - (end - start).microseconds/float(1000000)
            if (wait > 0):
                time.sleep(wait)
            else:
                pass #probably  we are overcomitted, but it's ok

            if self.memq.qsize() > 20:
                self.flushq()

        # push everything to rabbitmq
        self.flushq()


    def flushq(self):

        if self.ccq is not None:

            # declare queue
            mq = RabbitHelper()
            mq.declare(self.ccq)

            while self.memq.empty() == False:
                try:
                    msg = self.memq.get_nowait()
                    msg = json.dumps(msg)
                    mq.putMsg(self.ccq, msg)
                except queue.Empty:
                    pass

                # hot keys
                if len(self.hotkeys) > 0:
                    key_map = {'start' : self.hotkeys[0],
                               'end' : self.hotkeys[-1]}
                    msg = json.dumps(key_map)
                    mq.putMsg(self.ccq, msg)


    def do_cycle(self):

        threads = []

        if self.create_count > 0:
            t = gevent.spawn(self.mset, self.template['kv'], self.create_count)
            threads.append(t)

        if self.update_count > 0:
            t = gevent.spawn(self.mset_update, self.template['kv'], self.update_count)
            threads.append(t)

        if self.get_count > 0:
            t = gevent.spawn(self.mget, self.get_count)
            threads.append(t)

        if self.del_count > 0:
            t = gevent.spawn(self.mdelete, self.del_count)
            threads.append(t)


        return threads

    def mset(self, template, count):
        msg = {}
        keys = []
        cursor = 0
        j = 0

        for j in xrange(count):
            self.i = self.i+1
            msg[self.name+str(self.i)] = template
            keys.append(self.name+str(self.i))
            if ((j+1) % self.batch_size) == 0:
                batch = keys[cursor:j+1]
                self.memq.put_nowait({'start' : batch[0],
                                      'end'  : batch[-1]})
                self._mset(msg)
                cursor = j + 1
                msg = {}

        if (cursor < j) and (len(msg) > 0):
            self._mset(msg)
            self.memq.put_nowait({'start' : keys[cursor],
                                  'end'  : keys[-1]})

    def _mset(self, msg):
        try:
            self.cb.set_multi(msg)
        except TemporaryFailError:
            logging.warn("temp failure during mset - cluster may be unstable")
        except TimeoutError:
            logging.warn("cluster timed trying to handle mset")

    def mset_update(self, template, count):

        msg = {}
        batches = self.getKeys(count)
        if len(batches) > 0:

            for batch in batches:
                try:
                    for key in batch:
                        msg[key] = template
                    self.cb.set_multi(msg)
                except NotFoundError as nf:
                    logging.error("update key not found!  %s: " % nf.key)
                except TimeoutError:
                    logging.warn("cluster timed out trying to handle mset - cluster may be unstable")
                except TemporaryFailError:
                    logging.warn("temp failure during mset - cluster may be unstable")


    def mget(self, count):

        batches = []
        if self.miss_perc > 0:
            batches = self.getCacheMissKeys(count)
        else:
            batches = self.getKeys(count)

        if len(batches) > 0:

            for batch in batches:
                try:
                    self.cb.get_multi(batch)
                except NotFoundError as nf:
                    logging.warn("get key not found!  %s: " % nf.key)
                except TimeoutError:
                    logging.warn("cluster timed out trying to handle mget - cluster may be unstable")


    def mdelete(self, count):
        batches = self.getKeys(count, requeue = False)
        keys_deleted = 0

        # delete from buffer
        if len(batches) > 0:
            keys_deleted = self._mdelete(batches)
        else:
            pass

    def _mdelete(self, batches):
        keys_deleted = 0
        for batch in batches:
            try:
                if len(batch) > 0:
                    keys_deleted = len(batch) + keys_deleted
                    self.cb.delete_multi(batch)
            except NotFoundError as nf:
                logging.warn("get key not found!  %s: " % nf.key)
            except TimeoutError:
                logging.warn("cluster timed out trying to handle mdelete - cluster may be unstable")

        return keys_deleted


    def getCacheMissKeys(self, count):

        # returns batches of keys where first batch contains # of keys to miss
        keys_retrieved = 0
        batches = []
        miss_keys = []
        requeue = len(self.hotkeys) > 0

        keys = self.getKeysFromQueue(requeue = requeue, force_stale = True)

        if requeue == False:
            # hotkeys were taken off queue and cannot be reused
            self.hotkeys = keys


        if len(keys) > 0:
            # miss% of count keys
            num_to_miss = int( ((self.miss_perc/float(100)) * len(keys)) / float(self.op_factor))
            miss_keys = keys[:num_to_miss]
            batches.append(miss_keys)
            keys_retrieved = len(miss_keys)


        # use old hotkeys for rest of set
        while keys_retrieved < count:

            keys = self.hotkeys

            # in case we got too many keys slice the batch
            need = count - keys_retrieved
            if(len(keys) > need):
                keys = keys[:need]

            keys_retrieved = keys_retrieved + len(keys)

            # add to batch
            batches.append(keys)

        return batches

    def getKeys(self, count, requeue = True):

        keys_retrieved = 0
        batches = []

        while keys_retrieved < count:

            # get keys
            keys = self.getKeysFromQueue(requeue)

            if len(keys) == 0:
                break

            # in case we got too many keys slice the batch
            need = count - keys_retrieved
            if(len(keys) > need):
                keys = keys[:need]

            keys_retrieved = keys_retrieved + len(keys)

            # add to batch
            batches.append(keys)


        return batches

    def getKeysFromQueue(self, requeue = True, force_stale = False):

        # get key mapping and convert to keys
        keys = []
        key_map = None

        # priority to stale queue
        if force_stale:
            key_map = self.getKeyMapFromRemoteQueue(requeue)

        # fall back to local qeueue
        if key_map is None:
            key_map = self.getKeyMapFromLocalQueue(requeue)

        if key_map:
            keys = self.keyMapToKeys(key_map)


        return keys

    def keyMapToKeys(self, key_map):

        keys = []
        # reconstruct key-space
        prefix, start_idx = key_map['start'].split('_')
        prefix, end_idx = key_map['end'].split('_')

        for i in range(int(start_idx), int(end_idx) + 1):
            keys.append(prefix+"_"+str(i))

        return keys


    def fillq(self):

        if self.ccq == None:
            return

        # put about 20 items into the queue
        for i in xrange(20):
            key_map = self.getKeyMapFromRemoteQueue()
            if key_map:
                self.memq.put_nowait(key_map)

    def getKeyMapFromLocalQueue(self, requeue = True):

        key_map = None

        try:
            key_map = self.memq.get_nowait()

        except queue.Empty:
            #no more items
            if self.ccq is not None:
                self.fillq()

        return key_map

    def getKeyMapFromRemoteQueue(self, requeue = True):
        key_map = None
        mq = RabbitHelper()
        if mq.qsize(self.ccq) > 0:
            try:
                key_map = mq.getJsonMsg(self.ccq, requeue = requeue )
            except Exception:
                pass
        return key_map


class SDKProcess(Process):

    def __init__(self, id, task):

        super(SDKProcess, self).__init__()

        self.task = task
        self.id = id
        self.clients = []
        p_id = self.id
        self.client_events = [Event() for e in xrange(CLIENTSPERPROCESS)]

        for i in xrange(CLIENTSPERPROCESS):
            name = _random_string(4)+"-"+str(p_id)+str(i)+"_"

            # start client
            client = SDKClient(name, self.task, self.client_events[i])
            self.clients.append(client)

            p_id = p_id + 1



    def run(self):

        # start process clients
        for client  in self.clients:
            client.start()

        self.clients[0].join()


    def terminate(self):
        for e in self.client_events:
            e.set()

def _random_string(length):
    return (("%%0%dX" % (length * 2)) % random.getrandbits(length * 8)).encode("ascii")

def kill_procs():

    for p in PROCSSES:
        p.terminate()


def start_client_processes(task):

    for i in range(NUMPROCESSES):

        # set process id and provide queue
        p_id = (i)*CLIENTSPERPROCESS
        p = SDKProcess(p_id, task)

        # start
        p.start()

        # archive
        PROCSSES.append(p)

def init(message):
    body = message.body
    try:
        task = json.loads(str(body))
        kill_procs()
        start_client_processes(task)

    except Exception as ex:
        print "Unable to start workload processes"
        print ex

    message.ack()



def main():
    args = parser.parse_args()
    CB_CLUSTER_TAG = args.cluster

    # listen for task from workers
    consumer_queue = "sdk_consumers_"+CB_CLUSTER_TAG
    channel.queue_declare(queue = consumer_queue, durable = True, auto_delete = True)
    channel.basic_consume(consumer_queue, callback=init)

    while True:
        conn.drain_events()


if __name__ == "__main__":
    main()

