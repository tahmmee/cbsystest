from rabbit_helper import PersistedMQ, RabbitHelper
import json 
from gevent import Greenlet

from couchbase.experimental import enable as enable_experimental
enable_experimental()
from gcouchbase.connection import GConnection
from librabbitmq import Connection

#mq = Connection(host="localhost", userid="guest",
#                  password="guest", virtual_host="default")
#channel = mq.channel()

#def dump_message(message):
#    data = message.body
#channel.basic_consume("gvset", callback=dump_message)


def do(cb, data):

    try:
        tasks = json.loads(str(data))
        for subtask in tasks:
            type_ = subtask['task']
            args = subtask['args']
            keys = args[0]
            meta = args[1]
            template = meta['kv']

            msg = {}
            for key in keys:
                msg[key] = template

            cb.set_multi(msg)

    except Exception as ex:
        import pdb; pdb.set_trace()
        print ex 



class Consumer(Greenlet):

    def __init__(self, queue):
        Greenlet.__init__(self)
        self.queue = queue 
        self.cb = GConnection(bucket='default')
        self.conn = RabbitHelper()

    def _run(self):

        while True:
            data = self.conn.getMsg("gvset") 
            if data:
                do(self.cb, data)


g = Consumer("gvset")
g.start()
g.join()
