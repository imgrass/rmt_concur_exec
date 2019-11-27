# The protocol of publisher and subscriber is shown below:
#        +---+                       +---+
#        |pub|                       |sub|
#        +-+-+                       +-+-+
#          |                           |
#          |<---------wait-------------| waitting
#          |                           |
#          |--------ack\r<host>------->| connecting
#          |                           |
#          |<-------wait\r<host>-------| connected
#          |----------cmd------------->|
#          |<------okay\r<host>--------|
#          |---------okay------------->|
#          |         ......            |
#          |                           |
#          |<-------wait\r<host>-------|
#          |---------end-------------->| disconnecting
#          |                           |
#          |<---------wait-------------| waitting
#
#
#    case 1: ignore failed cmd
#          |         ......            |
#          |<-------wait\r<host>-------| connected
#          |----------cmd------------->|
#          |<------fail\r<host>--------|
#          |                           |
#          |---------retry------------>|
#          |       ...3 times...       |
#          |<------fail\r<host>--------|
#          |---------ignore----------->|
#          |                           |
#          |<-------wait\r<host>-------|
#          |------next cmd------------>|
#          |         ......            |
#
#
#    case 1: end to execute when failed occurred
#          |         ......            |
#          |<-------wait\r<host>-------| connected
#          |----------cmd------------->|
#          |<------fail\r<host>--------|
#          |                           |
#          |---------retry------------>|
#          |       ...3 times...       |
#          |<------fail\r<host>--------|
#          |-----------end------------>| disconnecting
#          |                           |
#          |<---------wait-------------| waitting
from log_x import LogX
from pub_sub import msg_trans_proto as mtp
from ssh_handler import ssh_handler

import os


Log = LogX(__name__)

class publisher(object):
    def __init__(self):
        pass

    def handler(self, fdr, fdw):
        self.fdr = fdr
        self.fdw = fdw

        req = mtp.read(fdr)
        if req == 'wait':
            self.hd_waitting()
            return

        head = req.split('\r')[0]
        host = req.split('\r')[1]

        if head == 'wait':
            self.hd_connected_wait(host)
        elif head == 'okay':
            self.hd_connected_okay(host)
        elif head == 'fail':
            self.hd_connected_fail(host)

    def hd_waitting(self):
        pass

    def hd_connected_wait(self, host):
        pass

    def hd_connected_wait(self, host):
        pass

    def hd_connected_wait(self, host):
        pass


class subscriber(object):
    def __init__(self):
        # used for ssh loading
        self.host = None
        self.port = None
        self.user = None
        self.key_file = None
        self.password = None
        self.ssh_handler = ssh_handler()

        self.fdr = None
        self.fdw = None
        self.latest_cmd = None

    def handler(self, fdr, fdw, user, key_file, password, port=22):
        self.fdr = fdr
        self.fdw = fdw
        self.user = user
        self.key_file = key_file
        self.password = password
        self.port = port

        while True:
            self.hd_waitting()
            self.hd_connecting()
            self.hd_connected()
            self.hd_disconnecting()

    def hd_waitting(self):
        Log.info('sub process <pid:%d> waitting for task...' % os.getpid())
        mtp.write(self.fdw, 'wait')

    def hd_connecting(self):
        while True:
            rsp = mtp.read(self.fdr, timeout=-1)
            head = rsp.split('\r')[0]
            load = rsp.split('\r')[1]

            if head == 'ack' and len(load) > 0:
                self.host = load
                Log.info('sub process <pid:%d> is connecting <host:%s>...' %
                         (os.getpid(), self.host))
                return

            self.hd_waitting()

    def hd_connected(self):
        kwargs = {
                'addr':         self.host,
                'port':         self.port,
                'username':     self.user,
                'key_filename': self.key_file,
                'password':     self.password,
                }
        self.ssh_handler.create_ssh_channel(**kwargs)
        Log.info('<pid:%d> connected <host:%s> successfully!' %
                 (os.getpid(), self.host))

        mtp.write(self.fdw, 'wait\r%s' % self.host)
        self.latest_cmd = mtp.read(self.fdr, timeout=-1)
        while True:
            if self._rmt_exec_cmd():
                mtp.write(self.fdw, 'okay\r%s' % self.host)
            else:
                mtp.write(self.fdw, 'fail\r%s' % self.host)

            reply = mtp.read(self.fdr, timeout=-1)
            if reply == 'okay' or reply == 'ignore':
                mtp.write(self.fdw, 'wait\r%s' % self.host)
                self.latest_cmd = mtp.read(self.fdr, timeout=-1)
                continue
            elif reply == 'retry':
                continue
            elif reply == 'end':
                return

    def hd_disconnecting(self):
        self.ssh_handler.disconnect_ssh_channel()
