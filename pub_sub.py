'''\
The protocol of publisher and subscriber is shown below:
       +---+                       +---+
       |pub|                       |sub|
       +-+-+                       +-+-+
         |                           |
         |<---------wait-------------| waitting
         |                           |
         |-------ack\\r<host>------->| connecting
         |                           |
         |<------wait\\r<host>-------| connected
         |----------cmd------------->|
         |---------<cmd>------------>|
         |<-----okay\\r<host>--------|
         |---------okay------------->|
         |         ......            |
         |                           |
         |<------wait\\r<host>-------|
         |---------end-------------->| disconnecting
         |                           |
         |<---------wait-------------| waitting


   case 1: ignore failed cmd
         |         ......            |
         |<------wait\\r<host>-------| connected
         |----------cmd------------->|
         |---------<cmd>------------>|
         |<-----fail\\r<host>--------|
         |                           |
         |---------retry------------>|
         |       ...3 times...       |
         |<-----fail\\r<host>--------|
         |---------ignore----------->|
         |                           |
         |<------wait\\r<host>-------|
         |------next cmd------------>|
         |         ......            |


   case 1: end to execute when failed occurred
         |         ......            |
         |<------wait\\r<host>-------| connected
         |----------cmd------------->|
         |---------<cmd>------------>|
         |<-----fail\\r<host>--------|
         |                           |
         |---------retry------------>|
         |       ...3 times...       |
         |<-----fail\\r<host>--------|
         |-----------end------------>| disconnecting
         |                           |
         |<---------wait-------------| waitting
\r'''

from log_x import LogX
from concur_handler import msg_trans_proto as mtp
from ssh_handler import ssh_handler

import os


Log = LogX(__name__)

class publisher(object):
    '''
          inqueue--->+------------------------------+
                     | +--+  +--+  +--+  +--+  +--+ |  dequeue
        guest_queue: | |g1|  |g2|  |g3|  |..|  |gn| |-----------+
                     | +--+  +--+  +--+  +--+  +--+ |           |
                     +------------------------------+           |
                                                                |
                     +------------------------------+           |
                     |  p1    p2    p3    ..    pn  |           |
                     | +--+  +--+  +--+  +--+  +--+ |           |
        recept_pool: | |id|  |id|  |id|  |id|  |id| |<----------+
                     | +--+  +--+  +--+  +--+  +--+ | allocate reception
                     | |G1|  |G2|  |G3|  |G.|  |Gn| | process for guest
                     | +--+  +--+  +--+  +--+  +--+ |
                     +------------------------------+
                     ||  every process exec cmd in cmd_lst in order, and
                     ||  modify map by response of executing cmd, every
                     ||  process has four status:
                     ||     @wait:  0x11
                     ||     @hding: 0x10
                     ||     @fail:  0x01
                     ||     @okay:  0x00
                     ||      +-------------------------------+
                     ||      | +------+----+   +------+----+ |
        cmd_lst:     |+----->| |0xffff|cmd1|   |0xffff|cmdn| |
                     +------>| +------+----+   +------+----+ |
                             +-------------------------------+
    '''
    PUB_FLG_IGNORE_FAIL = 0x01

    MAX_RETRIES = 1

    def __init__(self, guest_queue, cmd_lst, concurrency, mode=0x00):
        # Note that: for simplicity, I use 'list' to format 'guest_queue', the
        #       'dequeue' operator would be replaced by 'list.pop'. So the
        #       first entry to be handled need to be placed at tail.
        self.guest_queue = guest_queue
        self.guest_queue.reverse()
        Log.info('---> self.guest_queue is %s' % str(self.guest_queue))

        # initialize cmd_lst
        self.concurrency = concurrency
        p_map = 2 ** (2 * self.concurrency) - 1
        self.cmd_lst = [[p_map, cmd] for cmd in cmd_lst]

        # initialize recept_pool
        self.recept_pool = [[id, None] for id in xrange(self.concurrency)]

        # use to handle issue when remote exec cmd failed
        self.mode = mode
        self.n_retries = 0

    def _get_status(self, p_map, p_id):
        status = p_map & (0x3 << (2 * p_id))
        if status & 0x3:
            return 'wait'
        elif status & 0x02:
            return 'hding'
        elif status & 0x01:
            return 'fail'
        elif status & 0x00:
            return 'okay'

    def _set_status_okay(self, index, p_id):
        self.cmd_lst[index][0] &= (~(0x3 << (2 * p_id)))

    def _set_status_fail(self, index, p_id):
        self.cmd_lst[index][0] &= (~(0x1 << (2 * p_id + 1)))
        self.cmd_lst[index][0] |= (0x1 << (2 * p_id))

    def _set_status_hding(self, index, p_id):
        self.cmd_lst[index][0] &= (~(0x1 << (2 * p_id)))
        self.cmd_lst[index][0] |= (0x1 << (2 * p_id + 1))

    def _set_status_wait(self, index, p_id):
        self.cmd_lst[index][0] &= (0x3 << (2 * p_id))

    def _find_next_waitted_cmd(self, p_id):
        for p_map, cmd in self.cmd_lst:
            status = self._get_status(p_map, p_id)
            if status == 'wait':
                return (False, cmd)
            elif status == 'hding':
                return (True, cmd)
        return (False, None)

    def _get_p_id_by_host(self, host):
        for p_id, guest in self.recept_pool:
            if guest == host:
                return p_id
        return None

    def _get_waitting_cmd_index(self, host):
        p_id = self._get_p_id_by_host(host)
        for i in xrange(len(self.cmd_lst)):
            if self._get_status(self.cmd_lst[i][0], p_id) == 'wait':
                return i
        return None

    def handler(self, fdr, fdw):
        self.fdr = fdr
        self.fdw = fdw

        req = mtp.read(fdr)
        if req == 'wait':
            # allocate host to this process
            self.hd_waitting()
            return

        head = req.split('\r')[0]
        host = req.split('\r')[1]
        Log.info('  ..&_& publisher recieve <reqest:%s:%s>' % (head, host))

        if head == 'wait':
            self.hd_connected_wait(host)
        elif head == 'okay':
            self.hd_connected_okay(host)
        elif head == 'fail':
            self.hd_connected_fail(host)

    # check if main loop need to be break
    def fin_func(self):
        if len(self.guest_queue) > 0:
            return False

        for p_id, guest in self.recept_pool:
            if guest:
                return False

        return True

    def hd_waitting(self):
        if len(self.cmd_lst) == 0:
            return

        # find free process in recept_pool
        for p_id, host in self.recept_pool:
            if not host:
                break
        if len(self.guest_queue) > 0:
            new_guest = self.guest_queue.pop()
        else:
            Log.info('(^_^)> No guest need to be servered')
            return

        self.recept_pool[p_id][1] = new_guest
        for i in xrange(len(self.cmd_lst)):
            self._set_status_wait(i, p_id)

        mtp.write(self.fdw, 'ack\r%s' % new_guest)

    def hd_connected_wait(self, host):
        # find which process recept this guest
        p_id = self._get_p_id_by_host(host)

        is_hding, next_waitted_cmd = self._find_next_waitted_cmd(p_id)
        if is_hding:
            return

        if next_waitted_cmd:
            mtp.write(self.fdw, 'cmd')
            mtp.write(self.fdw, next_waitted_cmd)
            index = self._get_waitting_cmd_index(host)
            self._set_status_hding(index, p_id)
        else:
            Log.info('  ..(^_^)<host:%s> exec all cmds completely!' % host)
            mtp.write(self.fdw, 'end')
            self.recept_pool[p_id][1] = None

    def hd_connected_okay(self, host):
        p_id = self._get_p_id_by_host(host)
        index = self._get_waitting_cmd_index(host)
        Log.info('-=-> <%s> <p_map:%d> <p_id:%d>' %
                 (str(self.cmd_lst), index, p_id))
        self._set_status_okay(index, p_id)
        Log.info('-=-> <%s> <p_map:%d> <p_id:%d>' %
                 (str(self.cmd_lst), index, p_id))
        self._set_status_okay(index, p_id)
        mtp.write(self.fdw, 'okay')
        return

    def hd_connected_fail(self, host):
        p_id = self._get_p_id_by_host(host)
        if self.mode & publisher.PUB_FLG_IGNORE_FAIL:
            mtp.write(self.fdw, 'ignore')
            index = self._get_waitting_cmd_index(host)
            self._set_status_fail(index, p_id)

        if self.n_retries < self.MAX_RETRIES:
            mtp.write(self.fdw, 'retry')
            self.n_retries += 1
            return
        else:
            mtp.write(self.fdw, 'end')
            self.recept_pool[p_id][1] = None


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

    def _rmt_exec_cmd(self):
        Log.info('    @<pid:%d><host:%s> exec <%s>' %
                  (os.getpid(), self.host, self.latest_cmd))
        stdout, stderr = self.ssh_handler.exec_cmd(self.latest_cmd)

        err_info = stderr.read()
        if len(err_info):
            Log.warning('  ..@_@.<host:%s> exec <%s> return fail' %
                        (self.host, self.latest_cmd))
            Log.warning('  --> stderr:\n%s' % err_info)
            return False

        Log.info('  --> stdout:\n%s' % stdout.read())
        return True


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
        # if connecting failed, exiting this sub process
        self.ssh_handler.create_ssh_channel(**kwargs)
        Log.info('<pid:%d> connected <host:%s> successfully!' %
                 (os.getpid(), self.host))

        mtp.write(self.fdw, 'wait\r%s' % self.host)
        while True:
            reply = mtp.read(self.fdr, timeout=-1)
            Log.info('  ..*_* subscriber recieved <reply:%s>' % reply)
            if reply == 'okay' or reply == 'retry':
                mtp.write(self.fdw, 'wait\r%s' % self.host)
                continue
            elif reply == 'end':
                return
            elif reply == 'cmd':
                self.latest_cmd = mtp.read(self.fdr, timeout=-1)
                Log.info('  ..*_* subscriber exec <cmd:%s>' % self.latest_cmd)
            elif reply == 'ignore':
                pass

            if self._rmt_exec_cmd():
                Log.info('  ..*_* subscriber send okay')
                mtp.write(self.fdw, 'okay\r%s' % self.host)
            else:
                Log.info('  ..*_* subscriber send fail')
                mtp.write(self.fdw, 'fail\r%s' % self.host)

    def hd_disconnecting(self):
        self.ssh_handler.disconnect_ssh_channel()
