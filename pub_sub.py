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
         |<-okay\\r<host>\\r<result>-|
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
         |<-fail\\r<host>\\r<result>-|
         |                           |
         |---------retry------------>|
         |       ...3 times...       |
         |<-fail\\r<host>\\r<result>-|
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
         |<-fail\\r<host>\\r<result>-|
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
                     ||     @wait:  2#11
                     ||     @hding: 2#10
                     ||     @fail:  2#01
                     ||     @okay:  2#00
                     ||      +-------------------------------+
                     ||      | +------+----+   +------+----+ |
        cmd_lst:     |+----->| |0xffff|cmd1|   |0xffff|cmdn| |
                     +------>| +------+----+   +------+----+ |
                             +-------------------------------+
    '''
    STATUS_WAIT  = 0x03
    STATUS_HDING = 0x02
    STATUS_FAIL  = 0x01
    STATUS_OKAY  = 0x00

    PUB_FLG_IGNORE_FAIL = 0x01

    MAX_RETRIES = 1

    def __init__(self, guest_queue, cmd_lst, concurrency, group=None,
                 mode=0x00):
        # Note that: for simplicity, I use 'list' to format 'guest_queue', the
        #       'dequeue' operator would be replaced by 'list.pop'. So the
        #       first entry to be handled need to be placed at tail.
        #
        #       @concurrency    the number of processes processed in parallel,
        #                       note that it is less or equal than the number
        #                       of actual processes. it is recommended that
        #                       they be equal.
        #
        #       @group          the number of guests received per group. it is
        #                       greater or equal than the concurrency
        self.guest_queue = guest_queue
        self.guest_queue.reverse()
        self.group = group
        if self.group:
            if concurrency > self.group:
                raise Exception('the number of concurrency:%d > group:%d' %
                                (concurrency, self.group))
            self.n_received_guests = 0

        # initialize cmd_lst
        self.concurrency = concurrency
        p_map = 2 ** (2 * self.concurrency) - 1
        self.cmd_lst = [[p_map, cmd] for cmd in cmd_lst]

        # initialize recept_pool
        self.recept_pool = [[id, None] for id in xrange(self.concurrency)]

        # use to handle issue when remote exec cmd failed
        self.mode = mode
        self.n_retries = 0

        # database
        self.db_handler = None

    def __del__(self):
        ## when cls <publisher> exit when __init__, the method <db_handler> is
        #  not existed in self.
        try:
            db_handler = getattr(self, 'db_handler')
            db_handler.commit()
        except AttributeError:
            pass

    def set_db_handler(self, db_handler):
        self.db_handler = db_handler

        # init table <tb_hosts> and <tb_commands>
        # note that <guest_queue> is reversed
        len_guest_queue = len(self.guest_queue)
        for i in xrange(len_guest_queue):
            self.db_handler.put_host(self.guest_queue[len_guest_queue-i-1], 0)

        for p_map, cmd in self.cmd_lst:
            self.db_handler.put_command(cmd)

    def _get_status(self, p_map, p_id):
        status = (p_map & (0x3 << (2 * p_id))) >> (2 * p_id)
        Log.debug('....> status is %d' % status)
        if status is self.STATUS_WAIT:
            return 'wait'
        elif status is self.STATUS_HDING:
            return 'hding'
        elif status is self.STATUS_FAIL:
            return 'fail'
        elif status is self.STATUS_OKAY:
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
        self.cmd_lst[index][0] |= (0x3 << (2 * p_id))

    def _find_next_waitted_cmd(self, p_id):
        Log.debug('..>p_id:%d & cmd_lst is <%s>' % (p_id, str(self.cmd_lst)))
        for p_map, cmd in self.cmd_lst:
            status = self._get_status(p_map, p_id)
            Log.debug('...><p_map:%d> <status:%s> <cmd:%s>' % (p_map, status, cmd))
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
            status = self._get_status(self.cmd_lst[i][0], p_id)
            if status == 'wait'or status == 'hding':
                return i
        return None

    ## add record to database if connected
    # record: <host, cmd, status, result>
    def _record_result(self, host, cmd, status, result):
        if not self.db_handler:
            Log.debug('--<host:%s> <flg:%s> <result:%s>' %
                      (host, status, result))
            return
        Log.info('  record result <host:%s> <cmd:%s> <status:%d> <result:%s>' %
                 (host, cmd, status, result))
        self.db_handler.put_result(host, cmd, status, result)

    def _prompt_group(self):
        lst_host_group = []
        for i in xrange(1, self.group+1):
            try:
                lst_host_group.append(self.guest_queue[-i])
            except IndexError:
                break
        str_host_group = ', '.join(lst_host_group)

        lst_cmds = [cmd for p_map, cmd in self.cmd_lst]
        str_cmds = ', '.join(lst_cmds)

        print(\
            '''\
                \r%s
                \r  hosts: %s
                \r%s
                \r  cmds: %s
                \r%s\
            ''' %
            (64*'*', str_host_group, 64*'-', str_cmds, 64*'*')
            )
        confirm = raw_input('do you confirm to execute those cmds on the '
                            'hosts?(y/Y/all):')
        if confirm in ['y', 'Y']:
            print('...okay, execute now')
            return
        else:
            raise Exception('pub:exit')

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

        if head == 'wait':
            self.hd_connected_wait(host)
        elif head == 'okay':
            result = req.split('\r')[2]
            self.hd_connected_okay(host, result)
        elif head == 'fail':
            result = req.split('\r')[2]
            self.hd_connected_fail(host, result)

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

        # used for group.
        if self.group and self.n_received_guests >= self.group:
            self.n_received_guests = 0
        if self.group and self.n_received_guests is 0:
            self._prompt_group()

        if len(self.guest_queue) > 0:
            new_guest = self.guest_queue.pop()
            if self.group:
                self.n_received_guests += 1
        else:
            Log.info('(^_^)> No guest need to be servered')
            return

        self.recept_pool[p_id][1] = new_guest
        for i in xrange(len(self.cmd_lst)):
            self._set_status_wait(i, p_id)

        mtp.write(self.fdw, 'ack\r%s' % new_guest)
        self.n_retries = 0

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

    def hd_connected_okay(self, host, result):
        p_id = self._get_p_id_by_host(host)
        index = self._get_waitting_cmd_index(host)
        self._set_status_okay(index, p_id)

        self._record_result(host, self.cmd_lst[index][1], self.STATUS_OKAY,
                            result)

        mtp.write(self.fdw, 'okay')
        return

    def hd_connected_fail(self, host, result):
        p_id = self._get_p_id_by_host(host)
        if self.mode & publisher.PUB_FLG_IGNORE_FAIL:
            mtp.write(self.fdw, 'ignore')
            index = self._get_waitting_cmd_index(host)
            self._set_status_fail(index, p_id)
            self._record_result(host, self.cmd_lst[index][1], self.STATUS_FAIL,
                                result)
            return

        if self.n_retries < self.MAX_RETRIES:
            mtp.write(self.fdw, 'retry')
            self.n_retries += 1
            return
        else:
            index = self._get_waitting_cmd_index(host)
            self._record_result(host, self.cmd_lst[index][1], self.STATUS_FAIL,
                                result)
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

        str_buf = stderr.read()
        if len(str_buf):
            Log.warning('  ..@_@.<host:%s> exec <%s> return fail' %
                        (self.host, self.latest_cmd))
            Log.warning('  --> stderr:%s' % str_buf)
            return False, str_buf

        str_buf = stdout.read()
        Log.info('  --> stdout:%s' % str_buf)
        return True, str_buf


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
        Log.debug('sub process <pid:%d> waitting for task...' % os.getpid())
        mtp.write(self.fdw, 'wait')

    def hd_connecting(self):
        while True:
            rsp = mtp.read(self.fdr, timeout=-1)
            head = rsp.split('\r')[0]
            load = rsp.split('\r')[1]

            if head == 'ack' and len(load) > 0:
                self.host = load
                Log.debug('sub process <pid:%d> is connecting <host:%s>...' %
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
            if reply == 'okay' or reply == 'ignore':
                mtp.write(self.fdw, 'wait\r%s' % self.host)
                continue
            elif reply == 'end':
                return
            elif reply == 'cmd':
                self.latest_cmd = mtp.read(self.fdr, timeout=-1)
                Log.debug('  ..*_* subscriber exec <cmd:%s>' % self.latest_cmd)
            elif reply == 'retry':
                pass

            status, str_buf = self._rmt_exec_cmd()
            if status:
                mtp.write(self.fdw, 'okay\r%s\r%s' % (self.host, str_buf))
            else:
                mtp.write(self.fdw, 'fail\r%s\r%s' % (self.host, str_buf))

    def hd_disconnecting(self):
        self.ssh_handler.disconnect_ssh_channel()
