from log_x import LogX
import errno
import fcntl
import os
import resource
import select
import signal
import string
import sys
import time

Log = LogX(__name__)


class pipe_link_exception(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info


class msg_trans_proto(object):
    '''
    Msg format:
        +----------------------------+
        |\0...|*|size|\0| ...load... |
        +----------------------------+
                ^
                +--> the length of load, two bits(0x00 < len < 0xff(256))
    assume bit of len is 2, msg is 'hello, world!', then show example as followed:

        *head       '\0\0'
        *separator  '*'
        *size       '0d'
        *load       'hello, world!'
                +-----------------------+
        msg --> |\0\0|*|0d|hello, world!|
                +-----------------------+
    '''

    BITS_OF_LEN = 2
    ## one bits represent one hex number 'f'
    MAX_SIZE = 16 ** BITS_OF_LEN - 1

    @classmethod
    def _read(cls, fdr, is_nonblock):
        # set fdr non-blocking if is_nonblock is True
        if is_nonblock:
            fl = fcntl.fcntl(fdr, fcntl.F_GETFL)
            fcntl.fcntl(fdr, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        # step 1: find head '\0...', number is BITS_OF_LEN
        n_continue_zero = 0
        while True:
            latest_byte = os.read(fdr, 1)
            if latest_byte == '\0':
                n_continue_zero += 1
            elif n_continue_zero < cls.BITS_OF_LEN:
                n_continue_zero = 0
            elif latest_byte == '*':
                break
            else:
                raise Exception('the format of pipe msg is error!')

        # step 2: parse len from flow
        size = os.read(fdr, cls.BITS_OF_LEN)
        size_oct = int(size, 0x10)

        # step 3: parse packet load from flow
        load = os.read(fdr, size_oct)
        return load

    @classmethod
    def read(cls, fdr, timeout=0):
        start_time = time.time()

        is_nonblock = True
        if timeout < 0:
            is_nonblock = False

        while True:
            try:
                #return cls._read(fdr, is_nonblock)
                buf = cls._read(fdr, is_nonblock)
                Log.debug('..-read-<pid:%s> read <%s>' % (os.getpid(), buf))
                return buf
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    pass
                else:
                    err_msg = '<fdr:%d> read pipe failed due to <%d:%s>' % \
                              (e.errno, e.strerror)
                    Log.error(err_msg)
                    raise pipe_link_exception(err_msg)

            eplased_time = time.time() - start_time
            if eplased_time >= timeout:
                return None

    @classmethod
    def write(cls, fdw, load):
        if len(load) > cls.MAX_SIZE:
            raise Exception('the size of load is larger than <limit:0x%.2X'
                            % cls.MAX_SIZE)
        head = ''.rjust(cls.BITS_OF_LEN, '\0')
        separator = '*'
        size_origin = '%X' % len(load)
        size = size_origin.zfill(cls.BITS_OF_LEN)
        msg = '%s%s%s%s' % (head, separator, size, load)
        os.write(fdw, msg)
        Log.debug('..-write-<pid:%s> write <%s>' % (os.getpid(), msg))


class multi_process(object):
    '''
    This is publisher-subscriber system
    +----------------------------------+  <--handle --<sub_func>
    |  p1       p2       p3       pn   |
    | +---+    +---+    +---+    +---+ |
    | |sub|    |sub|    |sub|    |sub| |  one sub is one process
    | +---+    +---+    +---+    +---+ |
    +----------------------------------+
       +-----+   |pipe ^
       |epoll|   |msg  | msg_trans_proto
       +-----+   v     |
    +----------------------------------+  <--handle --<pub_func>
    |               pub                |
    +----------------------------------+  <--end loop --<fin_func>
    '''

    MAX_CONCURRENCY = 32
    TIMEOUT = 0

    def __init__(self, concurrency, timeout=None):

        if concurrency > self.MAX_CONCURRENCY:
            Log.error('number of concurrency is larger than the limit %d' %
                      self.MAX_CONCURRENCY)
            sys.exit(1)
        self.concurrency = concurrency
        self.process_pool = {}

        if timeout:
            self.TIMEOUT = timeout
        self.epoll = select.epoll()

        # Note:
        #   *pub_func:      publisher --> publish msgs according the request
        #                   of subscriber.
        #   args ...
        #       @read       fd for reading <--from-- (sub_func.write)
        #       @write      fd for writing --to--> (sub_func.read)
        #       @buf        the request or response buf coming from subscriber
        #       @*argv      other args
        #       @**kwargs   ...
        #   *sub_func:      subscriber --> send request and then subscribe
        #                   msgs and handle them
        #   args ...
        #       @read       fd for reading <--from-- (pub_func.write)
        #       @write      fd for writing --to--> (pub_func.read)
        #       @*argv      other args
        #       @**kwargs   ...

        self.publisher = None
        self.pub_func = None
        self.pub_func_argv = None
        self.pub_func_kwargs = None

        self.subscriber = None
        self.sub_func = None
        self.sub_func_argv = None
        self.sub_func_kwargs = None

        self.fin_func = None

    def _exit(self):
        self._exit_process_in_pool()
        Log.info('..(&.&).. end monitor process with <pid:%d>' % os.getpid())
        # TODO: do clean up
        sys.exit(0)

    def register_publisher(self, obj_pub, *argv, **kwargs):
        self.publisher = obj_pub
        self.pub_func = obj_pub.handler
        self.pub_func_argv = argv
        self.pub_func_kwargs = kwargs

        self.fin_func = obj_pub.fin_func

    def register_subscriber(self, obj_sub, *argv, **kwargs):
        self.subscriber = obj_sub
        self.sub_func = obj_sub.handler
        self.sub_func_argv = argv
        self.sub_func_kwargs = kwargs

    def _create_new_pipe_pair(self):
        pr, cw = os.pipe()
        cr, pw = os.pipe()

        try:
            pid = os.fork()
        except OSError as e:
            raise Exception('fork failed due to %s:%s' %
                            (e.errno, e.strerror))

        if pid == 0:
            # for child process
            os.close(pr)
            os.close(pw)
            self.sub_func(cr, cw, *self.sub_func_argv, **self.sub_func_kwargs)

            Log.error('sub process should not run here')
            sys.exit(1)
        else:
            # for parent process
            Log.info('  (^_^) create sub process successfully! With <pid:%d>'
                     'and two pipe pw(%d)->cr(%d) and cw(%d)->pr(%d)' %
                     (pid, pw, cr, cw, pr))
            os.close(cr)
            os.close(cw)
            self.process_pool[pr] = {'pid':pid, 'pw':pw}
            self.epoll.register(pr, select.EPOLLIN)

    def _restore_pipe(self, fdr):
        Log.info('--> try to restore pipe by create new sub process')
        self.epoll.unregister(fdr)

        pid = self.process_pool[fdr]['pid']
        os.kill(pid, signal.SIGKILL)
        self.process_pool.pop(fdr)

        self._create_new_pipe_pair()

    def start(self):
        self._init_process_in_pool()
        # TODO: when pipe disconnected, it need to restore it by restarting
        #       new process

        # to monitor which pair of pipe(fdr/fdw) is using when pipe breaking
        self.runtime_pr = None
        while True:
            try:
                self._manage_process_pool()
            except OSError as e:
                if e.errno == errno.EPIPE:
                    Log.warning('Pipe<pr:%d> disconnected with sub process'
                                '<pid:%d> due to %s' %
                                (self.runtime_pr,
                                 self.process_pool[self.runtime_pr]['pid'],
                                 e.strerror))
                    self._restore_pipe(self.runtime_pr)
                else:
                    Log.error('Unexception failed!<%d:%s>' %
                              (e.errno, e.strerror))
                    self._exit()

    def _manage_process_pool(self):
        while True:
            # the time of breaking main loop is determined by publisher
            if self.fin_func():
                self._exit()
                break

            events = self.epoll.poll(self.TIMEOUT)
            if not events:
                continue;

            for pr, event in events:
                self.runtime_pr = pr
                if event & select.EPOLLIN:
                    pw = self.process_pool[pr]['pw']
                    self.pub_func(pr, pw, *self.pub_func_argv,
                                  **self.pub_func_kwargs)

    def _exit_process_in_pool(self):
        for pr in self.process_pool.keys():
            pid = self.process_pool[pr]['pid']
            pw = self.process_pool[pr]['pw']
            Log.info('...end process <pid:%d>' % pid)
            os.kill(pid, signal.SIGKILL)
            os.close(pr)
            os.close(pw)

    def _init_process_in_pool(self):
        Log.info('init process poll with concurrency:%d' % self.concurrency)
        for i in xrange(self.concurrency):
            self._create_new_pipe_pair()

    def daemonize(self):
        Log.info('Daemonize process...')
        try:
            pid = os.fork()
        except OSError as e:
            raise Exception('fork failed due to %s:%s' % (e.errno, e.strerror))

        if pid == 0:
            os.setsid()
            signal.signal(signal.SIGHUP, signal.SIG_IGN)

            try:
                pid = os.fork()
            except OSError as e:
                raise Exception('fork failed due to %s:%s' %
                                (e.errno, e.strerror))

            if pid == 0:
                os.chdir('/')
                os.umask(0)
            else:
                os._exit(0)
        else:
            os._exit(0)

        # it was originally necessary to turn off all fds to avoid side-effect
        # but I wish this process can inherite fd open by 'Log'
        return 0
