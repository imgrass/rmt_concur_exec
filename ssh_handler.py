from log_x import LogX
from sys import exit
import paramiko

LOG = LogX(__name__)


class ssh_handler(object):
    '''
        \rUsed to create ssh channel and get the output by executing cmd in
        \rremote host
    '''
    def __init__(self):
        self.trans = None

    def create_ssh_channel(self, **kwargs):
        if self.trans:
            self.trans.close()
        try:
            self.addr = kwargs['addr']
            self.port = kwargs.get('port', 22)
            self.username = kwargs['username']
            self.key_filename = kwargs.get('key_filename', None)
            self.password = kwargs.get('password', None)
        except KeyError as e:
            LOG.error('parse kwargs failed with exception: %s' % e)
            LOG.debug('the kwargs is %s' % kwargs)
            exit(1)

        LOG.info('ssh %s@%s:%d' % (self.username, self.addr, self.port))
        if not self.key_filename and not self.password:
            LOG.warning('permission denied, please provide password or key')
            exit(1)

        try:
            trans = paramiko.Transport((self.addr, self.port))
            if self.password:
                trans.connect(username=self.username, password=self.password)
            else:
                pkey = paramiko.RSAKey.from_private_key_file(
                        self.key_filename,
                        password=self.password)
                trans.connect(username=self.username, pkey=pkey)
        except Exception as e:
            LOG.error("create ssh connection failed due to: <class:%s> %s" %
                      (e.__class__, e))
            try:
                trans.close()
            except:
                pass
            exit(1)
        self.trans = trans

    def exec_cmd(self, cmd, timeout=None):
        chan = self.trans.open_session(timeout=timeout)
        chan.settimeout(timeout)
        chan.exec_command(cmd)
        stdout = chan.makefile('r', -1)
        stderr = chan.makefile_stderr('r', -1)
        return stdout, stderr

    def disconnect_ssh_channel(self):
        self.trans.close()
