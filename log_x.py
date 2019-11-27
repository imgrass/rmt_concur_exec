import logging
import sys


class LogXInitException(Exception):

    def __init__(self, info):
        self.info = 'LogX init failed due to %s' % info

    def __str__(self):
        return self.info


class LogX(object):

    FATAL = logging.FATAL
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    WARNING = logging.WARNING
    INFO = logging.INFO
    DEBUG = logging.DEBUG

    ## only set once
    pub_log_file = None
    pub_log_level = None
    pub_formatter = logging.Formatter('%(asctime)s %(message)s')

    is_stdout = False

    def __init__(self, name):

        logger = logging.getLogger(name)
        self.logger = logger
        # used to check if public parameters are already set
        self.is_pub_args_set = False
        self.is_stdout_opened = False

    def set_public_atrr(self, log_level, log_file):
        if (LogX.pub_log_level or LogX.pub_log_file) and \
           (log_level or log_file):
            raise LogXInitException('LogX only allow be initialized once, '
                                    '<pub_log_file:%s> and <pub_log_level:%d' %
                                    (LogX.pub_log_file, LogX.pub_log_level))
        LogX.pub_log_file = log_file
        LogX.pub_log_level = log_level
        self._add_pub_file_handler()
        self.is_pub_args_set = True
        self.logger.info('INIT ELOG with <log:%s>...' % self.pub_log_file)

    def open_global_stdout(self):
        LogX.is_stdout = True

    def open_private_stdout(self):
        self.is_stdout = True

    def _add_pub_stdout_handler(self, private_log_level=None):
        console = logging.StreamHandler()
        console.setLevel(private_log_level or self.pub_log_level)
        console.setFormatter(self.pub_formatter)
        self.logger.addHandler(console)

    def _add_pub_file_handler(self):
        if not self.pub_log_file:
            raise LogXInitException('public log file has not been defined!')
        if not self.pub_log_level:
            raise LogXInitException('public log level has not been defined!')

        self.logger.setLevel(self.pub_log_level)
        pub_file_handler = logging.FileHandler(self.pub_log_file)
        pub_file_handler.setFormatter(self.pub_formatter)
        self.logger.addHandler(pub_file_handler)

    def _handler(self, action, info):
        # check if public log file handler is added
        if not self.is_pub_args_set:
            self._add_pub_file_handler()
            self.is_pub_args_set = True

        if self.is_stdout and not self.is_stdout_opened:
            self._add_pub_stdout_handler()
            self.is_stdout_opened = True

        if action not in ('fatal', 'critical', 'error', 'warning', 'info',
                          'debug'):
            raise LogXInitException('the <action:%s> is invalid' % action)

        method = getattr(self.logger, action)

        frame = sys._getframe(2)
        lineno = frame.f_lineno
        code = frame.f_code
        func_name = code.co_name
        file_name = code.co_filename

        msg = '(%s)<%s:%s><line:%d>: %s' % \
              (action.upper(), file_name, func_name, lineno, info)
        method(msg)

    def fatal(self, info):
        self._handler('fatal', info)

    def critical(self, info):
        self._handler('critical', info)

    def error(self, info):
        self._handler('error', info)

    def warning(self, info):
        self._handler('warning', info)

    def info(self, info):
        self._handler('info', info)

    def debug(self, info):
        self._handler('debug', info)


def test():
    LOG = LogX(__name__)
    LOG.set_public_atrr(LogX.INFO, 'a.log')
    LOG.warning("I want to change the world!")
    LOG.info("I want to change the world!")
    LOG.debug("I want to change the world!")

    LOG1 = LogX('xxxx')
    LOG1.open_private_stdout()
    LOG1.info('... open private stdout')


if __name__ == '__main__':
    test()
