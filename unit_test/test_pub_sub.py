#!/usr/bin/env python

import os
import sys
import time

sys.path.append(os.path.abspath('../'))
from log_x import LogX
from concur_handler import multi_process
from pub_sub import publisher, subscriber


Log = LogX(__name__)
log_file = './log/%s.log' % __file__.split('.')[0]
Log.set_public_atrr(LogX.INFO, log_file)
Log.open_global_stdout()


class unit_test(object):
    def __init__(self):
        # ssh argvs
        self.guest_queue = [
                '172.17.0.2',
                '172.17.0.3',
                '172.17.0.4',
                '172.17.0.5',
                '172.17.0.6',
                ]
        self.user = 'root'
        self.key_file = None
        self.password = 'rootroot'

        self.cmd_lst = [
                'date',
                'date1',
                'hostname',
                'cat /etc/hosts',
                ]
        self.concurrency = 5
        self.mode = 0x00
        self.mode |= publisher.PUB_FLG_IGNORE_FAIL

        self.multi_process = multi_process(self.concurrency)
        self.publisher = publisher(self.guest_queue, self.cmd_lst,
                                   self.concurrency, self.mode)
        self.subscriber = subscriber()

    def case(self):
        self.multi_process.register_publisher(self.publisher)
        self.multi_process.register_subscriber(self.subscriber,
                                              self.user,
                                              self.key_file,
                                              self.password)
        self.multi_process.start()


unit_test().case()
