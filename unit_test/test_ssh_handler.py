#!/usr/bin/env python

import os
import sys

sys.path.append(os.path.abspath('../'))
from log_x import LogX
from ssh_handler import ssh_handler


Log = LogX(__name__)
log_file = './log/%s.log' % __file__.split('.')[0]
Log.set_public_atrr(LogX.INFO, log_file)
Log.open_global_stdout()


class unit_test(object):
    def case1(self):
        kwargs = {
                'addr': '',
                'port': 22,
                'username': '',
                'key_filename': '',}
        self.handler = ssh_handler()
        self.handler.create_ssh_channel(**kwargs)

        stdout, stderr = self.handler.exec_cmd('hostname')
        if len(stderr.read()):
            Log.error('--> %s' % stderr.read())
        Log.info('output--> %s' % stdout.read())

    def case2(self):
        self.handler.disconnect_ssh_channel()
        kwargs = {
                'addr': '',
                'port': 22,
                'username': '',
                'key_filename': '',}
        self.handler.create_ssh_channel(**kwargs)

        stdout, stderr = self.handler.exec_cmd('hostname')
        if len(stderr.read()):
            Log.error('--> %s' % stderr.read())
        Log.info('output--> %s' % stdout.read())


test=unit_test()
test.case1()
test.case2()
