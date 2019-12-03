#!/usr/bin/env python

import os
import sys
import time

sys.path.append(os.path.abspath('../'))
from log_x import LogX
from db_handler import db_handler


Log = LogX(__name__)
log_file = './log/%s.log' % __file__.split('.')[0]
Log.set_public_atrr(LogX.INFO, log_file)
Log.open_global_stdout()


class unit_test(object):
    def case(self):
        #suf_tm = time.strftime('%y-%m-%d-%H-%M-%S', time.gmtime())
        db_name = 'log/test_.db'
        hdr = db_handler(db_name, is_replace=True)

        hosts = ['host1', 'host2', 'host3']
        statuses = [0, 0, 0]
        for host, status in zip(hosts, statuses):
            hdr.put_host(host, status)

        commands = ['date', 'ls', 'cat /etc/hosts']
        for command in commands:
            hdr.put_command(command)

        for host in hosts:
            for command in commands:
                hdr.put_result(host, command, 0, 'xxxxx')
        hdr.commit()

        rhosts = hdr.get_hosts()
        rcmds = hdr.get_cmds()
        rresults = hdr.get_results()

        print('--> hosts are %s' % str(rhosts))
        print('--> cmds are %s' % str(rcmds))
        print('--> rresults are %s' % str(rresults))

unit_test().case()
