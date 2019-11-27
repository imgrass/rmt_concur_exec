#!/usr/bin/env python

import os
import sys
import time

sys.path.append(os.path.abspath('../'))
from log_x import LogX
from pub_sub import multi_process, msg_trans_proto


Log = LogX(__name__)
log_file = './log/%s.log' % __file__.split('.')[0]
Log.set_public_atrr(LogX.INFO, log_file)
Log.open_global_stdout()


class unit_test(object):
    '''
    status:
        1. wait:    sub process wait cmd to handle
        2. okay:    sub process handle cmd successfully
        3. fail:    handle failed
        4. end:     pub_func check if the program(loop) should end
    '''
    date = time.time()

    @staticmethod
    def fin_func():
        if time.time() - unit_test.date > 3:
            return True
        return False

    @staticmethod
    def pub_func(fdr, fdw, buf):
        Log.info('pub_func: buf is %s' % buf)
        if buf == 'wait':
            Log.info('..<--pub recv wait')
            msg_trans_proto.write(fdw, 'wait')

        elif buf == 'okay':
            Log.info('..<--pub recv okay')
            msg_trans_proto.write(fdw, 'okay')

        elif buf == 'fail':
            Log.info('..<--pub recv fail')
            msg_trans_proto.write(fdw, 'fail')

        return False


    @staticmethod
    def sub_func(fdr, fdw):

        Log.info('sub process<pid:%d, ppid:%d> inherit two pipe<fdr:%d, fdw:%d>' %
              (os.getpid(), os.getppid(), fdr, fdw))

        Log.info('sub process with <pid:%d> would send <msg:wait>'% os.getpid())
        msg_trans_proto.write(fdw, 'wait')
        rtn = msg_trans_proto.read(fdr, timeout=1)
        Log.info('..-->sub recv %s' % rtn)

        msg_trans_proto.write(fdw, 'okay')
        rtn = msg_trans_proto.read(fdr, timeout=1)
        Log.info('..-->sub recv %s' % rtn)

        msg_trans_proto.write(fdw, 'fail')
        rtn = msg_trans_proto.read(fdr, timeout=1)
        Log.info('..-->sub recv %s' % rtn)

        time.sleep(30)


test = multi_process(5)
test.register_publisher(unit_test.pub_func)
test.register_subscriber(unit_test.sub_func)
test.register_finisher(unit_test.fin_func)
test.start()
