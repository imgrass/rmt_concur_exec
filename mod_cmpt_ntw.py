#!/usr/bin/env python
# -*- coding: utf8 -*-

import signal
import os
import re
import threading, getopt, sys, string, time
from socket import gethostname
from time import sleep
from time import gmtime
from commands import getstatusoutput

try:
    from ssh_handler import ssh_handler
except ImportError:
    print 'import ssh_handler failed! pls check the existence of this module'
    sys.exit(1)

record_file_name = '.cmpt_status_record.eouylei'


def set_host_status(host, status):
    rtn ,old_status = getstatusoutput("sed 's/^%s:\([a-z]*\)$/\1/g %s" % (host, record_file_name))
    print('--> update status of host from <%s> --> <%s>' % (old_status, status))

    rtn, output = getstatusoutput("sed -i 's/^\(%s:\)[a-z]*$/\1:%s/g %s" % (host, status, record_file_name))
    if rtn != 0:
        print('  ..(>_<!) update status of <host:%s> failed!' % host)


def sig_handler(sig, frame):
    print('---> receive a signal %d, exit' % (sig))
    exit(1)

def usage():
    print """
    \r-h --help         print the help
    \r-m --mode         [manual/auto] default is manual
    \r-i --interval     default is 2 seconds
    \r-c --concurrency  default is 10 threads
    """

def parse_argv():
    mode = 'manual'
    interval = 2
    concurrency = 10

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hm:i:c:", ["help", "mode=", "interval=", "concurrency="])
        for op, value in opts:
            if op in ("-m", "--mode") and value in ("manual", "auto"):
                mode = value
            elif op in ("-i", "--interval"):
                interval = string.atoi(value)
            elif op in ("-c", "--concurrency"):
                concurrency = string.atoi(value)
            elif op in ("-h", "--help"):
                usage()
                sys.exit()
    except Exception as e:
        print '--> parse argv failed due to Exception: %s' % e
        usage()
        exit(1)

    print("== run with <mode:%s> <interval:%d> <concurrency:%d>" % (mode, interval, concurrency))
    return (mode, interval, concurrency)

def wait_for_all_thread_exit(thread_pool):
    while True:
        alive = False
        for thread in thread_pool:
            alive = alive or thread.isAlive()
        if not alive:
            break

def concur_handler(host_set):
    ## host_set is a set of threads who needed to be handled
    thread_pool = []
    print('--> start to handle these hosts:')
    for host in host_set:
        host_name = '%s' % host[0]
        th_name = 'th_%s' % host_name
        print('  ..for <host:%s> do <thread:%s>' % (host_name, th_name))
        thread = threading.Thread(target = do_mod_cmpt_ntw, args = (host_name, ), name = th_name)
        thread.setDaemon(True)
        thread_pool.append(thread)
        thread.start()
    wait_for_all_thread_exit(thread_pool)
    print('(^_^)> handle %d thread over!' % len(host_set))

def host_status_record(mode):
    # test file is existed or not, if not, create it
    # file format:
    #   host1   wait/okay/fail
    #       * wait      this host is not modified
    #       * okay      this host is modified successfully
    #       * fail      this host is modified failed
    if os.access(record_file_name, os.F_OK):
        print('record file <%s> is exist, we would read status of host from here' % record_file_name)
        return

    try:
        file_obj = open(record_file_name, 'w')
    except Exception as e:
        print('create record file failed due to %s' % e)
        sys.exit(1)

    if "fuel" not in gethostname():
        print('(>_<!) the host is not fuel, we can not process here!')
        sys.exit(1)

    status, output = getstatusoutput("fuel node")
    for record in output.split('\n'):
        #print('--> debug: record is %s' % record)
        searchobj = re.search(r'.*\| (compute-[0-9]*-[0-9]*) .*\|.*', record)
        if searchobj:
            fmt_record = '%s:wait\n' % searchobj.group(1)
            #print('--> debug: fmt_record is %s' % fmt_record)
            file_obj.write(fmt_record)

    file_obj.close()


def get_host_poll(record_file_name):
    host_pool = []
    hostname = ''
    status = ''

    with open(record_file_name, 'r') as fp:
        for line in fp:
            searchobj = re.search(r'(compute-[0-9]*-[0-9]*):([a-z]*)', line)
            hostname = searchobj.group(1)
            status = searchobj.group(2)
            host_pool.append((hostname, status))

    return host_pool

def generate_host_set_from_pool(host_pool, concurrency):
    host_set = []
    for host in host_pool:
        if len(host_set) >= concurrency:
            print('--> host_set is %s' % host_set)
            yield host_set
            host_set = []
        if host[1] != 'okay':
            host_set.append(host)

    if len(host_set) > 0:
        print('--> host_set is %s' % host_set)
        yield host_set


def do_mod_cmpt_ntw(host):
    kwargs = {
            'addr': host,
            'username': 'root',
            'key_filename': '/root/.ssh/id_rsa',
            }
    try:
        handler = ssh_handler(**kwargs)
        print('handler is %s' % handler)
    except Exception as e:
        print('  ..(>_<!)> ssh connect <host:%s> failed' % host)
        set_host_status(host, 'fail')
        sys.exit(1)

    try:
        tm = gmtime()
        suffix = "%d_%d_%d_%d_%d" % \
                 (tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)

        stdout, stderr = handler.exec_cmd("date")

        # handler.exec_cmd("cp %s %s.bak.%s" % ("/etc/network/interfaces.d/ifcfg-br-prv",
        #                                       "/etc/network/interfaces.d/ifcfg-br-prv",
        #                                       suffix))
        # stdout, stderr = handler.exec_cmd("sed -i '/^\(iface br-prv inet \)manual.*/\1static/g %s" %
        #         "/etc/network/interfaces.d/ifcfg-br-prv")
        # if not stderr:
        #     print('  ..(^_^)> modify <host:%s> successfully!' % host)
        #     set_host_status(host, 'okay')
        # else:
        #     print('  ..(^_^)> modify <host:%s> failed!' % host)
        #     set_host_status(host, 'fail')
        if len(stderr.read()) is 0:
            print('  --output is <%s>' % stdout.read().strip('\n'))
        else:
            print('  ..(>_<) exec cmd on <host:%s> failed due to %s' % (host, stderr.read().strip('\n')))

    except Exception as e:
        print('  ..(>_<!)> ssh remote executed cmd on <host:%s> failed due to %s' % (host, e))
        sys.exit(1)


def handle_interval(mode, interval):
    if mode == 'manual':
        sys.exit(0)
    elif mode == 'auto':
        sleep(interval)

def main():
    #set signal handler
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    mode, interval, concurrency = parse_argv()

    host_status_record(mode)
    host_pool = get_host_poll(record_file_name)

    for host_set in generate_host_set_from_pool(host_pool, concurrency):
        concur_handler(host_set)
        handle_interval(mode, interval)

if __name__ == '__main__':
    main()
