#!/usr/bin/env python
import string
import os
import getopt, sys

sys.path.append(os.path.abspath('../'))
from log_x import LogX
from concur_handler import multi_process
from pub_sub import publisher, subscriber
from db_handler import db_handler


Log = LogX(__name__)
log_file = './%s.log' % __file__.split('.')[0]
Log.set_public_atrr(LogX.INFO, log_file)
Log.open_global_stdout()


def usage():
    print """
    \r-h --help         print the help
    \r-c --concurrency  default is 1 workers(processes)
    \r-g --group        default not set group. setting group meanings you
    \r                  should confirm every group of hosts to exec cmds

    \r-o --hosts        file that defines the hostnames
    \r-m --commands     file that defines the commands

    \r-u --user         for ssh load
    \r-k --keyfile      ssh key file. (/path/to/.ssh/id_rsa)
    \r-p --password     password of ssh
    """


def exit_with_info(info):
    print('--(>_<): exit due to %s' % info)
    sys.exit(1)


def check_parameters_integrity(parameters):
    if not parameters['hosts']or \
       not parameters['commands']or \
       not parameters['user']:
        exit_with_info('host, commands and user can not be None!')

    if not parameters['keyfile'] and not parameters['password']:
        print('attention no permission, both keyfile and password are None')


def parse_argv():
    ## set default value:
    parameters = {
        'concurrency' : 1,
        'group' : None,

        'hosts' : None,
        'commands' : None,

        'user' : None,
        'keyfile' : None,
        'password' : None,
            }

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:g:o:m:u:k:p:",
                                   ["help", "concurrency=", "group=", "hosts=",
                                    "commands=", "user=", "keyfile=",
                                    "password="])
        for op, value in opts:
            if op in ("-h", "--help"):
                usage()
                sys.exit()
            elif op in ("-c", "--concurrency"):
                parameters['concurrency'] = string.atoi(value)
            elif op in ("-g", "--group"):
                parameters['group'] = string.atoi(value)
            elif op in ("-o", "--hosts"):
                hosts = value
                parameters['hosts'] = hosts
                if not os.access(hosts, os.F_OK):
                    exit_with_info('hosts file %s is not existed!' % hosts)
            elif op in ("-m", "--commands"):
                commands = value
                parameters['commands'] = commands
                if not os.access(commands, os.F_OK):
                    exit_with_info('commands file %s is not existed!' %
                                   commands)
            elif op in ("-u", "--user"):
                parameters['user'] = value
            elif op in ("-k", "--keyfile"):
                keyfile = value
                parameters['keyfile'] = keyfile
                if not os.access(keyfile, os.F_OK):
                    exit_with_info('keyfile file %s is not existed!' % keyfile)
            elif op in ("-p", "--password"):
                parameters['password'] = value
            else:
                usage()
                exit_with_info('can not handle this request "%s"' % op)
    except Exception as e:
        usage()
        exit_with_info('%s' % e)

    check_parameters_integrity(parameters)
    return parameters


def get_host_pool(hosts):
    host_pool = []

    with open(hosts, 'r') as fp:
        for line in fp:
            host_pool.append(line.strip('\n'))
    return host_pool


def get_command_pool(commands):
    command_pool = []

    with open(commands, 'r') as fp:
        for line in fp:
            command_pool.append(line.strip('\n'))
    return command_pool


def main():
    argv = parse_argv()

    mode = 0x00
    mode |= publisher.PUB_FLG_IGNORE_FAIL

    mlp= multi_process(argv['concurrency'])

    pub = publisher(get_host_pool(argv['hosts']),
                          get_command_pool(argv['commands']),
                          argv['concurrency'],
                          mode=mode,
                          group=argv['group'])
    db_name = '%s.db' % __file__.split('.')[0]
    hdr = db_handler(db_name, is_replace=True)
    pub.set_db_handler(hdr)

    sub = subscriber()

    mlp.register_publisher(pub)
    mlp.register_subscriber(sub,
                            argv['user'],
                            argv['keyfile'],
                            argv['password'])

    mlp.start()


if __name__ == '__main__':
    main()
