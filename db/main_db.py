import os
import sys

sys.path.append(os.path.abspath('../'))
from log_x import LogX
from db_handler import table_base


class tb_results(table_base):
    '''
    status: STATUS_OKAY: 0x00
            STATUS_FAIL: 0x01
    '''
    __tablename__ = 'results'

    def create_table(self):
        self.add_tb_entry('id', self.INT, cln_mode=self.CLN_FLG_PRIMARY)
        self.add_tb_entry('host', self.INT)
        self.add_tb_entry('cmd', self.INT)
        self.add_tb_entry('status', self.INT)
        self.add_tb_entry('result', self.TEXT)


class tb_hosts(table_base):
    '''
    status:     0x01    --> this bit represent the ssh connection status, 1 is
                            connected, 0 is disconnected
    '''
    __tablename__ = 'hosts'

    def create_table(self):
        self.add_tb_entry('id', self.INT, cln_mode=self.CLN_FLG_PRIMARY)
        self.add_tb_entry('hostname', self.TEXT, cln_mode=self.CLN_FLG_UNIQUE)
        self.add_tb_entry('status', self.INT)


class tb_commands(table_base):
    __tablename__ = 'commands'

    def create_table(self):
        self.add_tb_entry('id', self.INT, cln_mode=self.CLN_FLG_PRIMARY)
        self.add_tb_entry('command', self.TEXT)


class tb_stastics(table_base):
    __tablename__ = 'stastics'

    def create_table(self):
        self.add_tb_entry('id', self.INT, cln_mode=self.CLN_FLG_PRIMARY)
        self.add_tb_entry('nhosts', self.INT)
        self.add_tb_entry('ncommands', self.INT)
        self.add_tb_entry('nresults', self.INT)
