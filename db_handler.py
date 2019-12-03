from log_x import LogX
import inspect
import os
import sqlite3 as db
import sys


Log = LogX(__name__)


class db_exception(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info


class table_base(object):

    INT = 'integer'
    TEXT = 'text'
    REAL = 'real'

    CLN_FLG_NOT_NULL =  0x01
    CLN_FLG_UNIQUE   =  0x02
    CLN_FLG_PRIMARY  =  0x04

    def __init__(self):
        self.entry_lst = []
        self.create_table()

    def sql_str(self):
        sql_str = 'create table %s (%s)' % \
                  (self.__tablename__, ', '.join(self.entry_lst))
        Log.debug('  ..would create <table:%s> with <SQL:%s>' %
                  (self.__tablename__, sql_str))
        return sql_str

    def add_tb_entry(self, entry, type, cln_mode=0):
        # set default mode
        if cln_mode is 0:
            cln_mode = self.CLN_FLG_NOT_NULL

        flg_not_null = ''
        flg_unique = ''
        flg_primary = ''

        if cln_mode & self.CLN_FLG_NOT_NULL:
            flg_not_null = ' not null'
        if cln_mode & self.CLN_FLG_UNIQUE:
            flg_unique = ' unique'
        if cln_mode & self.CLN_FLG_PRIMARY:
            flg_primary = ' primary key'

        self.entry_lst.append('%s %s%s%s%s' % (entry, type, flg_primary,
                                               flg_unique, flg_not_null))

    def set_default(self, entry_name, dft_value):
        i = 0
        for entry in self.entry_lst:
            if entry.split(' ')[0] == entry_name:
                self.entry_lst[i] = '%s default %s' % (entry, dft_value)
                return
            i += 1
        msg = 'Not found <key:%s>' % entry_name
        Log.fatal(msg)
        raise db_exception(msg)

    def set_check(self, entry_name, chk_rule):
        i = 0
        for entry in self.entry_lst:
            if entry.split(' ')[0] == entry_name:
                self.entry_lst[i] = '%s check(%s)' % (entry, chk_rule)
                return
            i += 1
        msg = 'Not found <key:%s>' % entry_name
        Log.fatal(msg)
        raise db_exception(msg)


from db.main_db import tb_stastics,\
                       tb_hosts,\
                       tb_commands, \
                       tb_results
class db_handler(object):
    '''
    Usage:
        1:initialize database with all registered table classes that need to
            be written in '__tables__'. And there is one import thing is this
            class need to be imported below, or else Exception would be
            reported due to loop importing.
        2:this class provides all public interface to database. <put/get>
            @tb_hosts:      record hosts need to be accessed
            @tb_commands:   record commands need to be executed
            @tb_results:    record the results returned from the remote host
            @tb_stastics:   record the stastics of <host, commands, results>
    '''
    __tables__ = [
            tb_stastics,
            tb_hosts,
            tb_commands,
            tb_results,
            ## self-defined below
            ]

    def __init__(self, db_name, is_replace=False):
        if is_replace and os.access(db_name, os.F_OK):
            os.unlink(db_name)

        self.conn = db.connect(db_name)
        self.cursor = self.conn.cursor()

        self._create_tables()
        self._init_tb_stastics()

    def _init_tb_stastics(self):
        self.cursor.execute("insert into %s (id, nhosts, ncommands, nresults) "
                            "values (0, 0, 0, 0)" % (tb_stastics.__tablename__))

    def _create_tables(self):

        # check if table is already created
        c = self.cursor
        c.execute('select tbl_name from sqlite_master '
                            'where type=\'table\'')
        existed_tables = c.fetchall()

        for table in self.__tables__:
            table_name = table.__tablename__
            if (unicode(table_name), ) in existed_tables:
                Log.info('table %s is already existed' % table_name)
                continue

            c.execute(table().sql_str())
            Log.debug('result is %s' % str(c.fetchall()))

    def commit(self):
        self.conn.commit()

    ## =======================================================================
    #   used for public interface
    def put_host(self, host, status):
        c = self.cursor
        c.fetchall()

        c.execute('select nhosts from %s where id=0' %
                  (tb_stastics.__tablename__))
        cur_nhosts = c.fetchone()[0]
        c.execute('insert into %s (id, hostname, status) values (%d, "%s", %d)' %
                  (tb_hosts.__tablename__, cur_nhosts+1, host, status))
        c.execute('update %s set nhosts=%d where id=0' %
                           (tb_stastics.__tablename__, cur_nhosts+1))

        c.fetchall()

    def put_command(self, command):
        c = self.cursor
        c.fetchall()

        c.execute('select ncommands from %s where id=0' %
                  (tb_stastics.__tablename__))
        cur_ncmds = c.fetchone()[0]
        c.execute('insert into %s (id, command) values (%d, "%s")' %
                  (tb_commands.__tablename__, cur_ncmds+1, command))
        c.execute('update %s set ncommands=%d where id=0' %
                           (tb_stastics.__tablename__, cur_ncmds+1))

        c.fetchall()

    def put_result(self, host, cmd, status, result):
        c = self.cursor
        c.fetchall()

        c.execute('select nresults from %s where id=0' %
                  (tb_stastics.__tablename__))
        cur_nresults = c.fetchone()[0]

        # get host id
        c.execute('select id from %s where hostname="%s"' %
                  (tb_hosts.__tablename__, host))
        host_id = c.fetchone()[0]

        # get command id
        c.execute('select id from %s where command="%s"' %
                  (tb_commands.__tablename__, cmd))
        cmd_id = c.fetchone()[0]

        c.execute('insert into %s (id, host, cmd, status, result) values '
                  '(%d, %d, %d, %d, "%s")' %
                  (tb_results.__tablename__, cur_nresults+1, host_id, cmd_id,
                   status, result))
        c.execute('update %s set nresults=%d where id=0' %
                           (tb_stastics.__tablename__, cur_nresults+1))

        c.fetchall()

    def get_hosts(self):
        c = self.cursor
        c.execute('select * from %s' % (tb_hosts.__tablename__))
        return c.fetchall()

    def get_cmds(self):
        c = self.cursor
        c.execute('select * from %s' % (tb_commands.__tablename__))
        return c.fetchall()

    def get_results(self):
        c = self.cursor
        c.execute('select * from %s' %
                  (tb_results.__tablename__))
        return c.fetchall()

