#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on Jul 14, 2013

@author: tomr
'''

import numbers
import subprocess

STATUS_OPTIONS = ['simple', 'summary', 'detailed']


class HbaseShell(object):

    '''
    This class provides python access to hbase shell functionality
    
    Implementation is based on:
    http://wiki.apache.org/hadoop/Hbase/Shell
    
    Does not support java class filters on scan commands, e.g.,
    hbase> scan 't1', {FILTER => org.apache.hadoop.hbase.filter.ColumnPaginationFilter.new(1, 0)}
    '''

    # Utils functions for inner usage

    @staticmethod
    def exectueCmd(cmd):
        p1 = subprocess.Popen(['echo', cmd], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(['hbase', 'shell'], stdin=p1.stdout,
                              stdout=subprocess.PIPE)
        p1.stdout.close()
        output = []
        line = p2.stdout.readline()
        while line:
            output.append(line.strip())
            line = p2.stdout.readline()
        p2.stdout.close()
        return output

    @staticmethod
    def argsDataToHbaseArgsString(dataArgs):
        if not dataArgs:
            return ''
        if type(dataArgs) == type({}):
            hbaseDictString = '{' + ', '.join([k.upper() + ' => '
                    + HbaseShell.argsDataToHbaseArgsString(v) for (k,
                    v) in dataArgs.iteritems() if v != None]) + '}'
            return (hbaseDictString if len(hbaseDictString) > 2 else '')
        if type(dataArgs) == str:
            if "'" in dataArgs:
                return '"' + dataArgs + '"'
            return "'" + dataArgs + "'"
        if isinstance(dataArgs, numbers.Integral):
            return str(dataArgs)
        if type(dataArgs) == bool:
            return str(dataArgs).lower()
        if type(dataArgs) == type([]):
            return '[' \
                + ', '.join([HbaseShell.argsDataToHbaseArgsString(v)
                            for v in dataArgs]) + ']'

    @staticmethod
    def ddlFuncParamtersToStr(
        cmdName,
        table,
        columns=None,
        args=None,
        ):

        cmd = cmdName + "'" + table + "'"
        columnsStr = \
            ', '.join([HbaseShell.argsDataToHbaseArgsString(cf)
                      for cf in columns])
        additioalDataStr = HbaseShell.argsDataToHbaseArgsString(args)
        if columnsStr != '' or additioalDataStr != '':
            cmd += ', ' + ', '.join([dataStr for dataStr in
                                    [columnsStr, additioalDataStr]
                                    if dataStr != ''])
        return HbaseShell.exectueCmd(cmd)

    # Hbase shell general commands

    @staticmethod
    def status(st=None):
        cmd = 'status'
        if st != None:
            if st not in STATUS_OPTIONS:
                raise ValueError('Invalid status option, legal values: '
                                  + str(STATUS_OPTIONS))
            else:
                cmd += " '" + st + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def version():
        cmd = 'version'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def whoami():
        cmd = 'whoami'
        return HbaseShell.exectueCmd(cmd)

    # Hbase shell ddl commands

    @staticmethod
    def alter(table, columns=None, args=None):
        '''
        parameters -
        table - table name
        columns - list of dictionaries which describes the column families:
        {name : column-family name (obligatory), version : number of versions to keep, int (optional), 
        ttl : time to live, long (optional), etc. all the possible \
        properties a column may get}
        args - all the general parameters alter can get
        '''

        return HbaseShell.ddlFuncParamtersToStr('create', table,
                columns, args)

    @staticmethod
    def alter_async(table, columns=None, args=None):
        '''
        parameters -
        table - table name
        columns - list of dictionaries which describes the column families:
        {name : column-family name (obligatory), version : number of versions to keep, int (optional), 
        ttl : time to live, long (optional), etc. all the possible \
        properties a column may get}
        args - all the general parameters alter can get
        '''

        return HbaseShell.ddlFuncParamtersToStr('alter_async', table,
                columns, args)

    @staticmethod
    def alter_status(table):
        cmd = "alter_status '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def create(table, columns=None, args=None):
        '''
        parameters -
        table - table name
        columns - list of dictionaries which describes the column families:
        {name : column-family name (obligatory), version : number of versions to keep, int (optional), 
        ttl : time to live, long (optional), blockcache : boolean (optional), etc. all the possible \
        properties a column may get}
        args - all the general parameters create can get
        '''

        return HbaseShell.ddlFuncParamtersToStr('create', table,
                columns, args)

    @staticmethod
    def describe(table):
        cmd = "describe '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def disable(table):
        cmd = "disable '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def disable_all():
        cmd = 'disable_all'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def drop(table):
        cmd = "drop '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def drop_all():
        cmd = 'drop_all'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def enable(table):
        cmd = "enable '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def enable_all():
        cmd = 'enable_all'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def exists(table):
        cmd = "exists '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def get_table(table):
        cmd = "get_table '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def is_disabled(table):
        cmd = "is_disabled '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def is_enabled(table):
        cmd = "is_enabled '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def list():
        cmd = 'list'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def show_filters(table):
        cmd = 'show_filters'
        return HbaseShell.exectueCmd(cmd)

    # Hbase shell dml commands

    @staticmethod
    def count(table, args=None):
        cmd = "count '" + table + "'"
        dataStr = HbaseShell.argsDataToHbaseArgsString(args)
        if dataStr != '':
            cmd += ', ' + dataStr
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def delete(
        table,
        row,
        column,
        value,
        timestamp=None,
        ):

        cmd = "delete '" + table + "', '" + row + "', '" + column \
            + "', '" + value + "'"
        if timestamp != None:
            if isinstance(timestamp, numbers.Integral):
                cmd += ", '" + str(timestamp) + "'"
            else:
                raise ValueError('timestamp must be integral')
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def deleteall(
        table,
        row,
        column=None,
        timestamp=None,
        ):

        cmd = "deleteall '" + table + "', '" + row
        if column != None:
            cmd += "', '" + column + "'"
        if timestamp != None:
            if isinstance(timestamp, numbers.Integral):
                cmd += ", '" + str(timestamp) + "'"
            else:
                raise ValueError('timestamp must be integral')
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def get(table, row, args=None):

        cmd = "get '" + table + "', '" + row + "' "
        dataStr = HbaseShell.argsDataToHbaseArgsString(args)
        if dataStr != '':
            cmd += ', ' + dataStr
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def get_counter(table, row, column):
        cmd = "get_counter '" + table + "', " + row + "', " + column \
            + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def incr(
        table,
        row,
        column,
        value=None,
        ):

        cmd = "incr '" + table + "', " + row + "', " + column + "'"
        if value != None:
            cmd += ', ' + str(value)
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def put(
        table,
        row,
        column,
        value,
        timestamp=None,
        ):

        cmd = "put '" + table + "', '" + row + "', '" + column + "', '" \
            + str(value)
        if timestamp != None:
            if isinstance(timestamp, numbers.Integral):
                cmd += "', '" + str(timestamp)
            else:
                raise ValueError('timestamp must be integral')

        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def scan(table, args=None):

        cmd = "scan '" + table + "'"
        dataStr = HbaseShell.argsDataToHbaseArgsString(args)
        if dataStr != '':
            cmd += ', ' + dataStr
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def truncate(table):
        cmd = "truncate '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    # Hbase shell tools commands

    @staticmethod
    def tools():
        cmd = 'tools'
        return HbaseShell.exectueCmd(cmd)

    # Hbase shell replication commands

    @staticmethod
    def add_peer(peer_id, cluster):
        cmd = "add_peer '" + peer_id + '\', "' + cluster + '"'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def disable_peer(peer_id):
        cmd = "disable_peer '" + peer_id + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def enable_peer(peer_id):
        cmd = "enable_peer '" + peer_id + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def list_peers(peer_id):
        cmd = 'list_peers'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def remove_peer(peer_id):
        cmd = "remove_peer '" + peer_id + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def start_replication(peer_id):
        cmd = 'start_replication'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def stop_replication(peer_id):
        cmd = 'stop_replication'
        return HbaseShell.exectueCmd(cmd)

    # Hbase shell snapshot commands

    @staticmethod
    def clone_snapshot(snapshot, table):
        cmd = "clone_snapshot '" + snapshot + "', '" + table + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def delete_snapshot(snapshot):
        cmd = "delete_snapshot '" + snapshot + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def list_snapshots():
        cmd = 'list_snapshots'
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def restore_snapshot(snapshot):
        cmd = "restore_snapshot '" + snapshot + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def snapshot(table, snapshot):
        cmd = "snapshot '" + table + "', '" + snapshot + "'"
        return HbaseShell.exectueCmd(cmd)

    # Hbase shell security commands

    @staticmethod
    def grant(
        user,
        permissions,
        table=None,
        columnFamily=None,
        columnQualifier=None,
        ):

        cmd = "grant '" + user + "', " + permissions + "'"
        if table != None:
            cmd += ", '" + table + "'"
            if columnFamily != None:
                cmd += ", '" + columnFamily + "'"
                if columnQualifier != None:
                    cmd += ", '" + columnQualifier + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def revoke(
        user,
        table=None,
        columnFamily=None,
        columnQualifier=None,
        ):

        cmd = "revoke '" + user + "'"
        if table != None:
            cmd += ", '" + table + "'"
            if columnFamily != None:
                cmd += ", '" + columnFamily + "'"
                if columnQualifier != None:
                    cmd += ", '" + columnQualifier + "'"
        return HbaseShell.exectueCmd(cmd)

    @staticmethod
    def user_permission(table):
        cmd = "user_permission '" + table + "'"
        return HbaseShell.exectueCmd(cmd)


