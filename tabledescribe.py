#!/usr/bin/env python
# coding=utf-8
'''
Author: penfree
Date: 2020-04-23 11:44:43

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import re

class Column(object):
    
    def __init__(self, s):
        pat = re.compile(ur"({|, )([^ ]+?) => '(.+?)'")
        result = pat.findall(s)
        self.attrs = {}
        for item in result:
            self.attrs[item[1]] = item[2]
    
    def __getattr__(self, key):
        return self.attrs.get(key)


class TableDescribe(object):

    def __init__(self, s):
        """ Analysis of describe 'table'
        
        Args:
            s (str): the result of command "describe 'table'"
        """
        self.columns = {}
        lines = s if isinstance(s, list) else s.split('\n')
        self.table = None
        self.enabled = False
        for line in lines:
            if line.startswith('Table'):
                pat = re.compile(ur'Table (.*?) is (.*)')
                m = pat.search(line)
                if not m:
                    raise ValueError('cannot parse %s' % line)
                self.table = m.group(1)
                self.enabled = True if m.group(2) == 'ENABLED' else False
            elif line.startswith('{'):
                col = Column(line)
                self.columns[col.NAME] = col
        




if __name__ == '__main__':
    t = TableDescribe('''Table final_view_jswjw is DISABLED
final_view_jswjw
COLUMN FAMILIES DESCRIPTION
{NAME => 'emr', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE =>
'65536', REPLICATION_SCOPE => '0'}
{NAME => 'exam', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE =>
 '65536', REPLICATION_SCOPE => '0'}
{NAME => 'feature', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE
 => '65536', REPLICATION_SCOPE => '0'}
{NAME => 'info', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE =>
 '65536', REPLICATION_SCOPE => '0'}
{NAME => 'lab', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE =>
'65536', REPLICATION_SCOPE => '0'}
{NAME => 'order', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE', MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE =
> '65536', REPLICATION_SCOPE => '0'}
6 row(s) in 0.5310 seconds''')