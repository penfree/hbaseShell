#!/usr/bin/env python
# coding=utf-8
'''
Author: penfree
Date: 2020-04-23 13:14:31

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from argparse import ArgumentParser
from HbaseShell import HbaseShell
import logging
logging.basicConfig(level=logging.DEBUG)

def getArgs():
    parser = ArgumentParser()
    sub_parsers = parser.add_subparsers(dest='action')
    enable_compress_parser = sub_parsers.add_parser('compress', help='Enable compress of hbase table')
    enable_compress_parser.add_argument('--table', '-t', dest='tables', nargs='*', help='tables to compress, compress all if tables is not specified')
    enable_compress_parser.add_argument('--compress-method', default='SNAPPY', dest='compress_method')

    return parser.parse_args()


def compressTables(tables, compress_method='SNAPPY'):
    """compress hbase table
    
    Args:
        tables (list): tables to compress
        compress_method (str): the compress method, default is SNAPPY
    """
    # get table list
    if not tables:
        tables = HbaseShell.list()
    failed, skipped = 0, 0
    for t in tables:
        logging.info('Begin check %s', t)
        table_info = HbaseShell.describe(t)
        alter_columns = []
        for col in table_info.columns.itervalues():
            if col.COMPRESSION == compress_method:
                continue
            alter_columns.append(
                {'name': col.NAME, 'compression': compress_method}
            )
        if not alter_columns:
            logging.info('[%s] need not alter', t)
            skipped += 1
        else:
            logging.info('Begin compress [%s]', t)
            try:
                if table_info.enabled:
                    HbaseShell.disable(t)
                HbaseShell.alter(t, alter_columns)
                HbaseShell.enable(t)
                HbaseShell.exectueCmd("major_compact '%s'" % t)
            except Exception, e:
                logging.exception(e)
                logging.error('compress [%s] failed', t)
                failed += 1
    logging.info('%d of %d failed, %d skipped', failed, len(tables), skipped)


def main():
    args = getArgs()
    if args.action == 'compress':
        compressTables(args.tables, args.compress_method)
    

main()
