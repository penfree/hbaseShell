#!/usr/bin/env python
# coding=utf-8
'''
Author: qiupengfei@rxthinking.com
Date: 2020-04-23 12:55:14

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from collections import defaultdict
import re


class HBaseRegionInfo(object):

    def __init__(self, title_str, status_str):
        '''
            table_name:
            start_key:
            timestamp:   
            region_name: encoded region name
            properties:
                numberOfStores
                numberOfStorefiles
                storefileUncompressedSizeMB
                lastMajorCompactionTimestamp
                storefileSizeMB
                compressionRatio
                memstoreSizeMB
                storefileIndexSizeMB
                readRequestsCount
                writeRequestsCount
                rootIndexSizeKB
                totalStaticIndexSizeKB
                totalStaticBloomSizeKB
                totalCompactingKVs
                currentCompactedKVs
                compactionProgressPct
                completeSequenceId
                dataLocality
        '''
        properties = status_str.strip().split(',')
        for item in properties:
            words = item.strip().split('=')
            pname = words[0].strip()
            pvalue = float(words[1])
            setattr(self, pname, pvalue)
        self.region_id = title_str.strip()[1:-1]
        words = self.region_id.strip().split(',')
        self.table_name = words[0].strip()
        self.start_key = ','.join(words[1:-1]).strip()
        self.timestamp = words[-1].split('.')[0].strip()
        self.region_name = words[-1].split('.')[1].strip()

    def dump(self):
        return self.__dict__


class HBaseRegionServerInfo(object):

    def __init__(self, title_str, status_str):
        '''
        '''
        words = title_str.strip().split(' ')
        self.name = words[0].split(':')[0]
        self.port = words[0].split(':')[1]
        self.timestamp = words[-1]

    def dump(self):
        return {
            'name': self.name,
            'port': self.port,
        }


class HBaseDetailedStatus(object):

    RegionServerTitlePat = re.compile(u'^\s+(.+):(\d+)\s(\d+)$')
    RegionTitlePat = re.compile(u'^\s+".*?,.*?,\d{13}\.[a-z0-9]+\."$')

    def __init__(self, status_str):
        self.status_str = status_str
        self.tables = defaultdict(list)
        self.regionservers = {}

    @classmethod
    def checkType(cls, line):
        if cls.RegionServerTitlePat.match(line):
            return 'regionserver'
        elif cls.RegionTitlePat.match(line):
            return 'region'
        else:
            return None

    @classmethod
    def parseDetailedStatus(cls, status_str):
        '''
            @Brief: Parse result of "status 'detailed'" in hbase shell
        '''
        lines = status_str if isinstance(status_str, list) else status_str.split('\n')
        status = HBaseDetailedStatus('\n'.join(lines))
        index = 0
        while index < len(lines):
            current_type = cls.checkType(lines[index])
            if current_type == 'regionserver':
                server = HBaseRegionServerInfo(lines[index], lines[index + 1])
                index += 2
                status.regionservers[server.name] = server
            elif current_type == 'region':
                region = HBaseRegionInfo(lines[index], lines[index + 1])
                index += 2
                status.tables[region.table_name].append(region)
            else:
                index += 1
        return status

    def getRegions(self, table):
        return self.tables[table]

    def dump(self):
        result = {
            'tables': {table: [v.dump() for v in values] for table, values in self.tables.iteritems()},
            'regionservers': [s.dump() for s in self.regionservers.itervalues()]
        }
        return result
    
    def __str__(self):
        return self.status_str
