__author__ = 'rbeelive'
# -*- coding: UTF-8 -*-
lksjflskjdf
sdfsdf
sdfsdf
sdfsdf
sd
fs
df
sdf
s
df
sdf
sd
fsdfsfdsf
sdfsdfsdfsdfsdfsdf
sdfsdfsdf
sdfsdfsd
f


import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import os
import os.path

import collections
import json
import IP
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
import pyes
import pygeoip


#
# es_host = '172.16.88.160'
es_host = '192.168.88.54'
# es_host = '192.168.46.1'
# es_host = '127.0.0.1'

es_port = 9200
es_index = 'c1'  # suggested: kippo
es_type = 'type'  # suggested: auth

gi = pygeoip.GeoIP('F:\\python\\elasticsearch\\data\\GeoLiteCity.dat')

# This is the ES mapping, we mostly need it to mark specific fields as "not_analyzed"
mapping = {
    "date": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "time": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "s-sitename": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "cs-ip": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "cs-method": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "cs-uri-stem": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "cs-uri-query": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "s-port": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "cs-username": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "cs(User-Agent)": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "sc-status": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "sc-substatus": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "sc-win32-status": {
        "type": u"string",
        "index": "not_analyzed"
    },
    "city": {
        "type": u"string",
    },
    "country": {
        "type": u"string",
    },

    "c-ip": {
        "type": u"string",
        "index": "not_analyzed"
    }
}

"""
"city": {
        "type": "string",
        "index": "not_analyzed"
        },
"""

# Setup ES connection, flush index and put mapping
es = pyes.ES('{0}:{1}'.format(es_host, es_port))
# es.indices.delete_index_if_exists(es_index)
es.indices.create_index_if_missing(es_index)
es.indices.put_mapping(es_type, {'properties': mapping}, [es_index])

fs = {}
login = "D:\\log\\"

for root, dirs, files in os.walk(login):
    for name in files:
        file = os.path.join(root, name)


        with open(str(file), 'r'.encode('utf-8'), ) as f:
            print file

            for line in f:
                if '#' in line[0]:
                    continue
                #elif 'Microsoft'  in line[1]:
                #print "sdfffffffffffff"
                #continue
                else:
                    try:
                        row_dict = collections.OrderedDict()
                        ss = unicode(line, errors='ignore')
                        row = ss.split(' ')
                    #print len(row)


                    #for i in range(len(row)):
                    #print len(row)


                    #print line
                    #print row_dict[2]
                    #ip = IP.find(row[9])
                    #print ip
                    #print lj
                        """
                        print "--"*20
                        print row[0]
                        print row[1]
                        print row[2]
                        print row[3]
                        print row[4]
                        print row[5]
                        print row[6]
                        print row[7]
                        print row[8]
                        print row[9]
                        print row[10]
                        print row[11]
                        print row[12]
                        print row[13]
                        """
                    #print  gi.country_code_by_addr(row[8])
                        row_dict['date'] = row[0]

                        row_dict['time'] = row[1]

                    #row_dict['s-sitename'] = row[2]
                        row_dict['sc-ip'] = row[2]

                        row_dict['cs-method'] = row[3]

                        row_dict['cs-uri-stem'] = row[4]

                        row_dict['cs-uri-query'] = row[4].decode("utf-8") + "?" + row[5].decode("utf-8")

                        row_dict['s-port'] = row[7]

                    #row_dict['cs-username'] = row[8]
                        row_dict['cs(User-Agent)'] = row[9]

                        row_dict['cs(Referer)'] = "NULL"

                        row_dict['sc-status'] = row[10]

                        if row[8] in ['::1', '-', '127.0.0.1']:
                        #print "lalalalalal"

                            row_dict['c-ip'] = "127.0.0.1"
                            row_dict['city'] = "Local"
                            row_dict['country'] = "Local"
                    except Exception, a:
                        print a
                        pass

                    else:
                            try:
                                row_dict['c-ip'] = row[8]
                                dd = gi.record_by_addr(row_dict['c-ip'])
                                row_dict['city'] = dd['city']
                                row_dict['country'] = dd['country_name']
                            except:
                                row_dict['city'] = "Local"
                                row_dict['country'] = "Local"

                            try:

                #row_dict['sc-substatus'] = row[12]
                #row_dict['sc-win32-status'] = row[13]
                                auth_json = json.dumps(row_dict)

                #print auth_json
                #auth_json = json.dumps(row_dic,tencoding=('utf-8'),ensure_ascii=False)

                                es.index(auth_json, es_index, es_type)

                            except Exception, a:
                                print a
                                pass

