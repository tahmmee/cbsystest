"""pushes stats from serieslydb to CBFS_HOST 

This module works by collecting stats from the seriesly database specified in testcfg.py
and furthermore uses the pandas (python data-anysis) module to store the stats into a dataframe
that is compatibale with version comparisions in the report generator.  Once in-memory as a
dataframe the stats are dumped to a csv file, compressed and pushed to CBFS.

usage: push_stats.py [-h] --spec <dir>/<test.js>
                          --version VERSION
                          --build BUILD
                          [--name NAME]

Where spec name is a required argument specifying the file used to generate stats.

"""

import sys
sys.path.append(".")
import argparse
import json
import gzip
import testcfg as cfg
import pandas as pd
import os
import shutil
from seriesly import Seriesly, exceptions
import requests

# cbfs
CBFS_HOST = 'http://cbfs.hq.couchbase.com:8484'

# archives array for keeping track of files to push (archive) into cbfs
archives = []

# setup parser
parser = argparse.ArgumentParser(description='CB System Test Stat Pusher')
parser.add_argument("--spec", help="path to json file used in test", metavar="<dir>/<test.js>", required = True)
parser.add_argument("--version",  help="couchbase version.. ie (2.2.0)", required = True)
parser.add_argument("--build",  help="build number", required = True)
parser.add_argument("--name", default=None, help="use to override name in test spec")

## connect to seriesly 
conn = Seriesly(cfg.SERIESLY_IP, 3133)



""" getDBData

  retrieve timeseries data from seriesly

"""
def getDBData(db):
  data = None

  try:
    db = conn[db]
    data = db.get_all()
    data = stripData(data)
  except exceptions.NotExistingDatabase:
    print "DB Not found: %s" % db
    print "cbmonitor running?"
    sys.exit(-1)

  return (data, None)[len(data) == 0]

""" stripData

  use data from the event db to collect only data from preceeding test

"""
def stripData(data):
  ev, _ = getSortedEventData()
  start_time = ev[0]
  copy = {}

  # remove data outside of start_time
  for d in data:
    if d >= start_time:
      copy[d] = data[d]

  del data
  return copy

def getSortedEventData():
  return sortDBData(conn['event'].get_all())


def get_query_params(start_time):
  query_params = { "group": 10000,
                   "reducer": "identity",
                   "from": start_time,
                   "ptr" : ""
                 }
  return query_params


"""
" sort data by its timestamp keys
"""
def sortDBData(data):

  sorted_data = []
  keys = []
  if(data):
    keys = sorted(data.iterkeys())

  for ts in keys:
    sorted_data.append(data[ts])

  return keys, sorted_data

def getSortedDBData(db):
  return sortDBData(getDBData(db))

"""
" make a timeseries dataframe
"""
def _createDataframe(index, data):

  df = None

  try:

    if(data):
      df = pd.DataFrame(data)
      df.index = index

  except ValueError as ex:
    print "unable to create dataframe: has incorrect format"
    raise Exception(ex)

  return df

"""
" get data from seriesly and convert to a 2d timeseries dataframe rows=ts, columns=stats
"""
def createDataframe(db):
  df = None
  data = getDBData(db)

  if data:
    index, data = getSortedDBData(db)
    df = _createDataframe(index, data)
  else:
    print "WARNING: stat db %s is empty!" % db

  return df


"""
" store stats per-phase to csv
"""
def storePhase(ns_dataframe, version, test, build, bucket):

  path = "system-test-results/%s/%s/%s/%s" % (version, test, build, bucket)
  print "Generating stats: %s" % path

  phase_dataframe = None
  columns = ns_dataframe.columns
  event_idx, _ = getSortedEventData()

  # plot each phase
  for i in xrange(len(event_idx)):
    if i == 0:
      phase_dataframe = ns_dataframe[ns_dataframe.index < event_idx[i+1]]
    elif i == len(event_idx) - 1:
      phase_dataframe = ns_dataframe[ns_dataframe.index > event_idx[i]]
    else:
      phase_dataframe = ns_dataframe[ (ns_dataframe.index < event_idx[i+1]) &\
        (ns_dataframe.index > event_idx[i])]

    phase_no = i
    dataframeToCsv(phase_dataframe, path, test, phase_no)

def dataframeToCsv(dataframe, path, test, phase_no):
    ph_csv  = "%s/%s_phase%s.csv" % (path, test, phase_no)
    ph_csv_gz  = "%s.gz" % ph_csv
    dataframe.to_csv(ph_csv)
    f = gzip.open(ph_csv_gz, 'wb')
    f.writelines(open(ph_csv, 'rb'))
    f.close()
    os.remove(ph_csv)
    archives.append(ph_csv_gz)


def generateStats(version, test, build, buckets):

  for bucket in buckets:
    ns_dataframe = createDataframe('ns_serverdefault%s' % bucket)

    if ns_dataframe:
      storePhase(ns_dataframe, version, test, build, bucket)


def pushStats():

  for data_file in archives:
    url = '%s/%s' % (CBFS_HOST, data_file)
    print "Uploading: " + url
    suffix = data_file.split('.')[-1]

    if(suffix == 'js'):
      headers = {'content-type': 'text/javascript'}
    else:
      headers = {'content-type': 'application/x-gzip'}
    data = open(data_file,'rb')
    r = requests.put(url, data=data, headers=headers)
    print r.text

def mkdir(path):
  if not os.path.exists(path):
      os.makedirs(path)
  else:
      shutil.rmtree(path)
      os.makedirs(path)

def prepareEnv(version, test, build, buckets):

  for bucket in buckets:
    path = "system-test-results/%s/%s/%s/%s" % (version, test, build, bucket)
    mkdir(path)



def loadSpec(spec):
  try:
    f = open(spec)
    specJS = json.loads(f.read())
    return specJS
  except Exception as ex:
    print "Invalid test spec: "+ str(ex)
    sys.exit(-1)

def setName(name, spec):

  if name is None:
    if 'name' in spec:
      name = str(spec['name'])
    else:
      print "test name missing from spec"
      sys.exit(-1)

  # remove spaces
  name = name.replace(' ','_')
  return name

def getBuckets():
  dbs = [db_name for db_name in conn.list_dbs() if db_name.find('ns_serverdefault')==0 ]

  # filter out dbs with host ip/name attached
  buckets = []
  for db in dbs:
    if(len([bucket for bucket in dbs if bucket.find(db) == 0]) != 1):
      db = db[len('ns_serverdefault'):]
      buckets.append(db)

  if(len(buckets) == 0):
    print "no bucket data in seriesly db"
    sys.exit(-1)

  return buckets

def createInfoFile(version, test, build, buckets, specPath):

  path = "system-test-results/%s/%s/%s" % (version, test, build)
  fname = '%s/_info.js' % path
  specName = specPath.split('/')[-1]

  info = {'buckets' : buckets,
          'spec' : specName,
          'files' : archives}

  f = open(fname, 'wb')
  f.write(json.dumps(info))

  # archive info for pushing to cbfs
  archives.append(fname)

  # archive spec for pushing to cbfs
  shutil.copy(specPath, path)
  archives.append("%s/%s" % (path, specName))

def main():


  args = parser.parse_args()
  specPath = args.spec
  spec = loadSpec(specPath)
  test = setName(args.name, spec)
  build = args.build
  version = args.version
  buckets = getBuckets()

  prepareEnv(version, test, build, buckets)
  generateStats(version, test, build, buckets)
  createInfoFile(version, test, build, buckets, specPath)
  pushStats()

if __name__ == "__main__":
    main()
