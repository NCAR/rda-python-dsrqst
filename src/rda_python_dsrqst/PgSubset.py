#
###############################################################################
#
#     Title : PgSubset.py
#
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 09/19/2020
#             2025-02-10 transferred to package rda_python_dsrqst from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for holding some global variables and
#             functions for dataset subsestting
#
#    Github : https://github.com/NCAR/rda-python-dsrqst.git
# 
###############################################################################
#
import re
from os import path as op
from rda_python_common import PgLOG
from rda_python_common import PgFile
from rda_python_common import PgDBI

#
# validate the subset request
# return dsrqst record upon success
#
def valid_subset_request(ridx, rdir, dsid, logact = PgLOG.LOGERR):

   logact |= PgLOG.ERRLOG
   if not ridx: return PgLOG.pglog("Miss Request Index for subset", logact)
   pgrqst = PgDBI.pgget("dsrqst", "*", "rindex = {}".format(ridx), logact)
   if not pgrqst:
      return PgLOG.pglog("{}: Request Index not on file".format(ridx), logact)   
   if pgrqst['rqsttype'] != "S" and pgrqst['rqsttype'] != "T":
      return PgLOG.pglog("{}: NOT a subset Request".format(ridx), logact)
   if not pgrqst['note']:
      return PgLOG.pglog("{}: Miss subset request info".format(ridx), logact)
   if dsid and pgrqst['dsid'] != dsid:
      return PgLOG.pglog("{}: Request of '{}' not for '{}'".format(ridx, pgrqst['dsid'], dsid), logact) 

   if rdir:
      ms = re.match(r'^{}/(.+)$'.format(PgLOG.PGLOG['RQSTHOME']), rdir)
      if ms:
         rid = ms.group(1)
         if rid != pgrqst['rqstid']:
            return PgLOG.pglog("{}: Directory NOT match Request Id of Index {}".format(rid, ridx), logact)
      else:
         return PgLOG.pglog("{}: Invalid directory for Request Index {}".format(rdir, ridx), logact)
   elif not pgrqst['rqstid']:
      return PgLOG.pglog("{}: Miss Request Id for request directory".format(ridx), logact)

   return pgrqst

#
# add a request file to given 
#
def add_subset_file(ridx, tofile, fromfile, type, dfmt, oidx, note, logact = PgLOG.LOGWRN):

   wfile = {}
   if fromfile: PgFile.local_copy_local(tofile, fromfile, logact)
   if type: wfile['type'] = type
   if dfmt: wfile['data_format'] = dfmt
   wfile['disp_order'] = oidx
   wfile['srctype'] = "W"
   if note: wfile['note'] = note
   cnd = "rindex = {} AND wfile = '{}'".format(ridx, tofile)
   if PgDBI.pgget("wfrqst", "", cnd):
      return PgDBI.pgupdt("wfrqst", wfile, cnd, logact)
   else:
      wfile['rindex'] = ridx
      wfile['wfile'] = tofile
      return PgDBI.pgadd("wfrqst", wfile, logact)

#
# set file count
#
def set_dsrqst_fcount(ridx, fcount, isize, rsize = None, logact = PgLOG.LOGWRN):

   record = {}
   record['fcount'] = fcount
   if isize: record['size_input'] = isize
   if rsize: record['size_request'] = rsize
   return PgDBI.pgupdt("dsrqst", record, "rindex = {}".format(ridx), logact)   

#
# set processed file count
#
def reset_dsrqst_pcount(ridx, pcount, logact = PgLOG.LOGWRN):

   record = {'pcount' : pcount}
   return PgDBI.pgupdt("dsrqst", record, "rindex = {}".format(ridx), logact)

#
# add child request
#
def add_request_child(pgrqst, dsid, fcount, isize = None, rsize = None):

   if dsid == pgrqst['dsid']:
      PgLOG.pglog("{}: Cannot add child request for the same dataset {}".format(pgrqst['rindex'], dsid), PgLOG.LOGERR)
      return pgrqst['rindex']

   record = {}
   record['rqstid'] = pgrqst['rqstid']
   record['rqsttype'] = pgrqst['rqsttype']
   record['dsid'] = dsid
   record['gindex'] = pgrqst['gindex']
   record['date_rqst'] = pgrqst['date_rqst']
   record['time_rqst'] = pgrqst['time_rqst']
   record['specialist'] = pgrqst['specialist']
   record['email'] = pgrqst['email']
   record['fcount'] = fcount if fcount else 0
   if isize != None: record['size_input'] = isize
   if rsize != None: record['size_request'] = rsize
   pgrec = PgDBI.pgget("dsrqst", "rindex", "pindex = {} AND dsid = '{}'".format(pgrqst['rindex'], dsid), PgLOG.LGEREX)
   if pgrec:
      ridx = pgrec['rindex']
      PgDBI.pgupdt("dsrqst", record, "rindex = {}".format(ridx), PgLOG.LGEREX)
   else:
      record['pindex'] = pgrqst['rindex']
      ridx = PgDBI.pgadd("dsrqst", record, PgLOG.LGEREX|PgLOG.AUTOID|PgLOG.DODFLT)
      PgLOG.pglog("{}: Child request added for Request {} of {}".format(ridx, pgrqst['rindex'], dsid), PgLOG.LOGWRN)

   return ridx

#
# increase 1 for pcount
#
def increment_dsrqst_pcount(ridx, logact = PgLOG.LOGWRN):

   return PgDBI.pgexec("UPDATE dsrqst SET pcount = pcount + 1 WHERE rindex = {}".format(ridx), logact)

#
# remove previously procssed subset files both in RDADB and on disk
#
def clean_subset_request(ridx, rdir, pattern, logact = PgLOG.LOGWRN):

   if ridx:
      rcnd = "rindex = {}".format(ridx)
      fcnt = PgDBI.pgget("wfrqst", "", rcnd, logact)
      if fcnt > 0:
         fcnt = PgDBI.pgdel("wfrqst", rcnd, logact)
         if fcnt > 0:
            s = 's' if fcnt > 1 else ''
            PgLOG.pglog("{} file record{} for Request Index {} removed from RDADB".format(fcnt, s, ridx), logact&(~PgLOG.EXITLG))

   if rdir and op.exists(rdir):
      fcnt = 0
      s = rdir + "/*"
      if pattern: s += (pattern + "*")
      sfiles = PgFile.local_glob(s)
      for file in sfiles:
         if PgFile.delete_local_file(file, logact, 4): fcnt += 1

      if fcnt > 0:
         s = 's' if fcnt > 1 else ''
         PgLOG.pglog("{} file{} cleaned from request directory {}".format(fcnt, s, rdir), logact&(~PgLOG.EXITLG))

#
# check if a subset request is built already
#
def request_built(ridx, rdir, cfile, fcnt, logact = PgLOG.LOGWRN):

   cnd = "rindex = {}".format(ridx)
   if fcnt and fcnt != PgDBI.pgget("wfrqst", "", cnd, logact): return 0
   if cfile:
      if not PgDBI.pgget("wfrqst", "", "{} and wfile = '{}'".format(cnd, cfile), logact): return 0
      if not op.exists("{}/{}".format(rdir, cfile)): return 0

   return 1

#
# add_request_file(ridx, file, pgrec. logact)
#   ridx - Request Index (mandatory)
#   file - Request file name (mandatory)
#   pgrec - optional hash reference for additional request file information.
#            pass null if no addtional file information.
#     All available keys are:
#     `pindex` int(11) DEFAULT '0' - 'if > 0, under a request partition',
#     `gindex` int(11) DEFAULT '0' - 'if > 0, under a subgroup in a dataset',
#     `srcid` int(11) DEFAULT '0' - 'if > 0, source file id, mssid/wid',
#     `srctype` char(1) DEFAULT 'W' - 'source data type, M-MSS, W-Web',
#     `size` bigint(20) DEFAULT '0' - 'bytes of data',
#     `date` date DEFAULT NULL - 'date file last loaded',
#     `time` time DEFAULT NULL - 'time file last loaded',
#     `type` char(1) DEFAULT 'D' - 'Data (default), dOcument or Software',
#     `status` char(1) DEFAULT 'R' - 'Requested, Online, Error loading',
#     `disp_order` int(11) DEFAULT NULL - 'display order of the files in a request',
#     `data_format` varchar(10) DEFAULT NULL - 'data format NetCDF, GRIB',
#     `file_format` varchar(10) DEFAULT NULL - 'archive format tar, compress',
#     `ofile` varchar(128) DEFAULT NULL - 'original file name for delayed/conversion modes',
#     `command` varchar(255) DEFAULT NULL - 'executable for processing subset of this requested file',
#     `cmd_detail` mediumtest - detail command info to build this file
#     `note` text - description of this file,
#      
def add_request_file(ridx, file, pgrec, logact = PgLOG.LOGWRN):

   record = {'srctype' : 'W', 'status' : 'R'}
   cnd = "rindex = {} AND wfile = '{}'".format(ridx, file)
   pgfile = PgDBI.pgget("wfrqst", "findex", cnd, logact)
   
   if pgrec:
      for key in pgrec:
         record[key] = pgrec[key]

   if pgfile:
      return PgDBI.pgupdt("wfrqst", record, "findex = {}".format(pgfile['findex']), logact)
   else:
      record['rindex'] = ridx
      record['wfile'] = file
      return PgDBI.pgadd("wfrqst", record, logact)

#
#  get longitude pair for given string
#
def get_longitudes(lstr, resol):
   
   ms = re.match(r'^(\S+)\s*(\w),\s*(\S+)\s*(\w)', lstr)
   if ms:
      w = float(ms.group(1))
      if ms.group(2) == 'W': w = -w
      e = float(ms.group(3))
      if ms.group(4) == 'W': e = -e
   else:
      PgLOG.pglog(lstr + ": Invalid Longitudes", PgLOG.LGEREX)

   if w > e: e += 360
   d = e - w
   if d >= 360.0:
      return (0.0, 360.0)
   elif d < resol:
      t = (w + resol - e)/2.0
      w -= t
      e += t

   if w < 0:
      w += 360.0
   elif w > 360.0:
      w -= 360.0

   if e < 0:
      e += 360.0
   elif e > 360.0:
      e -= 360.0

   return [w, e]

#
# get latitude pair for given string
#
def get_latitudes(lstr, resol):

   ms = re.match(r'^(\S+)\s*(\w),\s+(\S+)\s*(\w)', lstr)
   if ms:
      s = float(ms.group(1))
      if ms.group(2) == 'S': s = -s
      n = float(ms.group(3))
      if ms.group(4) == 'S': n = -n
   else:
      PgLOG.pglog(lstr + ": Invalid Latitudes", PgLOG.LGEREX)

   if s > n:    # swap north & south limits
      t = s
      s = n
      n = t

   if (n - s) < resol:
      t = (s + resol - n) / 2.0
      if n < (90 - t):
         n += t
      else:
         n = 90.0

      if s > (t - 90):
         s -= t
      else:
         s = -90.0

   return [s, n]   
