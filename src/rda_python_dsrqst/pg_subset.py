###############################################################################
#     Title : PgSubset.py
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 09/19/2020
#             2025-02-10 transferred to package rda_python_dsrqst from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for holding some global variables and
#             functions for dataset subsestting
#    Github : https://github.com/NCAR/rda-python-dsrqst.git
# 
###############################################################################
import re
from os import path as op
from rda_python_common.pg_file import PgFile

class PgSubset(PgFile):

   def __init__(self):
      super().__init__()  # initialize parent class

   # validate the subset request
   # return dsrqst record upon success
   def valid_subset_request(self, ridx, rdir, dsid, logact = None):
      if logact is None: logact = self.LOGERR
      logact |= self.ERRLOG
      if not ridx: return self.pglog("Miss Request Index for subset", logact)
      pgrqst = self.pgget("dsrqst", "*", "rindex = {}".format(ridx), logact)
      if not pgrqst:
         return self.pglog("{}: Request Index not on file".format(ridx), logact)   
      if pgrqst['rqsttype'] != "S" and pgrqst['rqsttype'] != "T":
         return self.pglog("{}: NOT a subset Request".format(ridx), logact)
      if not pgrqst['note']:
         return self.pglog("{}: Miss subset request info".format(ridx), logact)
      if dsid and pgrqst['dsid'] != dsid:
         return self.pglog("{}: Request of '{}' not for '{}'".format(ridx, pgrqst['dsid'], dsid), logact) 
      if rdir:
         ms = re.match(r'^{}/(.+)$'.format(self.PGLOG['RQSTHOME']), rdir)
         if ms:
            rid = ms.group(1)
            if rid != pgrqst['rqstid']:
               return self.pglog("{}: Directory NOT match Request Id of Index {}".format(rid, ridx), logact)
         else:
            return self.pglog("{}: Invalid directory for Request Index {}".format(rdir, ridx), logact)
      elif not pgrqst['rqstid']:
         return self.pglog("{}: Miss Request Id for request directory".format(ridx), logact)
      return pgrqst

   # add a request file to given 
   def add_subset_file(self, ridx, tofile, fromfile, type, dfmt, oidx, note, logact = None):
      if logact is None: logact = self.LOGWRN
      wfile = {}
      if fromfile: self.local_copy_local(tofile, fromfile, logact)
      if type: wfile['type'] = type
      if dfmt: wfile['data_format'] = dfmt
      wfile['disp_order'] = oidx
      wfile['srctype'] = "W"
      if note: wfile['note'] = note
      cnd = "rindex = {} AND wfile = '{}'".format(ridx, tofile)
      if self.pgget("wfrqst", "", cnd):
         return self.pgupdt("wfrqst", wfile, cnd, logact)
      else:
         wfile['rindex'] = ridx
         wfile['wfile'] = tofile
         return self.pgadd("wfrqst", wfile, logact)

   # set file count
   def set_dsrqst_fcount(self, ridx, fcount, isize, rsize = None, logact = None):
      if logact is None: logact = self.LOGWRN
      record = {}
      record['fcount'] = fcount
      if isize: record['size_input'] = isize
      if rsize: record['size_request'] = rsize
      return self.pgupdt("dsrqst", record, "rindex = {}".format(ridx), logact)   

   # set processed file count
   def reset_dsrqst_pcount(self, ridx, pcount, logact = None):
      if logact is None: logact = self.LOGWRN
      record = {'pcount' : pcount}
      return self.pgupdt("dsrqst", record, "rindex = {}".format(ridx), logact)

   # add child request
   def add_request_child(self, pgrqst, dsid, fcount, isize = None, rsize = None):
      if dsid == pgrqst['dsid']:
         self.pglog("{}: Cannot add child request for the same dataset {}".format(pgrqst['rindex'], dsid), self.LOGERR)
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
      pgrec = self.pgget("dsrqst", "rindex", "pindex = {} AND dsid = '{}'".format(pgrqst['rindex'], dsid), self.LGEREX)
      if pgrec:
         ridx = pgrec['rindex']
         self.pgupdt("dsrqst", record, "rindex = {}".format(ridx), self.LGEREX)
      else:
         record['pindex'] = pgrqst['rindex']
         ridx = self.pgadd("dsrqst", record, self.LGEREX|self.AUTOID|self.DODFLT)
         self.pglog("{}: Child request added for Request {} of {}".format(ridx, pgrqst['rindex'], dsid), self.LOGWRN)
      return ridx

   # increase 1 for pcount
   def increment_dsrqst_pcount(self, ridx, logact = None):
      if logact is None: logact = self.LOGWRN
      return self.pgexec("UPDATE dsrqst SET pcount = pcount + 1 WHERE rindex = {}".format(ridx), logact)

   # remove previously procssed subset files both in RDADB and on disk
   def clean_subset_request(self, ridx, rdir, pattern, logact = None):
      if logact is None: logact = self.LOGWRN
      if ridx:
         rcnd = "rindex = {}".format(ridx)
         fcnt = self.pgget("wfrqst", "", rcnd, logact)
         if fcnt > 0:
            fcnt = self.pgdel("wfrqst", rcnd, logact)
            if fcnt > 0:
               s = 's' if fcnt > 1 else ''
               self.pglog("{} file record{} for Request Index {} removed from RDADB".format(fcnt, s, ridx), logact&(~self.EXITLG))
      if rdir and op.exists(rdir):
         fcnt = 0
         s = rdir + "/*"
         if pattern: s += (pattern + "*")
         sfiles = self.local_glob(s)
         for file in sfiles:
            if self.delete_local_file(file, logact, 4): fcnt += 1
         if fcnt > 0:
            s = 's' if fcnt > 1 else ''
            self.pglog("{} file{} cleaned from request directory {}".format(fcnt, s, rdir), logact&(~self.EXITLG))

   # check if a subset request is built already
   def request_built(self, ridx, rdir, cfile, fcnt, logact = None):
      if logact is None: logact = self.LOGWRN
      cnd = "rindex = {}".format(ridx)
      if fcnt and fcnt != self.pgget("wfrqst", "", cnd, logact): return 0
      if cfile:
         if not self.pgget("wfrqst", "", "{} and wfile = '{}'".format(cnd, cfile), logact): return 0
         if not op.exists("{}/{}".format(rdir, cfile)): return 0
      return 1

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
   def add_request_file(self, ridx, file, pgrec, logact = None):
      if logact is None: logact = self.LOGWRN
      record = {'srctype' : 'W', 'status' : 'R'}
      cnd = "rindex = {} AND wfile = '{}'".format(ridx, file)
      pgfile = self.pgget("wfrqst", "findex", cnd, logact)
      if pgrec:
         for key in pgrec:
            record[key] = pgrec[key]
      if pgfile:
         return self.pgupdt("wfrqst", record, "findex = {}".format(pgfile['findex']), logact)
      else:
         record['rindex'] = ridx
         record['wfile'] = file
         return self.pgadd("wfrqst", record, logact)

   #  get longitude pair for given string
   def get_longitudes(self, lstr, resol):
      ms = re.match(r'^(\S+)\s*(\w),\s*(\S+)\s*(\w)', lstr)
      if ms:
         w = float(ms.group(1))
         if ms.group(2) == 'W': w = -w
         e = float(ms.group(3))
         if ms.group(4) == 'W': e = -e
      else:
         self.pglog(lstr + ": Invalid Longitudes", self.LGEREX)
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

   # get latitude pair for given string
   def get_latitudes(self, lstr, resol):
      ms = re.match(r'^(\S+)\s*(\w),\s+(\S+)\s*(\w)', lstr)
      if ms:
         s = float(ms.group(1))
         if ms.group(2) == 'S': s = -s
         n = float(ms.group(3))
         if ms.group(4) == 'S': n = -n
      else:
         self.pglog(lstr + ": Invalid Latitudes", self.LGEREX)
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
