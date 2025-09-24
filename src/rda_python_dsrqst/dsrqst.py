#!/usr/bin/env python3
#
##################################################################################
#
#     Title: dsrqst
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 10/10/2020
#            2025-02-10 transferred to package rda_python_dsrqst from
#            https://github.com/NCAR/rda-utility-programs.git
#   Purpose: python utility program to stage data files online temporarily for
#            public users to download, including subset and data format conversion
#
#    Github: https://github.com/NCAR/rda-python-dsrqst.git
#
##################################################################################
#
import sys
import os
import re
import glob
import time
from os import path as op
from rda_python_common import PgLOG
from rda_python_common import PgUtil
from rda_python_common import PgCMD
from rda_python_common import PgFile
from rda_python_common import PgOPT
from rda_python_common import PgSIG
from rda_python_common import PgLock
from rda_python_common import PgDBI
from rda_python_common import PgSplit
from . import PgRqst

ALLCNT = 0   # global counting variables
ERRMSG = ''
TFSIZE = 1610612736   # 1.5GB, average tar file size
MFSIZE = 536870912   # 0.5GB, skip member file if size is larger
TCOUNT = 3   # no tar if file count is less
CMPLMT = 100   # minimal partition limit for compression
CMPCNT = 0   # compression partition count after command call
EMLMAX = 5   # limit file error numbers for email

#
# main function to run dsrqst
#
def main():

   PgOPT.parsing_input('dsrqst')
   PgRqst.check_enough_options(PgOPT.PGOPT['CACT'])
   start_action()

   if PgLOG.PGLOG['DSCHECK']:
      if ERRMSG:
         PgDBI.record_dscheck_error(ERRMSG)
      else:
         PgCMD.record_dscheck_status("D")

   if PgOPT.OPTS[PgOPT.PGOPT['CACT']][2]: PgLOG.cmdlog()   # log end time if not getting action

   PgLOG.pgexit(0)

#
# start action of dsrqst
#
def start_action():

   global ALLCNT

   if PgOPT.PGOPT['CACT'] == 'CR':
      ALLCNT = len(PgOPT.params['RI'])
      clean_request_info()
   elif PgOPT.PGOPT['CACT'] == 'DL':
      if 'CI' in PgOPT.params: # delete request controls
         if 'WF' in PgOPT.params:  # delete web files
            ALLCNT = len(PgOPT.params['WF'])
            delete_source_files()
         else:
            ALLCNT = len(PgOPT.params['CI'])
            delete_request_control()
      elif 'WF' in PgOPT.params:  # delete web files
         ALLCNT = len(PgOPT.params['WF'])
         delete_web_files()
      elif 'RI' in PgOPT.params: # delete requests
         ALLCNT = len(PgOPT.params['RI'])
         delete_request_info()
      if 'UD' in PgOPT.params: clean_unused_data()
      if 'UF' in PgOPT.params: reset_all_file_status()
      if 'UR' in PgOPT.params: clean_unused_requests()
   elif PgOPT.PGOPT['CACT'] == 'ER':
      if not ('RI' in PgOPT.params or 'DS' in PgOPT.params):
         PgOPT.set_default_value("SN", PgOPT.params['LN'])
      email_request_status()
   elif PgOPT.PGOPT['CACT'] == 'GC':
      if not ('DS' in PgOPT.params or 'CI' in PgOPT.params):
         PgOPT.set_default_value("SN", PgOPT.params['LN'])
      get_request_control()
   elif PgOPT.PGOPT['CACT'] == 'GF':
      get_web_files()
   elif PgOPT.PGOPT['CACT'] == 'GP':
      if not ('DS' in PgOPT.params or 'RI' in PgOPT.params or 'PI' in PgOPT.params):
         PgOPT.set_default_value("SN", PgOPT.params['LN'])
      get_request_partitions()
   elif PgOPT.PGOPT['CACT'] == 'GR':
      if not ('RI' in PgOPT.params or 'DS' in PgOPT.params):
         PgOPT.set_default_value("SN", PgOPT.params['LN'])
      get_request_info()
   elif PgOPT.PGOPT['CACT'] == 'GT':
      get_tar_files()
   elif PgOPT.PGOPT['CACT'] == 'RP':
      ALLCNT = len(PgOPT.params['RI'])
      reset_purge_time()
   elif PgOPT.PGOPT['CACT'] == 'RR':
      ALLCNT = len(PgOPT.params['RI'])
      restore_requests()
   elif PgOPT.PGOPT['CACT'] == 'SC':
      ALLCNT = len(PgOPT.params['CI'])
      set_request_control()
   elif PgOPT.PGOPT['CACT'] == 'SF':
      if 'WF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['WF'])
         set_web_files()
      else:
         PgRqst.reorder_request_files(PgOPT.params['ON'])
   elif PgOPT.PGOPT['CACT'] == 'SP':
      ALLCNT = len(PgOPT.params['PI']) if 'PI' in PgOPT.params else 0
      if ALLCNT > 0:
         set_request_partitions()
      else:
         ALLCNT = len(PgOPT.params['RI'])
         add_request_partitions()
   elif PgOPT.PGOPT['CACT'] == 'SR':
      ALLCNT = len(PgOPT.params['RI'])
      set_request_info()
   elif PgOPT.PGOPT['CACT'] == 'ST':
      if 'WF' in PgOPT.params:
         ALLCNT = len(PgOPT.params['WF'])
         set_tar_files()
      else:
         PgRqst.reorder_tar_files(PgOPT.params['ON'])
   elif PgOPT.PGOPT['CACT'] == 'UL':
      if 'PI' in PgOPT.params:
         ALLCNT = len(PgOPT.params['PI'])
         unlock_partition_info()
      else:
         ALLCNT = len(PgOPT.params['RI'])
         unlock_request_info()
   elif PgOPT.PGOPT['CACT'] == 'IR':
      ALLCNT = len(PgOPT.params['RI'])
      interrupt_requests()
   elif PgOPT.PGOPT['CACT'] == 'IP':
      ALLCNT = len(PgOPT.params['PI'])
      interrupt_partitions(PgOPT.params['PI'], ALLCNT)
   elif PgOPT.PGOPT['CACT'] == 'BR':
      ALLCNT = len(PgOPT.params['RI'])
      build_requests()
   elif PgOPT.PGOPT['CACT'] == 'PP':
      ALLCNT = len(PgOPT.params['PI'])
      process_partitions()
   elif PgOPT.PGOPT['CACT'] == 'PR':
      ALLCNT = len(PgOPT.params['RI'])
      purge_requests()

#
# clean requests for given request indices
#
def clean_request_info():

   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("clean {} request{} ...".format(ALLCNT, s), PgLOG.WARNLG)
   PgFile.check_local_writable(PgOPT.params['WH'], "Clean Request", PgOPT.PGOPT['extlog'])
   PgOPT.validate_multiple_options(ALLCNT, ['DS', 'RS'])

   for i in range(ALLCNT):
      ridx = PgOPT.params['RI'][i]
      rcnd = "rindex = {}".format(ridx)
      pgrec = PgDBI.pgget("dsrqst", "*", rcnd, PgOPT.PGOPT['extlog'])
      if not pgrec: continue
      record = {}
      if pgrec['ptcount'] > 1 or pgrec['pid'] and pgrec['lockhost'] == 'partition':
          if not clean_partition_info(ridx, rcnd, pgrec): continue
      if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['extlog']) <= 0: continue
      if pgrec['rqstid']:     # clean the request directory
         rdir = PgRqst.get_file_path(None, pgrec['rqstid'], None, 1)
         if op.isdir(rdir) and PgLOG.pgsystem("rm -rf " + rdir):
            PgLOG.pglog(rdir + ": Directory is removed", PgLOG.LOGWRN)

      if PgDBI.pgget("wfrqst", "", rcnd):
         if PgOPT.request_type(pgrec['rqsttype'], 1):
            cnt = PgDBI.pgexec("UPDATE wfrqst SET status = 'R', pindex = 0 WHERE " + rcnd, PgOPT.PGOPT['extlog'])
            s = 's' if cnt > 1 else ''
            PgLOG.pglog("{} file record{} set to status 'R' for {}".format(cnt, s, rcnd), PgLOG.LOGWRN)
         else:
            cnt = PgDBI.pgdel("wfrqst", rcnd, PgOPT.PGOPT['extlog'])
            s = 's' if cnt > 1 else ''
            PgLOG.pglog("{} file record{} removed for {}".format(cnt, s, rcnd), PgLOG.LOGWRN)

      if PgDBI.pgget("tfrqst", "", rcnd):
         cnt = PgDBI.pgdel("tfrqst", rcnd, PgOPT.PGOPT['extlog'])
         s = 's' if cnt > 1 else ''
         PgLOG.pglog("{} tar file record{} removed for {}".format(cnt, s, rcnd), PgLOG.LOGWRN)

      if pgrec['pcount']: record['pcount'] = 0
      if not PgOPT.request_type(pgrec['rqsttype'], 1):
          if pgrec['fcount']: record['fcount'] = 0
          if pgrec['size_request']: record['size_request'] = 0

      if (PgRqst.cache_request_control(ridx, pgrec, PgOPT.PGOPT['CACT'], 0) and 
         (PgOPT.PGOPT['RCNTL']['ptlimit'] or PgOPT.PGOPT['RCNTL']['ptsize'])):
         if pgrec['ptcount']: record['ptcount'] = 0
      else:
         if pgrec['ptcount'] != 1: record['ptcount'] = 1

      if pgrec['tarcount']: record['tarcount'] = 0
      record['ecount'] = record['exectime'] = record['pid'] = 0
      record['lockhost'] = ''
      if 'RS' in PgOPT.params and PgOPT.params['RS'][i]:
         record['status'] = PgOPT.params['RS'][i]
      if 'RN' in PgOPT.params and PgOPT.params['RN'][i]:
         record['rqstid'] = PgOPT.params['RN'][i]
      if PgDBI.pgupdt("dsrqst", record, rcnd, PgOPT.PGOPT['extlog']):
         clean_request_usage(ridx, rcnd)
         PgLOG.pglog("{} Request {} is cleaned".format(PgOPT.request_type(pgrec['rqsttype']), ridx), PgLOG.LOGWRN)

#
# clean request partitions for given request index
#
def clean_partition_info(ridx, cnd, pgrqst):
   
   pgrecs = PgDBI.pgmget("ptrqst", "pindex", cnd, PgOPT.PGOPT['extlog'])
   pcnt = len(pgrecs['pindex']) if pgrecs else 0
   if pcnt > 0:
      s = 's' if pcnt > 1 else ''
      PgLOG.pglog("clean {} request partition{} for {} ...".format(pcnt, s, cnd), PgLOG.WARNLG)
      for i in range(pcnt):
         pidx = pgrecs['pindex'][i]
         pcnd = "pindex = {}".format(pidx)
         if PgLock.lock_partition(pidx, 1, PgOPT.PGOPT['extlog']) <= 0:
            return PgLOG.pglog("RQST{}: Cannot clean partition, {} is locked".format(ridx, pcnd), PgOPT.PGOPT['errlog'])
         PgDBI.pgdel("ptrqst", pcnd, PgOPT.PGOPT['extlog'])
      if PgDBI.pgget('dsrqst', '', cnd + " AND lockhost = 'partition' AND pid > 0"):
         PgDBI.pgexec("UPDATE dsrqst SET pid = 0 WHERE " + cnd, PgOPT.PGOPT['extlog'])

   return 1

#
# delete one request for given request indix
#
def delete_one_request(ridx, dcnt, cleanusage = 0):

   cnd = "rindex = {}".format(ridx)
   pgrqst = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
   if not pgrqst: PgOPT.action_error("Error get Request Record for " + cnd)
   shared = PgOPT.request_type(pgrqst['rqsttype'], 1)
   
   if pgrqst['rqstid'] and not pgrqst['location']:  # clean the request directory
      dpath = PgRqst.get_file_path(None, pgrqst['rqstid'], None, 1)
      if dpath != PgOPT.params['WH'] and op.isdir(dpath):
         if shared:
            cnt = 0
         else:
            files = glob.glob(dpath + "/*")
            cnt = len(files)
            if cnt > 0:
               file = dpath + "/index.html"
               if file in files: cnt -= 1
         PgLOG.pgsystem("rm -rf " + dpath, PgOPT.PGOPT['extlog'], 5)
         if cnt > 0:
            s = 's' if cnt > 1 else ''
            PgLOG.pglog("Directory {} and {} file{} under it are removed".format(dpath, cnt, s), PgLOG.LOGWRN)
            dcnt[2] += cnt
         else:
            PgLOG.pglog("Directory {} is removed".format(dpath), PgLOG.LOGWRN)

   if shared:
      pgrecs = PgDBI.pgmget("wfrqst", "wfile, ofile", cnd, PgOPT.PGOPT['extlog'])
      cnt = len(pgrecs['wfile']) if pgrecs else 0
      if cnt > 0:
         s = 's' if cnt > 1 else ''
         PgLOG.pglog("Delete {} associated file{} for Request Index {} ...".format(cnt, s, ridx), PgLOG.WARNLG)
         dpath = "data/" + pgrqst['dsid']
         for j in range(cnt):
            delete_one_file(pgrqst, pgrecs['wfile'][j], pgrecs['ofile'][j], dpath, 1, dcnt)
   else:
      cnt = PgDBI.pgdel("wfrqst", cnd, PgOPT.PGOPT['extlog'])
      if cnt > 0:
         s = 's' if cnt > 1 else ''
         PgLOG.pglog("{} file record{} removed from RDADB".format(cnt, s), PgLOG.LOGWRN)
         dcnt[0] += cnt
         dcnt[1] += cnt

   if pgrqst['ptcount'] > 1 or PgDBI.pgget("ptrqst", "", cnd):
      cnt = PgDBI.pgdel("ptrqst", cnd, PgOPT.PGOPT['extlog'])
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} partition record{} removed from RDADB".format(cnt, s), PgLOG.LOGWRN)
   if PgDBI.pgget("tfrqst", "", cnd):
      cnt = PgDBI.pgdel("tfrqst", cnd, PgOPT.PGOPT['extlog'])
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} tar file record{} removed from RDADB".format(cnt, s), PgLOG.LOGWRN)

   if PgDBI.pgdel("dsrqst", cnd, PgOPT.PGOPT['extlog']):
      if cleanusage: clean_request_usage(ridx, cnd)
      return 1
   else:
      return 0

#
# delete requests for given request indices
#
def delete_request_info():

   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Delete {} request{} ...".format(ALLCNT, s), PgLOG.WARNLG)
 
   PgFile.check_local_writable(PgOPT.params['WH'], "Delete Request", PgOPT.PGOPT['extlog'])
   PgOPT.validate_multiple_options(ALLCNT, ["DS"])
   dcnt = [0]*3
   delcnt = 0
   for i in range(ALLCNT):
      ridx = PgLock.lock_request(PgOPT.params['RI'][i], 1, PgOPT.PGOPT['extlog'])
      if ridx <= 0: continue
      delcnt += delete_one_request(ridx, dcnt, 1)

   PgLOG.pglog("{} of {} request{} deleted".format(delcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])
   if dcnt[0] > 0:
      s = 's' if dcnt[0] > 1 else ''
      PgLOG.pglog("{}/{} of {} request file{} deleted from RDADB/Disk".format(dcnt[1], dcnt[2], dcnt[0], s), PgOPT.PGOPT['wrnlog'])

#
# delete request controls for given request control indices
#
def delete_request_control():

   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Delete {} request control{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   delcnt = 0
   for i in range(ALLCNT):
      cnd = "cindex = {}".format(PgOPT.params['CI'][i])
      delcnt += PgDBI.pgdel("rcrqst", cnd, PgOPT.PGOPT['extlog'])

   PgLOG.pglog("{} of {} request control{} deleted".format(delcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# delete online files for given request indices
#
def delete_web_files():

   s = 's' if ALLCNT > 1 else ''

   PgLOG.pglog("Delete {} request file{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   PgOPT.validate_multiple_options(ALLCNT, ["RI"])
   ridx = 0
   dcnt = [0]*3
   for i in range(ALLCNT):
      if ridx != PgOPT.params['RI'][i]:
         ridx = PgLock.lock_request(PgOPT.params['RI'][i], 1, PgOPT.PGOPT['extlog'])
         if ridx <= 0: continue
         cnd = "rindex = {}".format(ridx)
         pgrqst = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
         if not pgrqst: PgOPT.action_error("Error get Request Record for " + cnd)
         shared = PgOPT.request_type(pgrqst['rqsttype'], 1)
         dpath = "data/" + pgrqst['dsid'] if shared else pgrqst['rqstid']

      pgrec = PgDBI.pgget("wfrqst", "ofile", "{} AND wfile = '{}'".format(cnd, PgOPT.params['WF'][i]), PgOPT.PGOPT['extlog'])
      delete_one_file(pgrqst, PgOPT.params['WF'][i], pgrec['ofile'] if pgrec else None, dpath, shared, dcnt)
      if i > (ALLCNT - 2) or  ridx != PgOPT.params['RI'][i+1]:
         PgRqst.set_request_count(cnd, pgrec)
         PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])   # unlock requests

   PgLOG.pglog("{}/{} of {} request file{} deleted from RDADB/Disk".format(dcnt[1], dcnt[2], ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# delete including source files for given request control indices
#
def delete_source_files():

   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Delete {} including source file{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   PgOPT.validate_multiple_options(ALLCNT, ["CI"])
   cidx = dcnt = 0
   for i in range(ALLCNT):
      if cidx != PgOPT.params['CI'][i]:
         cnd = "cindex = {}".format(cidx)
         pgrec = PgDBI.pgget("rcrqst", "*", cnd, PgOPT.PGOPT['extlog'])
         if not pgrec:
            PgOPT.action_error("Error get Request Control Record for " + cnd)
      dcnt += PgDBI.pgdel("sfrqst", "{} AND wfile = '{}'".format(cnd, PgOPT.params['WF'][i]), PgOPT.PGOPT['extlog'])

   PgLOG.pglog("{} of {} source file{} deleted from RDADB".format(dcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# Remove file record in RDADB and delete file physically on disk if needed
#
def delete_one_file(pgrqst, wfile, ofile, dpath, shared, cnts):

   ridx = pgrqst['rindex']
   cnd = "rindex = {}".format(ridx)
   cnts[0] += 1
   cnts[1] += PgDBI.pgdel("wfrqst", "{} AND wfile = '{}'".format(cnd, wfile), PgOPT.PGOPT['extlog'])

   file = PgRqst.get_file_path(wfile, dpath, None, 1)
   info = PgFile.check_local_file(file, 1, PgOPT.PGOPT['wrnlog'])
   if info:
      retain = 0
      if shared:
         pgrecs = PgDBI.pgmget("wfrqst", "*", "wfile = '{}'".format(wfile), PgOPT.PGOPT['extlog'])
         cnt = len(pgrecs['rindex']) if pgrecs else 0
         for i in range(cnt):
            pgrec = PgUtil.onerecord(pgrecs, i)
            rcnd = "rindex = {}".format(pgrec['rindex'])
            if not PgDBI.pgget("dsrqst", "", "{} AND dsid = '{}'".format(cnd, pgrqst['dsid']), PgOPT.PGOPT['extlog']): continue
            if pgrec['status'] == "O":
               retain += 1
               continue
            else:
               record = {'status' : 'O', 'size' : pgrec['size'], 'date' : pgrec['date'], 'time' : pgrec['time']}
               retain += PgDBI.pgupdt("wfrqst", record, "{} AND wfile = '{}'".format(cnd, wfile), PgOPT.PGOPT['extlog'])
      if not retain and PgLOG.pgsystem("rm -f " + file):
         PgLOG.pglog(file + ": deleted", PgOPT.PGOPT['wrnlog'])
         cnts[2] += 1

   if ofile and ofile != wfile:
      file = PgRqst.get_file_path(ofile, dpath, None, 1)
      info = PgFile.check_local_file(file, 1, PgOPT.PGOPT['wrnlog'])
      if info:
         if not (shared and PgDBI.pgget("wfrqst, dsrqst", "", "wfrqst.rindex = dsrqst.rindex AND ofile = '{}' AND dsid = '{}'".format(ofile, pgrqst['dsid']))):
            if PgLOG.pgsystem("rm -f " + ofile):
               PgLOG.pglog(ofile + ": deleted", PgOPT.PGOPT['wrnlog'])
               cnts[2] += 1

#
# get request information
#
def get_request_info():

   tname = "dsrqst"
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get request information from RDADB ...", PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['dsall'])
   if 'CS' in PgOPT.params:
       if 'A' not in fnames: fnames += "A"
       if 'R' not in fnames: fnames = "R" + fnames
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "R"
   condition = PgOPT.get_hash_condition(tname, None, None, 1)
   if 'ON' in PgOPT.params and 'OB' in PgOPT.params:
      oflds = onames
   else:
      condition += PgOPT.get_order_string(onames, tname)

   pgrecs = PgDBI.pgmget(tname, "*", condition, PgOPT.PGOPT['extlog'])
   if PgOPT.PGOPT['CACT'] == "GB": PgOPT.OUTPUT.write("[DSRQST]\n")
   if pgrecs:
       if 'CS' in PgOPT.params: pgrecs['status'] = PgRqst.get_request_status(pgrecs)
       if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
       if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])

   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = "s" if cnt > 1 else ""
      PgLOG.pglog("{} request{} retrieved".format(cnt, s), PgOPT.PGOPT['wrnlog'])
   else:
      PgLOG.pglog("No request information retrieved", PgOPT.PGOPT['wrnlog'])

#
# get request control information
#
def get_request_control():

   tname = "rcrqst"
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get request control information from RDADB ...", PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['rcall'])
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "BIT"
   condition = PgOPT.get_hash_condition(tname, None, None, 1)
   if 'ON' in PgOPT.params and PgOPT.params['OB']:
      oflds = onames
   else:
      condition += PgOPT.get_order_string(onames, tname)

   pgrecs = PgDBI.pgmget(tname, "*", condition, PgOPT.PGOPT['extlog'])
   if pgrecs:
       if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs,fnames, hash)
       if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])
   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} request control{} retrieved".format(cnt, s), PgOPT.PGOPT['wrnlog'])
   else:
      PgLOG.pglog("No request control information retrieved", PgOPT.PGOPT['wrnlog'])

#
# get request partition information
#
def get_request_partitions():

   tname = "ptrqst"
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get request partition information from RDADB ...", PgLOG.WARNLG)

   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['ptall'])
   if 'CS' in PgOPT.params:
       if 'A' not in fnames: fnames += "A"
       if 'P' not in fnames: fnames = "P" + fnames
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "P"
   condition = PgOPT.get_hash_condition(tname, None, None, 1)
   if 'ON' in PgOPT.params and 'OB' in PgOPT.params:
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)

   pgrecs = PgDBI.pgmget(tname, "*", condition, PgOPT.PGOPT['extlog'])
   if pgrecs:
       if 'CS' in PgOPT.params: pgrecs['status'] = PgRqst.get_partition_status(pgrecs)
       if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
       if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])
   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{} request partition{} retrieved".format(cnt, s), PgOPT.PGOPT['wrnlog'])
   else:
      PgLOG.pglog("No request partition information retrieved", PgOPT.PGOPT['wrnlog'])

#
# get online file information
#
def get_web_files():

   tables = "wfrqst INNER JOIN dsrqst ON wfrqst.rindex = dsrqst.rindex"
   tname = 'wfrqst'
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get request file information from RDADB ...", PgLOG.WARNLG)

   dojoin = 0
   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['wfall'])
   if 'WD' in PgOPT.params:
      fnames += "B"
      dojoin = 1
   qnames = fnames
   if 'R' not in qnames: qnames += 'R'
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "RO"
   qnames += PgOPT.append_order_fields(onames, fnames, tname)
   if not dojoin and 'DS' in PgOPT.params: dojoin = 1
   condition = PgOPT.get_hash_condition(tname, None, None, 1)
   if 'ON' in PgOPT.params and ('OB' in PgOPT.params):
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)
   pgrecs = PgDBI.pgmget(tables if dojoin else tname, PgOPT.get_string_fields(qnames, tname), condition, PgOPT.PGOPT['extlog'])

   if PgOPT.PGOPT['CACT'] == "GB": PgOPT.OUTPUT.write("[{}]\n".format(tname.upper()))
   if pgrecs:
      if 'srcid' in pgrecs:
         dsids = PgRqst.get_request_dsids(pgrecs['rindex'])
         pgrecs['srcid'] = PgRqst.fid2fname(pgrecs['srcid'], dsids, pgrecs['srctype'])
      if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
      if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])

   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = "s" if cnt > 1 else ""
      PgLOG.pglog("{} request file record{} retrieved".format(cnt, s), PgOPT.PGOPT['wrnlog'])
   else:
      PgLOG.pglog("no request file record retrieved", PgOPT.PGOPT['wrnlog'])

#
# get online file information
#
def get_tar_files():

   tables = "tfrqst INNER JOIN dsrqst ON tfrqst.rindex = dsrqst.rindex"
   tname = "tfrqst"
   hash = PgOPT.TBLHASH[tname]
   PgLOG.pglog("Get tar file information from RDADB ...", PgLOG.WARNLG)

   dojoin = 0
   lens = oflds = fnames = None
   if 'FN' in PgOPT.params: fnames = PgOPT.params['FN']
   fnames = PgDBI.fieldname_string(fnames, PgOPT.PGOPT[tname], PgOPT.PGOPT['tfall'])
   if 'WD' in PgOPT.params:
      fnames += "B"
      dojoin = 1
   qnames = fnames
   onames = PgOPT.params['ON'] if 'ON' in PgOPT.params else "RO"
   qnames += PgOPT.append_order_fields(onames, fnames, tname)
   if not dojoin and 'DS' in PgOPT.params: dojoin = 1
   condition = PgOPT.get_hash_condition(tname, None, None, 1)
   if 'ON' in PgOPT.params and PgOPT.params['OB']:
      oflds = PgOPT.append_order_fields(onames, None, tname)
   else:
      condition += PgOPT.get_order_string(onames, tname)
   pgrecs = PgDBI.pgmget(tables if dojoin else tname, PgOPT.get_string_fields(qnames, tname), condition, PgOPT.PGOPT['extlog'])

   if pgrecs:
      if 'FO' in PgOPT.params: lens = PgUtil.all_column_widths(pgrecs, fnames, hash)
      if oflds: pgrecs = PgUtil.sorthash(pgrecs, oflds, hash, PgOPT.params['OB'])

   PgOPT.OUTPUT.write(PgOPT.get_string_titles(fnames, hash, lens) + "\n")
   if pgrecs:
      cnt = PgOPT.print_column_format(pgrecs, fnames, hash, lens)
      s = "s" if cnt > 1 else ""
      PgLOG.pglog("{} tar file record{} retrieved".format(cnt, s), PgOPT.PGOPT['wrnlog'])
   else:
      PgLOG.pglog("no tar file record retrieved", PgOPT.PGOPT['wrnlog'])

#
# add or modify request information
#
def set_request_info():

   tname = "dsrqst"
   hash = PgOPT.TBLHASH[tname]
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Set information of {} request{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   addcnt = modcnt = 0
   if 'SN' not in PgOPT.params:
      PgOPT.params['SN'] = [PgOPT.params['LN']]
      PgOPT.OPTS['SN'][2] |= 2

   if 'WN' in PgOPT.params:
      if 'FC' not in PgOPT.params: PgOPT.params['FC'] = [0]*ALLCNT
      for i in range(ALLCNT):
         PgOPT.params['FC'][i] = PgDBI.pgget("wfrqst", "", "rindex = {}".format(PgOPT.params['RI'][i]), PgOPT.PGOPT['extlog'])

   flds = PgOPT.get_field_keys(tname, None, "R")
   PgOPT.validate_multiple_values(tname, ALLCNT, flds)
   if 'GU' in PgOPT.params and not PgOPT.params['RS']: flds += 'A'

   for i in range(ALLCNT):
      ridx = PgOPT.params['RI'][i]
      if ridx > 0:
         if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['extlog']) <= 0: continue
         cnd = "rindex = {}".format(ridx)
         pgrec = PgDBI.pgget(tname, "*", cnd, PgOPT.PGOPT['extlog'])
         if pgrec:
            if 'MD' not in PgOPT.params and pgrec['specialist'] != PgOPT.params['LN'] and PgOPT.params['LN'] != PgLOG.PGLOG['GDEXUSER']:
               PgOPT.action_error("{}: Must be '{}' to set request index {}".format(PgOPT.params['LN'], pgrec['specialist'], cnd))
            if 'GU' in PgOPT.params:
               if "POH".find(pgrec['status']) > -1:
                  purge_one_request(ridx, PgUtil.curdate(), PgUtil.curtime(), 0)
               else:
                  PgLOG.pglog("Status '{}' of Request {} must be in ('O', 'P', 'H') to gather usage".format(pgrec['status'], ridx), PgOPT.PGOPT['wrnlog'])
                  continue
         else:
            PgOPT.action_error("Miss request record for " + cnd)
      else:
         email = PgOPT.params['EM'][i] if 'EM' in PgOPT.params else None
         if not email: PgOPT.action_error("Miss user email to add new Request")
         unames = PgDBI.get_ruser_names(email)
         if not unames: continue
         pgrec = None

      if 'RS' in PgOPT.params and PgOPT.params['RS'][i] and len(PgOPT.params['RS'][i]) > 1:
         PgOPT.params['RS'][i] = PgOPT.params['RS'][i][0]   # just in case

      record = PgOPT.build_record(flds, pgrec, tname, i)
      if record:
         if 'dsid' in record: PgOPT.validate_dsowner("dsrqst", record['dsid'])
         if 'location' in record:
            rtype = record['rqsttype'] if 'rqsttype' in record else (pgrec['rqsttype'] if pgrec['rqsttype'] else 'U')
            if 'FHQST'.find(rtype) < 0:
               PgLOG.pglog("Can not set output location for Type '{}' Request {}".format(rtype, ridx), PgOPT.PGOPT['wrnlog'])
               continue
         if pgrec:
            record['pid'] = 0
            record['lockhost'] = ''
            if not ('rqstid' in record or pgrec['rqstid']):
               record['rqstid'] = PgRqst.add_request_id(ridx, pgrec['email'])

            if 'status' in record and record['status'] == 'O':
               if not ('fcount' in record or pgrec['fcount']): pgrec['fcount'] = PgRqst.set_request_count(cnd, pgrec, 1)
               if not (pgrec['date_ready'] or 'date_ready' in record): record['date_ready'] = PgUtil.curdate()
               if not (pgrec['time_ready'] or 'time_ready' in record): record['time_ready'] = PgUtil.curtime()
               if not (pgrec['date_purge'] or 'date_purge' in record):
                  record['date_purge'] = PgUtil.adddate(record['date_ready'] if 'date_ready' in record else pgrec['date_ready'], 0, 0, PgOPT.PGOPT['VP'])
               if not (pgrec['time_purge'] or 'time_purge' in record):
                  record['time_purge'] = record['time_ready'] if 'time_ready' in record else pgrec['time_ready']
            pcnt = 0
            if 'status' in record and record['status'] == 'Q':
               if pgrec['ptcount'] == -1: record['ptcount'] = 1
               if pgrec['status'] == 'E':
                  if pgrec['ptcount'] > 1: pcnt = PgDBI.pgexec("UPDATE ptrqst SET status = 'Q' WHERE {} AND status = 'E'".format(cnd), PgOPT.PGOPT['extlog'])
                  record['ecount'] = 0
            modcnt += PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog']|PgLOG.DODFLT)
            if pcnt: PgLOG.pglog("RQST{}: SET {} Partition Status E to Q".format(ridx, pcnt), PgOPT.PGOPT['wrnlog'])
         else:
            if 'specialist' not in record:
               record['specialist'] = PgOPT.params['LN']
            elif 'MD' not in PgOPT.params and record['specialist'] != PgOPT.params['LN'] and PgOPT.params['LN'] != PgLOG.PGLOG['GDEXUSER']:
               PgOPT.action_error("Must be '{}' to add request record".format(record['specialist']))
            if 'rqsttype' not in record: record['rqsttype'] = "C"  # default to customized request type
            nidx = new_request_id()
            lname = PgLOG.convert_chars(unames['lstname'], 'RQST')
            record['rqstid'] = "{}{}".format(lname.upper(), nidx)   # auto set request ID
            record['fromflag'] = 'M'
            if 'date_rqst' not in record: record['date_rqst'] = PgUtil.curdate()
            if 'time_rqst' not in record: record['time_rqst'] = PgUtil.curtime()
            ridx = PgDBI.pgadd(tname, record, PgOPT.PGOPT['extlog']|PgLOG.AUTOID|PgLOG.DODFLT)
            if ridx > 0:
               cnd = "rindex = {}".format(ridx)
               record = {}
               pgrec = PgDBI.pgget(tname, "*", cnd, PgOPT.PGOPT['extlog'])
               if ridx != nidx: record['rqstid'] = "{}{}".format(lname.upper(), ridx)   # auto reset request ID
               if PgRqst.cache_request_control(ridx, pgrec, PgOPT.PGOPT['CACT'], 0):
                  if (PgOPT.PGOPT['RCNTL']['ptlimit'] or PgOPT.PGOPT['RCNTL']['ptsize']):
                     record['ptcount'] = 0
                     record['size_request'] = 0
                  if 'RS' not in PgOPT.params:
                     stat = 'Q' if PgOPT.PGOPT['RCNTL']['control'] == 'A' else 'W'
                     if pgrec['status'] != stat: record['status'] = stat
               if pgrec['date_ready']: record['date_ready'] = None
               if pgrec['time_ready']: record['time_ready'] = None
               if pgrec['date_purge']: record['date_purge'] = None
               if pgrec['time_purge']: record['time_purge'] = None
               if record: PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog'])
               PgLOG.pglog("{}: Request Index {} added for <{}> {}".format(PgOPT.params['DS'][i], ridx, unames['name'], email), PgOPT.PGOPT['wrnlog'])
               addcnt += 1
      elif pgrec: # unlock request
         PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])

   PgLOG.pglog("{}/{} of {} request{} added/modified in RDADB!".format(addcnt, modcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# find a unique request name/ID from given user break name
# by appending (existing maximum rindex + 1) 
#
def new_request_id():

   pgrec = PgDBI.pgget("dsrqst", "MAX(rindex) maxid", '', PgLOG.LOGERR)

   if pgrec:
      return (pgrec['maxid'] + 1)
   else:
      return 0

#
# modify request partition information
#
def set_request_partitions():

   tname = "ptrqst"
   hash = PgOPT.TBLHASH[tname]
   s = 's' if ALLCNT > 1 else ''
   ridx = 0
   ridxs = {}
   PgLOG.pglog("Set information of {} request partition{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   modcnt = 0
   flds = PgOPT.get_field_keys(tname)
   PgOPT.validate_multiple_values(tname, ALLCNT, flds)

   for i in range(ALLCNT):   
      pidx = PgOPT.params['PI'][i]
      cnd = "pindex = {}".format(pidx)
      pgrec = PgDBI.pgget(tname, "*", cnd, PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("Error get Request Partition for " + cnd)
      record = PgOPT.build_record(flds, pgrec, tname, i)
      if record and PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog']):
         modcnt += 1
         if 'status' in record and record['status'] == 'Q' and pgrec['status'] == 'E':
            ridx = pgrec['rindex']
            if ridx in ridxs:
               ridxs[ridx] += 1
            else:
               ridxs[ridx] = 1

   for ridx in ridxs:
      record = {}
      rcnd = "rindex = {}".format(ridx)
      pgrqst = PgDBI.pgget(tname, "ecount, status", rcnd, PgOPT.PGOPT['extlog'])
      if not pgrqst: PgLOG.pglog("{}: request record not in RDADB".format(ridx), PgOPT.PGOPT['extlog'])
      record['ecount'] = pgrqst['ecount'] - ridxs[ridx]
      if record['ecount'] < 0: record['ecount'] = 0
      if pgrqst['status'] == 'E': record['status'] = 'Q'
      if PgDBI.pgupdt(tname, record, rcnd, PgOPT.PGOPT['extlog']):
         if 'status' in record: PgLOG.pglog("RQST{}: SET Request Status E to Q".format(ridx), PgOPT.PGOPT['wrnlog'])

   PgLOG.pglog("{} of {} request partition{} modified in RDADB!".format(modcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# add or modify request control information
#
def set_request_control():

   tname = "rcrqst"
   hash = PgOPT.TBLHASH[tname]
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Set information of {} request control{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   addcnt = modcnt = 0
   flds = PgOPT.get_field_keys(tname, None, 'C')
   PgOPT.validate_multiple_values(tname, ALLCNT, flds)
   dsids = {}
   pcnts = {}
   for i in range(ALLCNT):  
      cidx = PgOPT.params['CI'][i] if 'CI' in PgOPT.params else 0
      if cidx > 0:
         cnd = "cindex = {}".format(cidx)
         pgrec = PgDBI.pgget(tname, "*", cnd, PgOPT.PGOPT['extlog'])
         if not pgrec: PgOPT.action_error("Miss control record for " + cnd)
         if 'MD' not in PgOPT.params and pgrec['specialist'] != PgOPT.params['LN'] and PgOPT.params['LN'] != PgLOG.PGLOG['GDEXUSER']:
            PgOPT.action_error("{}: Must be '{}' to set reuqest control {}".format(PgOPT.params['LN'], pgrec['specialist'], cnd))
      else:
         pgrec  = None

      record = PgOPT.build_record(flds, pgrec, tname, i)
      if record:
         dsid = record['dsid'] if 'dsid' in record else pgrec['dsid']
         if 'gindex' in record and record['gindex']:
            grec = PgDBI.pgget("dsgroup", "pindex", "dsid = '{}' AND gindex = {}".format(dsid, record['gindex']), PgOPT.PGOPT['extlog'])
            if not grec:
               PgLOG.pglog("Group Index {}: not exists in '{}'".format(record['gindex'], dsid), PgLOG.LOGERR)
               continue
            elif grec['pindex']:
               PgLOG.pglog("Group Index {}: not a top group in '{}'".format(record['gindex'], dsid), PgLOG.LOGERR)
               continue
         if pgrec:
            if dsid != pgrec['dsid']:
               PgLOG.pglog("pgrec['dsid']-pgrec['cindex']: Cannot change dataset to pgrec['dsid'] for existing Request Control", PgLOG.LOGERR)
               continue
            modcnt += PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog'])
         else:
            if 'rqsttype' not in record:
               PgLOG.pglog("Missing Request Type to add Request Control", PgLOG.LOGERR)
               continue

            if 'specialist' not in record:
               record['specialist'] = PgOPT.params['LN']
            elif 'MD' not in PgOPT.params and record['specialist'] != PgOPT.params['LN'] and PgOPT.params['LN'] != PgLOG.PGLOG['GDEXUSER']:
               PgOPT.action_error("{}: Must be '{}' to add request control record".format(PgOPT.params['LN'], record['specialist']))
            cidx = PgDBI.pgadd(tname, record, PgOPT.PGOPT['extlog']|PgLOG.AUTOID)
            if cidx:
               PgLOG.pglog("Request Control Index {} added".format(cidx), PgOPT.PGOPT['wrnlog'])
               addcnt += 1

         if 'rqsttype' in record and dsid not in dsids:
            rtype = record['rqsttype']
            if dsid not in pcnts: pcnts[dsid] = {}
            if rtype not in pcnts[dsid]:
               pcnts[dsid][rtype] = PgDBI.pgget(tname, "", "dsid = '{}' AND rqsttype = '{}'".format(dsid, rtype))
               if pcnts[dsid][rtype] == 1: dsids[dsid] = 1

   PgLOG.pglog("{}/{} of {} request control{} added/modified in RDADB!".format(addcnt, modcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# add or modify information for tar files of requested online web files
#
def set_tar_files(rindex):

   tname = "tfrqst"
   hash = PgOPT.TBLHASH[tname]
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Set information of {} tar file{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   modcnt = 0
   flds = PgOPT.get_field_keys(tname, None, "B")   # exclude dataset
   PgOPT.validate_multiple_values(tname, ALLCNT, flds)
   if 'RO' in PgOPT.params and not PgOPT.params['DO']: flds += 'O'   
   fields = PgOPT.get_string_fields(flds, tname)
   dsids = PgRqst.get_request_dsids(PgOPT.params['RI'])

   ridx = rindex if rindex else 0
   for i in range(ALLCNT):
      if not rindex and ridx != PgOPT.params['RI'][i]:
         ridx = PgLock.lock_request(PgOPT.params['RI'][i], 1, PgOPT.PGOPT['extlog'])
         if ridx <= 0: continue
      if 'TI' in PgOPT.params:
         tcnd = "tindex = {}".format(PgOPT.params['TI'][i])
      else:
         tcnd = "wfile = '{}'".format(PgOPT.params['WF'][i])
      cnd = "rindex = {} AND {}".format(ridx, tcnd)
      pgrec = PgDBI.pgget(tname, fields, cnd, PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("Error get Tar File info for " + cnd)
      if 'RO' in PgOPT.params: PgOPT.params['DO'][i] = PgRqst.get_next_disp_order(ridx, tname)
      record = PgOPT.build_record(flds, pgrec, tname, i)
      if record:
         modcnt += PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog']|PgLOG.DODFLT)

      if not rindex and (i > (ALLCNT - 2) or  ridx != PgOPT.params['RI'][i + 1]):
         PgRqst.set_request_count("rindex = {}".format(ridx))
         PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])   # unlock requests

   PgLOG.pglog("{} of {} tar file{} modified in RDADB!".format(modcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# add or modify requested online web file information
#
def set_web_files(rindex = 0):

   tname = "wfrqst"
   stypes = 'CMW'
   hash = PgOPT.TBLHASH[tname]
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Set information of {} requested file{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   addcnt = modcnt = 0
   flds = PgOPT.get_field_keys(tname, None, "B")   # exclude dataset
   PgOPT.validate_multiple_values(tname, ALLCNT, flds)
   if 'RO' in PgOPT.params and not PgOPT.params['DO']: flds += 'O'   
   fields = PgOPT.get_string_fields(flds, tname)
   if 'SL' in PgOPT.params:
      dsids = PgRqst.get_request_dsids(PgOPT.params['RI'])
      PgOPT.params['SL'] = PgRqst.fname2fid(PgOPT.params['SL'], dsids, PgOPT.params['OT'])

   ridx = rindex if rindex else 0
   for i in range(ALLCNT):
      if not rindex and ridx != PgOPT.params['RI'][i]:
         ridx = PgLock.lock_request(PgOPT.params['RI'][i], 1, PgOPT.PGOPT['extlog'])
         if ridx <= 0: continue
      cnd = "rindex = {} AND wfile = '{}'".format(ridx, PgOPT.params['WF'][i])
      pgrec = PgDBI.pgget(tname, fields, cnd, PgOPT.PGOPT['extlog'])
      if 'RO' in PgOPT.params: PgOPT.params['DO'][i] = PgRqst.get_next_disp_order(ridx, tname)
      record = PgOPT.build_record(flds, pgrec, tname, i)
      if record:
         if 'srctype' in record and stypes.find(record['srctype']) < 0:
           PgOPT.action_error("{}: Source type must be one of '{}'".format(record['srctype'], stypes))
         if pgrec:
            modcnt += PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog']|PgLOG.DODFLT)
         else:
            if not ('disp_order' in record and record['disp_order']): record['disp_order'] = PgRqst.get_next_disp_order(ridx, tname)
            addcnt += PgDBI.pgadd(tname, record, PgOPT.PGOPT['extlog']|PgLOG.DODFLT)

      if not rindex and (i > (ALLCNT-2) or ridx != PgOPT.params['RI'][i+1]):
         PgRqst.set_request_count("rindex = {}".format(ridx))
         PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])   # unlock requests

   PgLOG.pglog("{}/{} of {} request file{} added/modified in RDADB!".format(addcnt, modcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# unlock requests for given request indices
#
def unlock_request_info():

   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Unlock {} request{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   modcnt = 0
   for ridx in PgOPT.params['RI']:
      pgrec = PgDBI.pgget("dsrqst", "pid, lockhost", "rindex = {}".format(ridx), PgOPT.PGOPT['extlog'])
      if not pgrec:
         PgLOG.pglog("Request {}: Not exists".format(ridx), PgOPT.PGOPT['errlog'])
      elif not pgrec['pid']:
         PgLOG.pglog("Request {}: Not locked".format(ridx), PgOPT.PGOPT['wrnlog'])
      elif pgrec['lockhost'] == "partition":
         PgLOG.pglog("Request {}: Partition of the request are under processing".format(ridx), PgOPT.PGOPT['wrnlog'])
      elif PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog']) > 0:
         modcnt += 1
         PgLOG.pglog("Request ridx: Unlocked {}/{}".format(ridx, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['wrnlog'])
      elif (PgFile.check_host_down(None, pgrec['lockhost']) and
            PgLock.lock_request(ridx, -2, PgOPT.PGOPT['extlog']) > 0):
         modcnt += 1
         PgLOG.pglog("Request {}: Force unlocked {}/{}".format(ridx, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['wrnlog'])
      else:
         PgLOG.pglog("Request {}: Unable to unlock {}/{}".format(ridx, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['wrnlog'])

   if ALLCNT > 1: PgLOG.pglog("{} of {} request{} unlocked from RDADB".format(modcnt, ALLCNT), PgLOG.LOGWRN) 

#
# unlock request partitions for given partition indices
#
def unlock_partition_info():

   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Unlock {} request partition{} ...".format(ALLCNT, s), PgLOG.WARNLG)

   modcnt = 0
   for pidx in PgOPT.params['PI']:
      pgrec = PgDBI.pgget("ptrqst", "pid, lockhost", "pindex = {}".format(pidx), PgOPT.PGOPT['extlog'])
      if not pgrec:
         PgLOG.pglog("Request Paritition {}: Not exists".format(pidx), PgOPT.PGOPT['errlog'])
      elif not pgrec['pid']:
         PgLOG.pglog("Request Partition {}: Not locked".format(pidx), PgOPT.PGOPT['wrnlog'])
      elif PgLock.lock_partition(pidx, 0, PgOPT.PGOPT['extlog']) > 0:
         modcnt += 1
         PgLOG.pglog("Request Paritition {}: Unlocked {}/{}".format(pidx, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['wrnlog'])
      elif (PgFile.check_host_down(None, pgrec['lockhost']) and
            PgLock.lock_partition(pidx, -2, PgOPT.PGOPT['extlog']) > 0):
         modcnt += 1
         PgLOG.pglog("Request Paritition {}: Force unlocked {}/{}".format(pidx, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['wrnlog'])
      else:
         PgLOG.pglog("Request Paritition {}: Unable to unlock {}/{}".format(pidx, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['wrnlog'])

   if ALLCNT > 1: PgLOG.pglog("{} of {} request partition{} unlocked from RDADB".format(modcnt, ALLCNT, s), PgLOG.LOGWRN) 

#
# interrupt requests for given request indices
#
def interrupt_requests():

   s = 's' if ALLCNT > 1 else ''
   delcnt = 0
   for i in range(ALLCNT) :
      ridx = PgOPT.params['RI'][i]
      cnd = "rindex = {}".format(ridx)
      pgrec = PgDBI.pgget("dsrqst", "dsid, pid, lockhost, status", cnd, PgOPT.PGOPT['extlog'])
      if not pgrec: PgLOG.pglog("{}: Request Index not in RDADB".format(ridx), PgOPT.PGOPT['extlog'])
      rstr = "Request {} of {}".format(ridx, pgrec['dsid'])
      if pgrec['status'] != "Q":
         PgLOG.pglog("{}: Status '{}'; must be 'Q' to interrupt".format(rstr, pgrec['status']), PgOPT.PGOPT['errlog'])
         continue

      pid = pgrec['pid']
      if pid == 0:
         PgLOG.pglog(rstr + ": Request is not under process; no interruption", PgOPT.PGOPT['wrnlog'])
         continue

      host = pgrec['lockhost']
      if host == "partition":
         pgparts = PgDBI.pgmget("ptrqst", "pindex", cnd + " AND pid > 0", PgOPT.PGOPT['extlog'])
         if pgparts:
            interrupt_partitions(pgparts['pindex'])
      else:
         if not PgFile.local_host_action(host, "interrupt request", rstr, PgOPT.PGOPT['errlog']): continue
   
         opts = "-h {} -p {}".format(host, pid)
         buf = PgLOG.pgsystem("rdaps " + opts, PgLOG.LOGWRN, 20)   # 21 = 4 + 16
         if buf:
            ms = re.match(r'^\s*(\w+)\s+', buf)
            if ms:
               uid = ms.group(1)
               if uid != PgOPT.params['LN']:
                  PgLOG.pglog("{}: Must be '{}' to interrupt {}".format(PgOPT.params['LN'], uid, rstr), PgOPT.PGOPT['wrnlog'])
                  continue
               if 'FI' not in PgOPT.params:
                  PgLOG.pglog(": locked by {}/{}; must add Mode option -FI (-ForceInterrupt) to interrupt".format(rstr, pid, host), PgOPT.PGOPT['wrnlog'])
                  continue
               if not PgLOG.pgsystem("rdakill " + opts, PgLOG.LOGWRN, 7):
                  PgLOG.pglog("{}: Failed to interrupt Request locked by {}/{}".format(rstr, pid, host), PgOPT.PGOPT['errlog'])
                  continue
         else:
            PgLOG.pglog("{}: Request process stopped already for {}/{}".format(rstr, pid, host), PgOPT.PGOPT['wrnlog'])
   
         pgrec = PgDBI.pgget("dsrqst", "pid, lockhost", cnd, PgOPT.PGOPT['extlog'])
         if not pgrec['pid']:
            if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['extlog']) <= 0: continue
         elif pid != pgrec['pid'] or host != pgrec['lockhost']:
            PgLOG.pglog("{}: Request is relocked by {}/{}".format(rstr, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['errlog'])
            continue

      record = {'pid' : 0, 'status' : 'I'}
      if PgDBI.pgupdt("dsrqst", record, cnd, PgOPT.PGOPT['extlog']):
         pgrec = PgDBI.pgget("dscheck", "*", "oindex = {} AND command = 'dsrqst' AND otype <> 'P'".format(ridx), PgOPT.PGOPT['extlog'])
         if pgrec:
            pgrec['status'] = 'I'
            PgCMD.delete_dscheck(pgrec, None, PgOPT.PGOPT['extlog'])
         delcnt += 1

   if ALLCNT > 1: PgLOG.pglog("{} of {} request{} interrupted".format(delcnt, ALLCNT, s), PgLOG.LOGWRN)

#
# interrupt request partitions for given partition indices
#
def interrupt_partitions(pindices = None, pcnt = 0):

   if not pindices: pindices = PgOPT.params['PI']
   if not pindices: return

   if not pcnt: pcnt = len(pindices)
   s = "s" if (pcnt > 1) else ""
   dcnt = 0
   for i in range(pcnt):
      pidx = pindices[i]
      cnd = "pindex = {}".format(pidx)
      pgrec = PgDBI.pgget("ptrqst", "dsid, pid, lockhost, status", cnd, PgOPT.PGOPT['extlog'])
      if not pgrec: PgLOG.pglog("Request Paritition {}: not in RDADB".format(pidx), PgOPT.PGOPT['extlog'])
      pstr = "Request Paritition {} of {}".format(pidx, pgrec['dsid'])
      if pgrec['status'] != "Q":
         PgLOG.pglog("{}: Status '{}'; must be 'Q' to interrupt".format(pstr, pgrec['status']), PgOPT.PGOPT['errlog'])
         continue

      pid = pgrec['pid']
      if pid == 0:
         PgLOG.pglog(pstr + ": not under process; no interruption", PgOPT.PGOPT['wrnlog'])
         continue

      host = pgrec['lockhost']
      if not PgFile.local_host_action(host, "interrupt partition", pstr, PgOPT.PGOPT['errlog']):
         continue

      opts = "-h {} -p {}".format(host, pid)
      buf = PgLOG.pgsystem("rdaps " + opts, PgLOG.LOGWRN, 20)   # 21 = 4 + 16
      if buf:
         ms = re.match(r'^\s*(\w+)\s+', buf)
         if ms:
            uid = ms.group(1)
            if uid != PgOPT.params['LN']:
               PgLOG.pglog("{}: Must be '{}' to interrupt {}".format(PgOPT.params['LN'], uid, pstr), PgOPT.PGOPT['wrnlog'])
               continue
            if 'FI' not in PgOPT.params:
               PgLOG.pglog("{}: Locked by {}/{}; must add Mode option -FI (-ForceInterrupt) to interrupt".format(pstr, pid, host), PgOPT.PGOPT['wrnlog'])
               continue
            if not PgLOG.pgsystem("rdakill " + opts, PgLOG.LOGWRN, 7):
               PgLOG.pglog("{}: Failed to interrupt, Request Partition locked by {}/{}".format(pstr, pid, host), PgOPT.PGOPT['errlog'])
               continue
      else:
         PgLOG.pglog("{}: Request process stopped already for {}/{}".format(pstr, pid, host), PgOPT.PGOPT['wrnlog'])

      pgrec = PgDBI.pgget("ptrqst", "pid, lockhost", cnd, PgOPT.PGOPT['extlog'])
      if not pgrec['pid']:
         if PgLock.lock_partition(pidx, 1, PgOPT.PGOPT['extlog']) <= 0: continue
      elif pid != pgrec['pid'] or host != pgrec['lockhost']:
         PgLOG.pglog("{}: Relocked by {}/{}".format(pstr, pgrec['pid'], pgrec['lockhost']), PgOPT.PGOPT['errlog'])
         continue

      record = {'status' : 'I', 'pid' : 0}
      if (PgDBI.pgupdt("ptrqst", record, cnd, PgOPT.PGOPT['extlog']) and
          PgLock.lock_partition(pidx, 0, PgOPT.PGOPT['extlog']) > 0):
         pgrec = PgDBI.pgget("dscheck", "*", "oindex = {} AND command = 'dsrqst' and otype = 'P'".format(pidx), PgOPT.PGOPT['extlog'])
         if pgrec:
            pgrec['status'] = 'I'
            PgCMD.delete_dscheck(pgrec, None, PgOPT.PGOPT['extlog'])
         dcnt += 1

   if pcnt > 1: PgLOG.pglog("{} of {} request partition{} interrupted".format(dcnt, pcnt, s), PgLOG.LOGWRN)

#
# add request partitions
#
def add_request_partitions():

   s = "s" if ALLCNT > 1 else ""
   PgLOG.pglog("Add partitions to {} Request{} ...".format(ALLCNT, s), PgLOG.WARNLG)
   indices = PgOPT.params['RI']

   mcnt = 0
   for i in range(ALLCNT):
      ridx = indices[i]
      cnd = "rindex = {}".format(ridx)
      pgrec = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
      if not pgrec: return PgLOG.pglog("can not get Request info for " + cnd, PgOPT.PGOPT['errlog'])
      if not PgRqst.cache_request_control(ridx, pgrec, 'SP'): continue
      if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['errlog']) <= 0: continue
      pcnt = add_one_request_partitions(ridx, cnd, pgrec)
      if not pcnt: continue   # adding partitions failed
      if pcnt > 1: mcnt += pcnt
      if ALLCNT > 1: continue      
#       continue if(PgRqst.request_limit())
      if pcnt == 1 and finish_one_request(ridx):
         PgLOG.pglog("RQST{}: request is built after no partition added".format(ridx), PgOPT.PGOPT['wrnlog'])

   if mcnt > 1:
      msg = "{} partitions added to {} request{} Successfully by {}".format(mcnt, ALLCNT, s, PgLOG.PGLOG['CURUID'])
      if PgLOG.PGLOG['CURUID'] != PgOPT.params['LN']: msg += " for {}".format(PgOPT.params['LN'])
      PgLOG.pglog(msg, PgOPT.PGOPT['wrnlog'])

   return mcnt

# unlock request and display/log error
def request_error(ridx, errmsg):

   PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])
   return PgLOG.pglog(errmsg, PgOPT.PGOPT['errlog'])

# unlock partition and display/log error
def partition_error(pidx, errmsg):

   PgLock.lock_partition(pidx, 0, PgOPT.PGOPT['extlog'])
   return PgLOG.pglog(errmsg, PgOPT.PGOPT['errlog'])

#
# add partitions to one request
#
def add_one_request_partitions(ridx, cnd, pgrqst, ptcmp = 0):

   rstat = pgrqst['status']
   rtype = pgrqst['rqsttype']
   rstr = "RQST{}-{}".format(ridx, pgrqst['dsid'])
   if pgrqst['specialist'] != PgOPT.params['LN']:
      return request_error(ridx, "{}: Must be '{}' to add partitions for {}".format(PgOPT.params['LN'], pgrqst['specialist'], rstr))

   if rstat != 'Q':
      if ('RS' in PgOPT.params and PgOPT.params['RS'][0] == 'Q' and
          PgDBI.pgexec("UPDATE dsrqst set status = 'Q' WHERE " + cnd, PgOPT.PGOPT['extlog'])):
         rstat = pgrqst['status'] = 'Q'
      else:
         return request_error(ridx, ": Status '{}', must be 'Q' to add partitions".format(rstr, rstat))

   pgcntl = PgOPT.PGOPT['RCNTL']
   if ptcmp:
      ptlimit = CMPLMT
      ptsize = 0
      cmd = None
   else:
      cmd = pgcntl['command']
      ptcmp = -1 if (pgrqst['tarflag'] == 'Y' or pgrqst['file_format']) and 'NP'.find(pgcntl['ptflag']) > -1 else 0
      if pgcntl['ptlimit']:
         ptlimit = get_partition_limit(pgcntl['ptlimit'], ptcmp)
         ptsize = 0
      else:
         ptlimit = 0
         ptsize = get_partition_size(pgcntl['ptsize'], ptcmp)

   if not (ptlimit or ptsize):
      if pgrqst['ptcount'] != 1: PgDBI.pgexec("UPDATE dsrqst set ptcount = 1 WHERE " + cnd, PgOPT.PGOPT['extlog'])
      return request_error(ridx, "{}: Not configured for partitioning by RC{}".format(rstr, pgcntl['cindex']))

   pcnt = PgDBI.pgget("ptrqst", "", cnd)
   if pcnt > 1:
      if pgrqst['ptcount'] != pcnt: PgDBI.pgexec("UPDATE dsrqst set ptcount = {} WHERE {}".format(pcnt, cnd), PgOPT.PGOPT['extlog'])
      return request_error(ridx, rstr + ": partitions added already")
   elif pcnt == 1:
      PgDBI.pgexec("UPDATE dsrqst set ptcount = 0 WHERE " + cnd, PgOPT.PGOPT['extlog'])
      PgDBI.pgdel("ptrqst", cnd, PgOPT.PGOPT['extlog'])

   syserr = ''
   if cmd:
      create_request_directory(pgrqst)   # create directory before set partitions 
      PgLOG.PGLOG['ERR2STD'] = ["Warning: "]
      PgLOG.pgsystem(cmd, PgLOG.LOGWRN, 261)   # 261 = 256 + 4 + 1
      PgLOG.PGLOG['ERR2STD'] = []
      if PgLOG.PGLOG['SYSERR']: syserr = "\n" + PgLOG.PGLOG['SYSERR']
      pgrqst = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
      if not pgrqst: return request_error(ridx, "{}: Error reget request record after {}{}".format(rstr, cmd, syserr))
      if pgrqst['status'] != 'Q':
         return request_error(ridx, "{}: status Q changed to {} after {}{}".format(rstr, pgrqst['status'], cmd,syserr))

   fields = 'findex, wfile, size, ofile'
   pgrecs = PgDBI.pgmget("wfrqst", fields, cnd + " ORDER BY wfile", PgOPT.PGOPT['extlog'])
   tcnt = len(pgrecs['wfile']) if pgrecs else 0
   if not tcnt and (syserr or pgcntl['empty_out'] != 'Y'):
      msg = "{}: NO file information found for partitioning{}".format(rstr, syserr)
      PgOPT.send_request_email_notice(pgrqst, msg, 0, 'E')
      PgDBI.pgexec("UPDATE dsrqst set status = 'E', ecount = ecount + 1 WHERE " + cnd, PgOPT.PGOPT['extlog'])
      return request_error(ridx, msg)

   # check and set input file size
   insize = 0
   for i in range(tcnt):
      if not pgrecs['size'][i]:
         finfo = PgFile.check_local_file(pgrecs['wfile'][i])
         if not finfo and pgrecs['ofile'][i]:
            finfo = PgFile.check_local_file(pgrecs['ofile'][i])
         if finfo and PgDBI.pgexec("UPDATE wfrqst SET size = {} WHERE findex = {}".format(finfo['data_size'], pgrecs['findex'][i]), PgOPT.PGOPT['extlog']):
            pgrecs['size'][i] = finfo['data_size']
      if pgrecs['size'][i]: insize += pgrecs['size'][i]

   if ptlimit:
      if tcnt <= ptlimit:
         PgLOG.pglog("{}: NO partition needed, file count {} < {}".format(rstr, tcnt, ptlimit), PgLOG.LOGWRN)
         pcnt = 1
      else:
         pcnt = int(tcnt/ptlimit)
         if pcnt > PgOPT.PGOPT['PTMAX']:
            PgLOG.pglog("{}: Too many partitions({}) for partition file count {}".format(rstr, pcnt, ptlimit), PgLOG.LOGWRN)
            ptlimit = int(tcnt/PgOPT.PGOPT['PTMAX'] + 1)
            PgLOG.pglog("{}: Increase partition file count to {} for total {}".format(rstr, ptlimit, tcnt), PgLOG.LOGWRN)
         else:
            ptlimit = int(tcnt/(int(tcnt/ptlimit)+1)+1)
         pcnt = 0
   else:
      if not insize and tcnt > 0: return request_error(ridx, "{}: NO size information found for partitioning{}".format(rstr, syserr))
      if insize <= ptsize:
         PgLOG.pglog("{}: NO partition needed, data size {} < {}".format(rstr, insize, ptsize), PgLOG.LOGWRN)
         pcnt = 1
      else:
         pcnt = int(insize/ptsize)
         if pcnt > PgOPT.PGOPT['PTMAX']:
            PgLOG.pglog("{}: Too many partitions({}) for partition data size {}".format(rstr, pcnt, ptsize), PgLOG.LOGWRN)
            ptsize = int(insize/PgOPT.PGOPT['PTMAX'] + 1)
            PgLOG.pglog("{}: Increase partition data size to {} for total {}".format(rstr, ptsize, insize), PgLOG.LOGWRN)
         pcnt = 0

   if pcnt == 0:   # add partitions
      addrec = {'rindex' : ridx, 'dsid' : pgrqst['dsid'], 'specialist' : pgrqst['specialist']}
      if ptcmp > 0: addrec['ptcmp'] = 'Y'
      modrec = {'status' : (PgOPT.params['PS'][0] if ('PS' in PgOPT.params and PgOPT.params['PS'][0]) else rstat)}
      pidx = 0
      for i in range(tcnt):
         if pidx == 0:
            addrec['ptorder'] = pcnt
            pcnt += 1
            pidx = PgDBI.pgadd("ptrqst", addrec, PgOPT.PGOPT['extlog']|PgLOG.AUTOID|PgLOG.DODFLT)
            pcnd = "pindex = {}".format(pidx)
            fcnd = "{} AND wfile BETWEEN '{}' AND ".format(cnd, pgrecs['wfile'][i])
            fcnt = fsize = 0
         fcnt += 1
         if ptlimit:
             if fcnt < ptlimit: continue
         else:
            fsize += pgrecs['size'][i]
            if fsize < ptsize: continue

         # skip the break partition if remaining file count is 1 or less than 10% of previous partition 
         mcnt = tcnt-i-1
         if mcnt > 0 and (mcnt == 1 or (10*mcnt) < fcnt): continue

         fcnd += "'{}'".format(pgrecs['wfile'][i])
         PgDBI.pgexec("UPDATE wfrqst SET {} WHERE {}".format(pcnd, fcnd), PgOPT.PGOPT['extlog'])
         modrec['fcount'] = fcnt
         PgDBI.pgupdt("ptrqst", modrec, pcnd, PgOPT.PGOPT['extlog'])
         if pcnt == 1 and ptcmp < 1: add_dynamic_partition_options(pidx, pgrqst, modrec, pcnd)
         pidx = 0

      if pidx:
         fcnd += "'{}'".format(pgrecs['wfile'][tcnt-1])
         PgDBI.pgexec("UPDATE wfrqst SET {} WHERE {}".format(pcnd, fcnd), PgOPT.PGOPT['extlog'])
         modrec['fcount'] = fcnt
         PgDBI.pgupdt("ptrqst", modrec, pcnd, PgOPT.PGOPT['extlog'])

   record = {'ptcount' : pcnt, 'pid' : 0}
   if insize and insize > pgrqst['size_input']: record['size_input'] = insize
   if not pgrqst['fcount'] or pgrqst['fcount'] < 0: record['fcount'] = tcnt
   PgDBI.pgupdt("dsrqst", record, cnd, PgOPT.PGOPT['extlog'])
   if pcnt > 1: PgLOG.pglog("{}: {} partitions Added".format(rstr, pcnt), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

   return pcnt

#
# get the dynamic option values for a partition
#
def add_dynamic_partition_options(pidx, pgrqst, modrec, pcnd):

   pgctl = PgCMD.get_dsrqst_control(pgrqst)
   if pgctl:
      pgptctl = {}
      for bkey in PgCMD.BOPTIONS:
         if bkey in pgctl and pgctl[bkey]:
            ms = re.match(r'^!(.+)$', pgctl[bkey])
            if ms:
               options = PgCMD.get_dynamic_options(ms.group(1), pidx, 'P')
               if options: pgptctl[bkey] = options
      if pgptctl:
         PgDBI.pgupdt("ptrqst", pgptctl, pcnd, PgOPT.PGOPT['extlog'])
         modrec.update(pgptctl)

#
# reduce ptlimit for more partitions if compression
#
def get_partition_limit(ptlimit, ptcmp = 0):

   if ptcmp and ptlimit > CMPLMT:
      ptlimit = int(ptlimit/6.0)
      if ptlimit < CMPLMT: ptlimit = CMPLMT

   return ptlimit

#
# reduce ptsize for more partitions if compression
#
def get_partition_size(ptsize, ptcmp = 0):

   minsize = 3000000000

   if ptcmp and ptsize > minsize:
      ptsize = int(ptsize/6.0)
      if ptsize < minsize: ptsize = minsize

   return ptsize

#
# build requests
#
def build_requests():

   s = "s" if ALLCNT > 1 else ""
   PgLOG.pglog("Build {} Request{} ...".format(ALLCNT, s), PgLOG.WARNLG)
   indices = PgOPT.params['RI']

   mcnt = 0
   for i in range(ALLCNT):
#       break if(request_limit())   # exceed total request limit
      ridx = indices[i]
      cnd = "rindex = {}".format(ridx)
      pgrqst = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
      if not pgrqst:
         PgLOG.pglog("RQST{}: can not get Request info".format(ridx), PgOPT.PGOPT['errlog'])
         continue

      if pgrqst['ptcount'] == 0:
         if not PgRqst.cache_request_control(ridx, pgrqst, 'SP'): continue
         if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['errlog']) <= 0: continue
         pgrqst['ptcount'] = add_one_request_partitions(ridx, cnd, pgrqst)
         if not pgrqst['ptcount']: continue   # adding partitions failed

      if pgrqst['ptcount'] > 1:
         pidx = finish_one_partition(ridx, cnd)
         if pidx: PgLOG.pglog("RPT{}: procssed for Rqst{}".format(pidx, ridx), PgOPT.PGOPT['errlog'])
         mcnt += finish_one_request(ridx, pidx)
      else:
         if not PgRqst.cache_request_control(ridx, pgrqst, 'BR'): continue
         if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['errlog']) <= 0: continue
         mcnt += build_one_request(ridx, cnd, pgrqst)
   
   if mcnt > 1:
      msg = "{} of {} request{} built Successfully by {}".format(mcnt, ALLCNT, PgLOG.PGLOG['CURUID'])
      if PgLOG.PGLOG['CURUID'] != PgOPT.params['LN']: msg += " for " + PgOPT.params['LN']
      PgLOG.pglog(msg, PgOPT.PGOPT['wrnlog'])

   return mcnt

#
# process request partitions
#
def process_partitions():

   s = "s" if ALLCNT > 1 else ""
   PgLOG.pglog("Process {} Request Partition{} ...".format(ALLCNT, s), PgLOG.WARNLG)
   indices = PgOPT.params['PI']

   mcnt = 0
   for i in range(ALLCNT):
#      if request_limit(): break   # exceed total request limit
      pidx = indices[i]
      cnd = "pindex = {}".format(pidx)
      pgpart = PgDBI.pgget("ptrqst", "*", cnd, PgOPT.PGOPT['extlog'])
      if not pgpart: return PgLOG.pglog("RPT{}: can not get Request Partition info".format(pidx), PgOPT.PGOPT['errlog'])

      ridx = pgpart['rindex']
      pgrqst = PgDBI.pgget("dsrqst", "*", "rindex = {}".format(ridx), PgOPT.PGOPT['extlog'])
      if not pgrqst: return PgLOG.pglog("RQST{}: can not get Request info".format(ridx), PgOPT.PGOPT['errlog'])
      if not PgRqst.cache_request_control(ridx, pgrqst, 'PP', pidx): continue
      if PgLock.lock_partition(pidx, 1, PgOPT.PGOPT['errlog']) <= 0: continue
      mcnt += process_one_partition(pidx, cnd, pgpart, ridx, pgrqst)
      if ALLCNT == 1 and mcnt > 0 and finish_one_request(ridx, pidx):
         PgLOG.pglog("RQST{}: built after RPT{} is processed".format(ridx, pidx), PgOPT.PGOPT['wrnlog'])
   
   if mcnt > 1:
      msg = "{} of {} request partition{} processed by {}".format(mcnt, ALLCNT, s, PgLOG.PGLOG['CURUID'])
      if PgLOG.PGLOG['CURUID'] != PgOPT.params['LN']: msg += " for " + PgOPT.params['LN']
      PgLOG.pglog(msg, PgOPT.PGOPT['wrnlog'])

   return mcnt

#
# try to finish building a request after its partions are all processed
#
def finish_one_request(ridx, pidx = 0):

   cnd = "rindex = {}".format(ridx)
   if PgDBI.pgget('ptrqst', "", cnd + " AND status <> 'O'", PgOPT.PGOPT['extlog']): return 0   # partition not done yet
   pgrqst = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
   if not pgrqst: return PgLOG.pglog("RQST{}: can not get Request info".format(ridx), PgOPT.PGOPT['errlog'])
   if pidx: PgCMD.change_dscheck_oinfo(pidx, 'P', ridx, 'R')
   if not PgRqst.cache_request_control(ridx, pgrqst, 'BR'): return 0
   if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['errlog']) <= 0: return 0

   return build_one_request(ridx, cnd, pgrqst)

#
#  finish one partition for a given request index
#
def finish_one_partition(ridx, cnd):

   pgrqst = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
   if not pgrqst: return PgLOG.pglog("RQST{}: can not get Request info".format(ridx), PgOPT.PGOPT['errlog'])

   # get the first queued partition
   pgpart = PgDBI.pgget("ptrqst", "*", cnd + " AND status = 'Q' AND pid = 0 ORDER by ptorder", PgOPT.PGOPT['extlog'])
   if not pgpart: return PgLOG.pglog("RQST{}: No queued Partition found to be processed".format(ridx), PgOPT.PGOPT['wrnlog'])
   pidx = pgpart['pindex']
   PgCMD.change_dscheck_oinfo(ridx, 'R', pidx, 'P')
   if not PgRqst.cache_request_control(ridx, pgrqst, 'PP', pidx): return 0
   if PgLock.lock_partition(pidx, 1, PgOPT.PGOPT['errlog']) <= 0: return 0

   return process_one_partition(pidx, "pindex = {}".format(pidx), pgpart, ridx, pgrqst)

#
# build one request
#
def build_one_request(ridx, cnd, pgrqst):

   global ERRMSG, CMPCNT
   rstat = pgrqst['status']
   rtype = pgrqst['rqsttype']
   rstr = "RQST{}-{}".format(ridx, pgrqst['dsid'])
   if pgrqst['specialist'] != PgOPT.params['LN']:
      return request_error(ridx, "{}: Must be '{}' to build {}".format(PgOPT.params['LN'], pgrqst['specialist'], rstr))

   if rstat != 'Q':
      if ('RS' in PgOPT.params and PgOPT.params['RS'][0] == 'Q' and
          PgDBI.pgexec("UPDATE dsrqst set status = 'Q' WHERE " + cnd)):
         rstat = pgrqst['status'] = 'Q'
         if pgrqst['ptcount'] == -1 and PgDBI.pgexec("UPDATE dsrqst set ptcount = 1 WHERE " + cnd):
            pgrqst['ptcount'] = 1
      else:
         return request_error(ridx, rstr + ": Status '{}', must be 'Q' to build".format(rstat))

   pgcntl = PgOPT.PGOPT['RCNTL']
   fcount = pgrqst['fcount']
   errmsg = ""
   rstat = "O"
   if pgrqst['location']:
      PgLOG.PGLOG['FILEMODE'] = 0o666
      PgLOG.PGLOG['EXECMODE'] = 0o777
   if pgrqst['ptcount'] == 0 and (pgcntl['ptlimit'] or pgcntl['ptsize']):
      return PgLOG.pglog("Set Partitions for partition-controlled request: dsrqst SP -NP -RI {}".format(ridx), PgOPT.PGOPT['errlog'])
   if pgrqst['ptcount'] < 2 or 'BF'.find(pgcntl['ptflag']) > -1:
      etime = time.time()
      cmd = pgcntl['command']
      if not (fcount or cmd or rtype == "C" and 'LF' in PgOPT.params):  # should not happen normally
         record = {'status' : 'E', 'pid' : 0}
         PgDBI.pgupdt("dsrqst", record, cnd, PgOPT.PGOPT['extlog'])
         return PgLOG.pglog("No enough information to build " + rstr, PgOPT.PGOPT['errlog'])
      if rtype == "F" or rtype == "A":
         (rstat, errmsg) = stage_convert_files(ridx, cnd, rstr, pgrqst, errmsg, cmd, rtype)
         cmd = None   # do not call command any more
      elif rtype == "C":
         stage_local_files(ridx, cnd, rstr, pgrqst)
      if rstat == 'O' and cmd:
         cret = call_command(ridx, cnd, cmd, rstr, pgrqst, 0, None)
         if 'pgrqst' in cret: pgrqst = cret['pgrqst']
         if 'errmsg' in cret:
            rstat = 'E'
            if cret['errmsg']: errmsg += cret['errmsg'] + "\n"
         elif CMPCNT > 0:
            CMPCNT = 0
            rstat = 'Q'
            fcount = pgrqst['fcount']
      etime = int(time.time() - etime)
   else:
      etime = 0

   cdate = PgUtil.curdate()
   ctime = PgUtil.curtime()

   if PgDBI.pgget("dsrqst", "", cnd + " AND status = 'I'"):
      rstat = 'I'
      errmsg = rstr + ": is interrupted during process\n"
   elif rstat == 'O':
      fcount = PgRqst.set_request_count(cnd, pgrqst, 1)
      pgrqst['date_ready'] = cdate
      pgrqst['time_ready'] = ctime
      pgrqst['date_purge'] = PgUtil.adddate(pgrqst['date_ready'], 0, 0, PgOPT.PGOPT['VP'])
      pgrqst['time_purge'] = pgrqst['time_ready']

   if 'NE' not in PgOPT.params and 'IQ'.find(rstat) < 0:
      if 'NO' not in PgOPT.params or errmsg:
         rstat = PgOPT.send_request_email_notice(pgrqst, errmsg, fcount, rstat, (PgOPT.PGOPT['ready'] if pgrqst['location'] else ""))
   elif errmsg:
      PgLOG.pglog(errmsg, PgOPT.PGOPT['errlog'])

   # set status and date/time
   record = {'status' : rstat, 'pid' : 0}
   if etime: record['exectime'] = etime + pgrqst['exectime']
   if rstat == 'O':
      record['date_ready'] = pgrqst['date_ready']
      record['time_ready'] = pgrqst['time_ready']
      if 'NO' in PgOPT.params:
         record['status'] = 'N'
         record['date_purge'] = record['time_purge'] = None
      else:
         record['date_purge'] = pgrqst['date_purge']
         record['time_purge'] = pgrqst['time_purge']
   else:
      ERRMSG += errmsg
      record['ecount'] = pgrqst['ecount'] + 1

   if PgDBI.pgupdt("dsrqst", record, cnd, PgOPT.PGOPT['extlog']) and rstat == 'O':
      if fcount > 0:
         rstr += " built successfully" 
         if 'NO' in PgOPT.params:
            rstr += ", but data not online,"
         else:
            purge_one_request(ridx, cdate, ctime, -1)
         rstr += " by " + PgLOG.PGLOG['CURUID']
         if PgLOG.PGLOG['CURUID'] != PgOPT.params['LN']: rstr += " for " + PgOPT.params['LN']
      else:
         rstr += " processed with No data by " + PgLOG.PGLOG['CURUID']
         if PgLOG.PGLOG['CURUID'] != PgOPT.params['LN']: rstr += " for " + PgOPT.params['LN']

      PgLOG.pglog("{} at {}".format(rstr, PgUtil.curtime(1)), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
      return 1
   else:
      return 0

#
# process one request partition
#
def process_one_partition(pidx, cnd, pgpart, ridx, pgrqst):

   global ERRMSG
   ret = 0
   rstat = pgpart['status']
   rtype = pgrqst['rqsttype']
   rstr = "RPT{}-RQST{}-{}".format(pidx, ridx, pgpart['dsid'])
   rcnd = "rindex = {}".format(ridx)
   if pgpart['specialist'] != PgOPT.params['LN']:
      return partition_error(pidx, "{}: Must be '{}' to process {}".format(PgOPT.params['LN'], pgrqst['specialist'], rstr))

   if rstat != 'Q':
      if ('PS' in PgOPT.params and PgOPT.params['PS'][0] == 'Q' and
          PgDBI.pgexec("UPDATE ptrqst set status = 'Q' WHERE " + cnd, PgOPT.PGOPT['extlog'])):
         rstat = pgpart['status'] = 'Q'
      else:
         return partition_error(pidx, "{}: Status '{}', must be 'Q' to process".format(rstr, rstat))

   etime = time.time()
   pgcntl = PgOPT.PGOPT['RCNTL']
   cmd = pgcntl['command']
   fcount = pgpart['fcount']
   errmsg = ""
   rstat = "O"
   if pgrqst['location']:
      PgLOG.PGLOG['FILEMODE'] = 0o666
      PgLOG.PGLOG['EXECMODE'] = 0o777

   if rtype == "F" or rtype == "A":
      (rstat, errmsg) = stage_convert_files(ridx, cnd, rstr, pgrqst, errmsg, cmd, rtype)
      cmd = ""   # do not call command any more

   if rstat == 'O' and cmd:
      cret = call_command(ridx, cnd, cmd, rstr, pgrqst, pidx, pgpart)
      if 'pgrqst' in cret: pgrqst = cret['pgrqst']
      if 'pgpart' in cret: pgpart = cret['pgpart']
      if 'errmsg' in cret:
         rstat = 'E'
         if cret['errmsg']: errmsg += cret['errmsg'] + "\n"

   etime = int(time.time() - etime)

   if PgDBI.pgget("ptrqst", "", cnd + " AND status = 'I'"):
      rstat = 'I'
      errmsg = rstr + ": is interrupted during process\n"
   if errmsg:
      if not ('NE' in PgOPT.params or rstat == "I"):
         PgOPT.send_request_email_notice(pgrqst, errmsg, fcount, rstat, '', pgpart)
      else:
         if errmsg: PgLOG.pglog(errmsg, PgOPT.PGOPT['errlog'])

   # set status and date/time
   record = {}
   record['status'] = rstat
   if etime:
      record['exectime'] = etime + pgpart['exectime']
      PgDBI.pgexec("UPDATE dsrqst SET exectime = exectime + {} WHERE {}".format(etime, rcnd), PgOPT.PGOPT['extlog'])
   if rstat != 'O': ERRMSG += errmsg

   if PgDBI.pgupdt("ptrqst", record, cnd, PgOPT.PGOPT['extlog']):
      if PgLock.lock_partition(pidx, 0, PgOPT.PGOPT['extlog']) > 0 and rstat == 'O':
         rstr += " built Successfully by {}".format(PgLOG.PGLOG['CURUID'])
         if PgLOG.PGLOG['CURUID'] != PgOPT.params['LN']: rstr += " for {}".format(PgOPT.params['LN'])
         PgLOG.pglog("{} at {}".format(rstr, PgUtil.curtime(1)), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
         ret = 1

      ecnt = 0
      qcnt = PgDBI.pgget('ptrqst', '', rcnd + " AND status = 'Q'", PgOPT.PGOPT['extlog'])
      if rstat == 'E':
         PgDBI.pgexec("UPDATE dsrqst SET ecount = ecount + 1 WHERE " + rcnd, PgOPT.PGOPT['extlog'])
         if not qcnt: ecnt = 1
      elif not qcnt:
         ecnt = PgDBI.pgget('ptrqst', '', rcnd + " AND status = 'E'", PgOPT.PGOPT['extlog'])
      if ecnt and PgDBI.pgget('dsrqst', '', rcnd + " AND status = 'Q'", PgOPT.PGOPT['extlog']):
         PgDBI.pgexec("UPDATE dsrqst SET status = 'E' WHERE " + rcnd, PgOPT.PGOPT['extlog'])
         PgLOG.pglog("RQST{}: SET Request Status Q to E for Failed Partition process".format(ridx), PgOPT.PGOPT['wrnlog'])
         ret = 0

   return ret

#
# convert file formats and stage online for download
#
def stage_convert_files(ridx, cnd, rstr, pgrqst, errmsg, cmd, rtype):

   cnts = {}
   if rtype == 'F':
      if not pgrqst['data_format']:  # should not happen normally
         errmsg += rstr + ": Miss conversion Data Format to build"
         return ("E", errmsg)
   else:
      if not pgrqst['file_format']:  # should not happen normally
         errmsg += rstr + ": Miss conversion Archive Format to build"
         return ("E", errmsg)

   # gather all file records for the request
   pgfiles = PgDBI.pgmget("wfrqst", "*", cnd, PgOPT.PGOPT['extlog'])
   cnts['F'] = len(pgfiles['wfile']) if pgfiles else 0
   if cnts['F'] == 0:  # should not happen normally
      errmsg += rstr + ": No file to build"
      return ("E", errmsg)

   s = "s" if cnts['F'] > 1 else ""
   PgLOG.pglog("Convert {} file{} for {} ...".format(cnts['F'], s, rstr), PgLOG.WARNLG)
   cnts['P'] = cnts['O'] = cnts['E'] = emlcnt = 0
   PgFile.change_local_directory(PgRqst.get_file_path(None, "data/" + pgrqst['dsid'], None, 1), PgOPT.PGOPT['extlog']|PgLOG.FRCLOG)
   ecnt = (cnts['F'] if cnts['F'] > 10 else (cnts['F']+1))
   efiles = [1]*ecnt
   if PgLOG.PGLOG['DSCHECK']:
      PgCMD.set_dscheck_fcount(cnts['F'], PgOPT.PGOPT['errlog'])
      PgCMD.set_dscheck_dcount(0, 0, PgOPT.PGOPT['errlog'])

   while True:
      for i in range(cnts['F']):
         if emlcnt == EMLMAX:  # skip for too many errors
            errmsg += "\n..."
            emlcnt += 1
         if not efiles[i]: continue
         fstat = 'O'
         pgrec = PgUtil.onerecord(pgfiles, i)
         wfile = pgrec['wfile']
         pstat = PgRqst.check_processed(wfile, pgrec, pgrqst['dsid'], ridx, rstr)
         if pstat > 0:
            PgLOG.pglog("{}-{}: converted already".format(pgrec['wfile'], rstr), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
            doconv = 0
         elif pstat < 0:
            cnts['P'] += 1
            continue
         else:
            if rtype == 'F':
               (wfile, msg) = PgRqst.convert_data_format(pgrec, pgrqst, cmd, rstr)
            else:
               (wfile, msg) = PgRqst.convert_archive_format(pgrec, pgrqst, cmd, rstr)
            if msg:
               if emlcnt < EMLMAX or (i+1) == cnts['F']:  errmsg += msg
               emlcnt += 1
               cnts['E'] += 1
               fstat = 'E'
            elif wfile is None:
               PgDBI.pgexec("UPDATE wfrqst SET pid = 0 WHERE findex = {}".format(pgrec['findex']), PgOPT.PGOPT['extlog'])
               cnts['P'] += 1
               continue

         msg = set_file_record(wfile, fstat, pgrec, pgfiles, cnts, i, pgrqst, pgrec['srctype'], rstr)
         if msg:
            if emlcnt < EMLMAX or (i+1) == cnts['F']:  errmsg += msg
            emlcnt += 1
         elif fstat == 'O':
            efiles[i] = 0
            cnts['O'] += 1
            if PgLOG.PGLOG['DSCHECK']:
               PgCMD.add_dscheck_dcount(1, pgfiles['size'][i], PgOPT.PGOPT['errlog'])

      if cnts['P'] == 0 and (cnts['E'] == 0 or cnts['E'] >= ecnt): break
      ecnt = cnts['E'] + cnts['P']
      errmsg += PgLOG.pglog("{}: Reconvert {}/{} file{} in {} seconds".format(rstr, ecnt, cnts['F'], s, PgSIG.PGSIG['ETIME']), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG|PgLOG.RETMSG)
      cnts['P'] = cnts['E'] = 0
      time.sleep(PgSIG.PGSIG['ETIME'])

   PgLOG.pglog("{}/{} of {} file{} staged Online/Error for {}".format(cnts['O'], cnts['E'], cnts['F'], s, rstr), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
   if cnts['E'] > 0:
      errmsg += PgLOG.pglog("{}/{} file{} failed conversion for {}".format(cnts['E'], cnts['F'], s, rstr), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
      return ("E", errmsg)
   else:
      return ("O", '')

#
# cp local files online, fatal if error
#
def stage_local_files(ridx, cnd, rstr, pgrqst):

   global ALLCNT

   lcnt = len(PgOPT.params['LF']) if 'LF' in PgOPT.params else 0
   if lcnt == 0: return

   rdir = PgRqst.get_file_path(None, pgrqst['rqstid'], pgrqst['location'], 1)
   PgFile.make_local_directory(rdir, PgOPT.PGOPT['extlog'])

   # check and set web formats, group ids
   if 'DF' not in PgOPT.params: PgOPT.params['DF'] = [pgrqst['data_format']]
   if 'OT' not in PgOPT.params: PgOPT.params['OT'] = ['C']
   if 'AF' not in PgOPT.params:
      if pgrqst['file_format']:
         if 'AF' not in PgOPT.params: PgOPT.params['AF'][0] = pgrqst['file_format']
      else:
         PgOPT.set_file_format(lcnt)
   PgOPT.params['FD'] = [None]*lcnt
   PgOPT.params['FT'] = [None]*lcnt
   PgOPT.params['FS'] = ['O']*lcnt
   PgOPT.params['SZ'] = PgFile.local_file_sizes(PgOPT.params['LF'])
   if 'WF' not in PgOPT.params:
      PgOPT.params['WF'] = [None]*lcnt
      for i in range(lcnt):
         PgOPT.params['WF'][i] = op.basename(PgOPT.params['LF'][i])

   s = "s" if (lcnt > 1) else ""
   PgLOG.pglog("Stage {} local file{} online for {} ...".format(lcnt, s, rstr), PgLOG.WARNLG)
   PgFile.check_local_writable(PgOPT.params['WH'], "Stage Requested Data", PgOPT.PGOPT['extlog'])
   cnd = "rindex = {} AND wfile = ".format(pgrqst['rindex'])
   scnt = 0
   for i in range(lcnt):
      file = PgLOG.join_paths(rdir, PgOPT.params['WF'][i])
      info = PgFile.check_local_file(file, 1, PgOPT.PGOPT['wrnlog'])
      if info:
         linfo = PgFile.check_local_file(PgOPT.params['LF'][i], 0, PgOPT.PGOPT['wrnlog'])
         if not linfo:
            PgLOG.pglog(PgOPT.params['LF'][i] + ": Local file not exists", PgOPT.PGOPT['extlog'])
         elif info['data_size'] == linfo['data_size']:
            PgLOG.pglog("web:{} STAGED already at {}:{}".format(file, info['date_modified'], info['time_modified']), PgLOG.WARNLG)
            PgOPT.params['FD'][i] = info['date_modified']
            PgOPT.params['FT'][i] = info['time_modified']
            continue

      if PgFile.local_copy_local(file, PgOPT.params['LF'][i], PgOPT.PGOPT['wrnlog']):
         info = PgFile.check_local_file(file, 1, PgOPT.PGOPT['wrnlog'])
         if info:
            PgOPT.params['FD'][i] = info['date_modified']
            PgOPT.params['FT'][i] = info['time_modified']
            scnt += 1

   PgLOG.pglog("{} of {} file{} staged Online for {}".format(scnt, lcnt, s, rstr), PgOPT.PGOPT['wrnlog'])
   
   scnt = ALLCNT
   ALLCNT = lcnt
   set_web_files(ridx)
   ALLCNT = scnt

#
# create a working data storage directory for a given request record
#
def create_request_directory(pgrqst):
   
   rdir = PgRqst.get_file_path(None, pgrqst['rqstid'], pgrqst['location'], 1)
   PgFile.make_local_directory(rdir, PgOPT.PGOPT['extlog'])

   if pgrqst['tarflag'] == 'Y':
      PgFile.make_local_directory("{}/{}".format(rdir, PgOPT.PGOPT['TARPATH']), PgOPT.PGOPT['extlog'])
   
#
#  call a command to build a customized request, such as subsetting 
#
def call_command(ridx, cnd, cmd, rstr, pgrqst, pidx, pgpart):

#   global CMPCNT
   rdir = PgRqst.get_file_path(None, pgrqst['rqstid'], pgrqst['location'], 1)
   cret = {}   # a dict to hold return info for this command call
   callcmd = 1
   fields = ("findex, wfile, gindex, tindex, type, srctype, size, date, time, " +
             "status, command, disp_order, data_format, file_format, ofile, checksum")

   PgFile.change_local_directory(rdir, PgOPT.PGOPT['extlog']|PgLOG.FRCLOG)
   if pgrqst['location'] and op.isfile(PgOPT.PGOPT['ready']): PgFile.delete_local_file(PgOPT.PGOPT['ready'])
   cmddump = ''
   pgcntl = PgOPT.PGOPT['RCNTL']
   empty_out = 1 if (pgcntl['empty_out'] == 'Y') else 0
   if pidx and 'BP'.find(pgcntl['ptflag']) < 0:
      callcmd = 0
   elif (pgcntl['ptlimit'] or pgcntl['ptsize']) and pgcntl['ptflag'] == 'N':
      callcmd = 0
   elif not pidx and pgrqst['ptcount'] < 0:
      callcmd = 0

   if callcmd:
#      cmdopt = 305 if empty_out else 49   # 49=1+16+32; 305=49+256
      cmdopt = 305
      for loop in range(3):
         cmddump = PgLOG.pgsystem(cmd, PgOPT.PGOPT['wrnlog'], cmdopt)
         if loop < 2 and PgLOG.PGLOG['SYSERR'] and 'Connection timed out' in PgLOG.PGLOG['SYSERR']:
            time.sleep(PgSIG.PGSIG['ETIME'])
         else:
            break
      cmddump = "\nCommand dump for {}:\n{}".format(cmd, cmddump) if cmddump else ""
      if empty_out and PgLOG.PGLOG['SYSERR']: empty_out = check_empty_error(PgLOG.PGLOG['SYSERR'])
      if pidx:
         pgrec = PgDBI.pgget("ptrqst", "*", cnd, PgOPT.PGOPT['extlog'])
         if not pgrec:
            cret['errmsg'] = "{}: Error reget partition record{}".format(rstr, cmddump)
            return cret
         cret['pgpart'] = pgpart = pgrec   # partition record refreshed
         if pgrec['status'] not in 'OQ':
            cret['errmsg'] = "{}: status Q changed to {}{}".format(rstr, pgrec['status'], cmddump)
            return cret
      else:
         pgrec = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
         if not pgrec:
            cret['errmsg'] = "{}: Error reget request record{}".format(rstr, cmddump)
            return cret
         cret['pgrqst'] = pgrqst = pgrec   # request record refreshed
         if pgrec['status'] not in 'OQ':
            cret['errmsg'] = "{}: status Q changed to {}{}".format(rstr, pgrec['status'], cmddump)
            return cret

   pgrecs = PgDBI.pgmget("wfrqst", fields, cnd + " ORDER BY wfile", PgOPT.PGOPT['extlog'])
   fcnt = len(pgrecs['findex']) if pgrecs else 0
   if pidx or not callcmd:
      cnt = 0
   else:
      finfo = PgFile.local_glob("*", 256)
      if finfo:
         cnt = len(finfo)
         wfiles = list(finfo.keys())
      else:
         cnt = 0
   if fcnt == 0 and cnt == 0:
      if not empty_out: cret['errmsg'] = "{}: No Data File info Found{}".format(rstr, cmddump)
      return cret

   if cnt > fcnt:
      pgrecs = None
   else:
      cnt = fcnt
      wfiles = []
   fcnd = cnd + " AND wfile ="

   if not pidx and callcmd:
      fcnt = pgrqst['fcount'] if pgrqst['fcount'] else 0
      if fcnt:
         if fcnt < 0:
            cret['errmsg'] = "{}: invalid file count {} Found{}".format(rstr, fcnt, cmddump)
            return cret
         elif cnt < fcnt:
            cret['errmsg'] = "{}: File counts miss-match: GOT {} while NEED {}{}".format(rstr, cnt, fcnt, cmddump)
            return cret

      if pgrqst['ptcount'] == 1:
         # set partition count to -1 for having called command already
         pgrqst['ptcount'] = -1
         PgDBI.pgexec("UPDATE dsrqst SET ptcount = -1 WHERE " + cnd, PgOPT.PGOPT['extlog'])

   if 'NO' in PgOPT.params: return cret

   lastcmd = 0 if pidx and 'FB'.find(pgcntl['ptflag']) > -1 else 1
   rfmt = tinfo = None
   if lastcmd:
      rfmt = pgrqst['file_format']
#
#      if pgrecs and rfmt and callcmd and not pidx and pgrqst['ptcount'] < 2 and fcnt > 2*CMPLMT:
#         wfile = None
#         for i in range(fcnt):
#            pgrec = PgUtil.onerecord(pgrecs, i)
#            if pgrec['type'] != 'D': continue
#            cfile = wfile = pgrec['wfile']
#            ffmt = pgrec['file_format']
#            break
#
#         if wfile:
#            afmt = PgRqst.valid_archive_format(rfmt, ffmt)
#            if afmt: (cfile, tmpfmt) = PgFile.compress_local_file(wfile, afmt, 3)
#            if cfile != wfile:
#               CMPCNT = add_one_request_partitions(ridx, cnd, pgrqst, 1)
#               if CMPCNT > 2 and PgDBI.pgexec("UPDATE dsrqst SET fcount = {}".format(fcnt), PgOPT.PGOPT['extlog']):
#                  pgrqst['fcount'] = fcnt
#                  return cret
#               CMPCNT = 0
#
      if pgrqst['tarflag'] == 'Y': tinfo = init_tarinfo(rstr, ridx, pidx, pgrqst)

   size = progress = 0
   if PgLOG.PGLOG['DSCHECK'] and pidx and not callcmd:
      progress = int(cnt/50)
      if progress == 0: progress = 1
      PgCMD.set_dscheck_fcount(cnt, PgOPT.PGOPT['errlog'])
      PgCMD.set_dscheck_dcount(0, 0, PgOPT.PGOPT['errlog'])

   errcnt = (cnt if cnt > 10 else (cnt + 1))
   efiles = [1]*errcnt
   s = "s" if cnt > 1 else ""
   ddcnt = dfcnt = acnt = mcnt = zcnt = emlcnt = 0
   zfmt = errmsg = ''
   chkopt = 39   # 1+2+4+32
   while True:
      dindices = []
      miscnt = ecnt = 0
      cfrec = None
      for i in range(cnt):
         if emlcnt == EMLMAX:  # skip for too many errors
            errmsg += "\n..."
            emlcnt += 1
         if not efiles[i]: continue
         if i and progress and (i%progress) == 0:
            PgCMD.set_dscheck_dcount(i, size, PgOPT.PGOPT['extlog'])

         if pgrecs:
            pgrec = PgUtil.onerecord(pgrecs, i)
            wfile = pgrec['wfile']
         else:
            wfile = wfiles[i]
            if re.search(r'index\d*\.html',  wfile) or re.match(r'^core\.\d+$', wfile):
               efiles[i] = 0
               continue
            pgrec = PgDBI.pgget("wfrqst", fields, "{} '{}'".format(fcnd, wfile), PgOPT.PGOPT['extlog'])

         if cfrec:
            cfile = cfrec['wfile']
            cfrec = None
            if cfile == wfile:
               efiles[i] = 0
               continue

         cfile = wfile
         fidx = dtype = ostat = 0
         afmt = fcmd = ffmt = None
         if pgrec:
            if pgrec['status'] == 'O' and pgrec['date'] and pgrec['size']: ostat = 1
            fidx = pgrec['findex']
            fcmd = pgrec['command']
            if pgrec['type'] == 'D': dtype = 1
            if lastcmd:
               ffmt = pgrec['file_format']
               if ostat and tinfo and pgrec['tindex'] > 0:
                  msg = build_tarfile(tinfo, fidx, wfile, pgrec['size'], ffmt, pgrec['tindex'])
                  if msg:
                     if emlcnt < EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                     emlcnt += 1
                     ecnt += 1
                     continue
                  efiles[i] = 0
                  continue   # included in a tar file already
            elif ostat and callcmd:
               efiles[i] = 0
               continue

         if dtype and (rfmt or ffmt):
            afmt = PgRqst.valid_archive_format(rfmt, ffmt)
            if afmt:
               (cfile, tmpfmt) = PgFile.compress_local_file(wfile, afmt, 3)
               if cfile == wfile: afmt = None

         if callcmd and ostat and not afmt:
            if tinfo and dtype:
               msg = build_tarfile(tinfo, fidx, wfile, pgrec['size'], ffmt)
               if msg:
                  if emlcnt < EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                  emlcnt += 1
                  ecnt += 1
                  continue
            efiles[i] = 0
            continue   # file is built via call command and no check online

         finfo = PgFile.check_local_file(wfile, chkopt)
         if finfo:
            if ostat and not afmt and finfo['data_size'] == pgrec['size']:
               if tinfo and dtype:
                  msg = build_tarfile(tinfo, fidx, wfile, pgrec['size'], ffmt)
                  if msg:
                     if emlcnt < EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                     emlcnt += 1
                     ecnt += 1
                     continue
               efiles[i] = 0
               continue   # file is built and online already
         if afmt:
            cinfo = PgFile.check_local_file(cfile, chkopt)
            cfrec = PgDBI.pgget("wfrqst", fields, "{} '{}'".format(fcnd, cfile), PgOPT.PGOPT['extlog'])
            if cinfo and cfrec and cfrec['status'] == 'O' and cfrec['size'] == cinfo['data_size']:
               # file compressed already use this one
               if finfo and PgFile.delete_local_file(wfile): ddcnt += 1
               if fidx and PgDBI.pgdel('wfrqst', "findex = {}".format(fidx)): dfcnt += 1 
               if tinfo:
                  msg = build_tarfile(tinfo, cfrec['findex'], cfile, cfrec['size'], ffmt)
                  if msg:
                     if emlcnt < EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                     emlcnt += 1
                     ecnt += 1
                     continue
               efiles[i] = 0
               continue
            elif pgrec and (finfo or fcmd):
               if cinfo and PgFile.delete_local_file(cfile): ddcnt += 1
               if cfrec and PgDBI.pgdel('wfrqst', "findex = {}".format(cfrec['findex'])): dfcnt += 1
            cinfo = cfrec = None

         empty_file = empty_out
         if fcmd and not (ostat and finfo and finfo['data_size']):
            # build file if not (exist and O-status)
            fcmd = get_file_command(fcmd, pgrec)
            cmdopt = 304 if empty_file else 48   # 48=16+32; 304=48+256
            cmddump = PgLOG.pgsystem(fcmd, PgOPT.PGOPT['wrnlog'], cmdopt)
            cmddump = "\nCommand dump for {}:\n{}".format(fcmd, cmddump) if cmddump else ""
            if empty_file and PgLOG.PGLOG['SYSERR']: empty_file = check_empty_error(PgLOG.PGLOG['SYSERR'])
            pgrec = PgDBI.pgget("wfrqst", fields, "findex = {}".format(fidx), PgOPT.PGOPT['extlog'])
            if not pgrec:
               cret['errmsg'] = "{}-{}({}): file record removed by {}".format(rstr, wfile, fidx, PgLOG.break_long_string(fcmd, 80, "...", 1))
               return cret
            cfile = wfile = pgrec['wfile']
            ffmt = afmt = None
            if lastcmd: ffmt = pgrec['file_format']
            dtype = 1 if pgrec['type'] == 'D' else 0
            if dtype and (rfmt or ffmt):
               afmt = PgRqst.valid_archive_format(rfmt, ffmt)
               if afmt:
                  if afmt: (cfile, tmpfmt) = PgFile.compress_local_file(wfile, afmt, 3)
                  if cfile == wfile: afmt = None
            finfo = PgFile.check_local_file(wfile, chkopt)

         if not finfo:
            if finfo != None:
               if emlcnt < EMLMAX or (i+1) == cnt:
                  errmsg += "\n{}-{}: Error check file under {}".format(rstr, wfile, rdir)
               emlcnt += 1
            elif empty_file:
               if fidx: dindices.append(fidx)
               miscnt += 1
            else:
               if emlcnt < EMLMAX or (i+1) == cnt:
                  errmsg += "\n{}-{}: File not exists under {}{}".format(rstr, wfile, rdir, cmddump)
               emlcnt += 1
               miscnt += 1
            ecnt += 1
            continue
         elif finfo['data_size'] == 0:
            PgFile.delete_local_file(wfile, PgOPT.PGOPT['extlog'])
            if empty_file:
               if fidx: dindices.append(fidx)
            else:
               if emlcnt < EMLMAX or (i+1) == cnt:
                  errmsg += "\n{}-{}: File is empty under {}{}".format(rstr, wfile, rdir, cmddump)
               emlcnt += 1
            miscnt += 1
            ecnt += 1
            continue

         if afmt:
            if PgLOG.pgsystem("rdazip -f {} {}".format(afmt, wfile), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG, 257):  # 257=1+256
               cinfo = PgFile.check_local_file(cfile, chkopt)
               if cinfo:
                  wfile = cfile
                  finfo = cinfo
                  zfmt = afmt
                  zcnt += 1
               elif finfo != None:
                  if emlcnt < EMLMAX or (i+1) == cnt:
                     errmsg += "\n{}-{}: Error check file under {}".format(rstr, cfile, rdir)
                  emlcnt += 1
                  ecnt += 1
                  continue
            else:
               if emlcnt < EMLMAX or (i+1) == cnt:
                  errmsg += "\n{}-{}: {}".format(rstr, cfile, (PgLOG.PGLOG['SYSERR'] if PgLOG.PGLOG['SYSERR'] else "Error rdazip " + wfile))
               emlcnt += 1
               ecnt += 1
               continue

         if progress: size += finfo['data_size']
         # record request file info
         PgFile.set_local_mode(wfile, 1, PgLOG.PGLOG['FILEMODE'], finfo['mode'], finfo['logname'])
         record = get_file_record(pgrec, finfo, pgrqst, wfile, i, "W")
         if record:
            if fidx:
               mcnt += PgDBI.pgupdt("wfrqst", record, "findex = {}".format(fidx), PgOPT.PGOPT['extlog'])
            else:
               fidx = PgDBI.pgadd("wfrqst", record, PgLOG.AUTOID|PgOPT.PGOPT['extlog'])
               if fidx: acnt += 1

         efiles[i] = 0
         if tinfo and dtype:
            msg = build_tarfile(tinfo, fidx, wfile, finfo['data_size'], ffmt)
            if msg:
               if emlcnt < EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
               emlcnt += 1
               ecnt += 1
               continue

      if ecnt == 0 or ecnt >= errcnt or tinfo: break
      errmsg += "\n" + PgLOG.pglog(("{}: {} ".format(rstr, ("Recheck" if callcmd else "Reprocess")) +
                                    "{}/{} file{} in {} seconds".format(ecnt, cnt, s, PgSIG.PGSIG['ETIME'])),
                                   PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG|PgLOG.RETMSG)
      errcnt = ecnt
      time.sleep(PgSIG.PGSIG['ETIME'])

   if zcnt > 0:
      s = "s" if zcnt > 1 else ""
      PgLOG.pglog("{} file{} {} compressed for {}".format(zcnt, s, zfmt, rstr), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
   if ecnt > 0:
      errmsg += "\n" + PgLOG.pglog("{}/{} files failed for {}".format(ecnt, cnt, rstr), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
      if not (empty_out and miscnt == ecnt):
         cret['errmsg'] = errmsg
         return cret
      dcnt = 0
      for didx in dindices:
         dcnt += PgDBI.pgdel("wfrqst", "findex = {}".format(didx), PgOPT.PGOPT['extlog'])
      if dcnt > 0:
         s = "s" if dcnt > 1 else ""
         PgLOG.pglog("{} empty file record{} removed for {}".format(dcnt, s, rstr), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

   if (ddcnt+dfcnt) > 0:
      s = "s" if (ddcnt+dfcnt) > 1 else ""
      PgLOG.pglog("{}/{} File/Record duplication{} removed for {}".format(ddcnt, dfcnt, s, rstr), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

   s = "s" if cnt > 1 else ""
   PgLOG.pglog("{}/{} of {} file record{} Added/Modified for {}".format(acnt, mcnt, cnt, s, rstr), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

   if tinfo:
      msg = build_tarfile(tinfo)
      if msg:
         cret['errmsg'] = "{}\n{}".format(errmsg, msg)
         return cret
      if pidx:
         if tinfo['tcnt'] != pgpart['tarcount']:
            PgDBI.pgexec("UPDATE ptrqst SET tarcount = {} WHERE {}".format(tinfo['tcnt'], cnd), PgOPT.PGOPT['extlog'])
            pgpart['tarcount'] = tinfo['tcnt']
      elif tinfo['tcnt'] != pgrqst['tarcount']:
         PgDBI.pgexec("UPDATE dsrqst SET tarcount = {} WHERE {}".format(tinfo['tcnt'], cnd), PgOPT.PGOPT['extlog'])
         pgrqst['tarcount'] = tinfo['tcnt']

   if pidx:   # check and fix partition file count
      fcnt = PgDBI.pgget("wfrqst", "", cnd, PgOPT.PGOPT['extlog'])
      if fcnt != pgpart['fcount']:
         PgDBI.pgexec("UPDATE ptrqst set fcount = {} WHERE {}".format(fcnt, cnd),  PgOPT.PGOPT['extlog'])
         pgpart['fcount'] = fcnt

   if progress: PgCMD.set_dscheck_dcount(cnt, size, PgOPT.PGOPT['extlog'])

   return cret


# return 1 if error message is ok for empty output
def check_empty_error(errmsg):
   
   ret = 0
   if re.search(r'ncks: ERROR Domain .* brackets no coordinate values', errmsg): ret = 1
   
   return ret

#
# specialist specified command for each file
#
def get_file_command(cmd, pgrec):

   if cmd.find('$') > -1: cmd = PgLOG.replace_environments(cmd, None, PgOPT.PGOPT['emlerr'])

   cmd = re.sub(r'( -OF| -WF)', ' ' + pgrec['wfile'], cmd, 1)
   cmd = re.sub(r' -FI', ' {}'.format(pgrec['findex']), cmd, 1)
   if pgrec['ofile']: cmd = re.sub(r'( -IF| -RF)', ' ' + pgrec['ofile'], cmd, 1)

   return cmd

#
# intialize the tarinfo dict for tarring small files
#
def init_tarinfo(rstr, ridx, pidx, pgrqst):
   
   tinfo = {
      'afmt' : pgrqst['file_format'],
      'rstr' : rstr,
      'ridx' : ridx,
      'pidx' : pidx,
      'dfmt' : pgrqst['data_format'],
      'gidx' : pgrqst['gindex'],
      'fidxs' : [],
      'files' : [],
      'tidxs' : [],
      'fcnt' : 0,
      'tcnt' : 0,
      'tsize' : 0,
      'tfiles' : [{'ii' : 0, 'fn' : 0, 'fmt' : ''}],
      'otcnt' : 0,
      'otars' : {}
   }
   return tinfo

#
# tarring small files
#
def build_tarfile(tinfo, fidx = 0, file = None, size = 0, afmt = None, tidx = 0):

   tn = tinfo['tcnt']
   fn = tinfo['fcnt']
   if fidx:
      if size > MFSIZE: return None  # skip file too big to tar

      # add file info to tarinfo for tarring later
      tinfo['tsize'] += size
      tinfo['fidxs'].append(fidx)
      tinfo['files'].append(file)
      tinfo['tidxs'].append(tidx if tidx else 0)
      tinfo['tfiles'][tn]['fn'] += 1
      if afmt: tinfo['tfiles'][tn]['fmt'] = afmt
      tinfo['fcnt'] = fn + 1
      if tinfo['tsize'] < TFSIZE: return None  # add more files to tar

     # start a new tar file
      ti = tn + 1
      tinfo['tcnt'] = ti
      tinfo['tsize'] = 0
      tinfo['tfiles'].append({'ii' : tinfo['fcnt'], 'fn' : 0, 'fmt' : ''})
      if tn == 0: return None  # do not build tar file yet

      # ready to build previous tar file
      ti = tn - 1
   else:
      # finish all tar files
      if fn < TCOUNT: return None   # too few files to tar
      if tn > 0:
         # check if the last tar file is needed
         fn = tinfo['tfiles'][tn]['fn']
         ti = tn - 1
         if fn < TCOUNT or 10*tinfo['tsize'] < TFSIZE:
            # The last tar file is empty or too small, add files to previous tar file
            if fn > 0: tinfo['tfiles'][ti]['fn'] += fn
         else:
            # build both the previous and last tar files
            tn += 1
            tinfo['tcnt'] = tn
      else:
         # only one tar file to be added
         ti = 0
         tn = tinfo['tcnt'] = 1

   copt = 261  # 261 = 1+4+256
   elog = PgOPT.PGOPT['errlog']
   xlog = PgOPT.PGOPT['extlog']
   if not PgFile.make_local_directory(PgOPT.PGOPT['TARPATH'], elog):
      return "{}-{}: Cannot create directory for tarring files".format(tinfo['rstr'], PgOPT.PGOPT['TARPATH'])

   # add tar file(s)
   for t in range(ti, tn):
      ii = tinfo['tfiles'][t]['ii']
      fn = tinfo['tfiles'][t]['fn']
      ln = ii + fn
      afmt = tinfo['afmt'] if tinfo['afmt'] else tinfo['tfiles'][t]['fmt']
      if afmt:
         tfmt = afmt + ".TAR"
      else:
         tfmt = "TAR"
      tfile = PgFile.join_filenames(tinfo['files'][ii], tinfo['files'][ln - 1], "-", afmt, "tar")
      tarfile = PgOPT.PGOPT['TARPATH'] + tfile

      s = "s" if fn > 1 else ""
      PgLOG.pglog("{}-{}: Tarring {} file{}...".format(tinfo['rstr'], tfile, fn, s), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
      tfrec = PgDBI.pgget("tfrqst", "*", "rindex = {} AND wfile = '{}'".format(tinfo['ridx'], tfile), xlog)
      if tfrec:
         tindex = tfrec['tindex']
         isotrec = 1
      else:  # add a new tar file record
         tfrec = {}
         tfrec['wfile'] = tfile
         tfrec['rindex'] = tinfo['ridx']
         tfrec['gindex'] = tinfo['gidx']
         if tinfo['pidx']: tfrec['pindex'] = tinfo['pidx']
         tfrec['data_format'] = tinfo['dfmt']
         tfrec['file_format'] = tfmt
         tfrec['fcount'] = fn
         tfrec['disp_order'] = t
         tindex = PgDBI.pgadd("tfrqst", tfrec, xlog|PgLOG.AUTOID)
         isotrec = 0
      tarinfo = PgFile.check_local_file(tarfile)
      isotar = 1 if tarinfo else 0
      tfcnt = 0
      for m in range(ii, ln):
         tidx = tinfo['tidxs'][m]
         file = tinfo['files'][m]
         info = PgFile.check_local_file(file)

         # No further action if a file is tarred and removed
         if tidx == tindex and tarinfo and not info: continue
         if tidx and tidx != tindex:
            if tidx in tinfo['otars']:
               otarfile = tinfo['otars'][tidx]
            else:
               record = PgDBI.pgget("tfrqst", "wfile", "tindex = {}".format(tidx), xlog)
               if record:
                  otarfile = PgOPT.PGOPT['TARPATH'] + record['wfile']
                  if not PgFile.check_local_file(otarfile): otarfile = ''
               else:
                  otarfile = ''
               # save unused tar files to delete later
               tinfo['otars'][tidx] = otarfile
               tinfo['otcnt'] += 1

            if not info and otarfile:
               # try to recover missing reuqest file from old tar file
               if not PgLOG.pgsystem("tar -xvf {} {}".format(otarfile, file), elog, copt):
                  errmsg = "{}-{}: Cannot recover tar member file {}".format(tinfo['rstr'], otarfile, file)
                  if PgLOG.PGLOG['SYSERR']: errmsg += "\n" + PgLOG.PGLOG['SYSERR']
                  return errmsg
               info = PgFile.check_local_file(file)

         if info:
            errmsg = None
            if tarinfo:
               if isotar:
                  finfo = PgFile.check_tar_file(file, tarfile)
                  if finfo and finfo['data_size'] != info['data_size']:
                     # only retar a wrong size file
                     if not PgLOG.pgsystem("tar --delete -vf {} {}".format(tarfile, file), elog, copt):
                        errmsg = "Cannot delete tar"
                     elif not PgLOG.pgsystem("tar -uvf {} {}".format(tarfile, file), elog, copt):
                        errmsg = "Cannot update tar"
               elif not PgLOG.pgsystem("tar -uvf {} {}".format(tarfile, file), elog, copt):
                  errmsg = "Cannot update tar"
            else:
               if not PgLOG.pgsystem("tar -cvf {} {}".format(tarfile, file), elog, copt):
                  errmsg = "Cannot create tar file for"
               else:
                  tarinfo = PgFile.check_local_file(tarfile, 128)
            if errmsg:
               errmsg = "{}-{}: {} member file {}".format(tinfo['rstr'], tarfile, errmsg, file)
               if PgLOG.PGLOG['SYSERR']: errmsg += "\n" + PgLOG.PGLOG['SYSERR']
               return errmsg
         elif not isotar:
            return PgLOG.pglog("{}-{}: MISS requested file {} to tar".format(tinfo['rstr'], tfile, file), elog|PgLOG.RETMSG)
         

         if tidx != tindex:   # update file recrod
            findex = tinfo['fidxs'][m]
            PgDBI.pgexec("UPDATE wfrqst set tindex = {} WHERE findex = {}".format(tindex, findex), xlog)

         # only delete a file after it is tarred and its db record is updated
         if info: PgFile.delete_local_file(file, elog)
         tfcnt += 1

      if tfcnt > 0:
         # reset tar file record
         tarinfo = PgFile.check_local_file(tarfile, 1)
         record = {'size' : tarinfo['data_size'], 'date' : tarinfo['date_modified'], 'time' : tarinfo['time_modified']}
         if isotrec:
            if tfrec['pindex'] != tinfo['pidx']: record['pindex'] = tinfo['pidx']
            if tfrec['fcount'] != fn: record['fcount'] = fn
            if tfrec['data_format'] != tinfo['dfmt']: record['data_format'] = tinfo['dfmt']
            if tfrec['file_format'] != tfmt: record['file_format'] = tfmt
         PgDBI.pgupdt("tfrqst", record, "tindex = {}".format(tindex), xlog)
         if tfcnt < fn:
            PgLOG.pglog("{}-{}: Tarred {} of {} file{} to an existing {}".format(tinfo['rstr'], tfile, tfcnt, fn, s, tarfile), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

   if not fidx and tinfo['otcnt'] > 0:
      # delete unused old tar file info
      for tidx in tinfo['otars']:
         if not PgDBI.pgget("wfrqst", "", "tindex = {}".format(tidx), xlog):
            PgDBI.pgdel("tfrqst", "tindex = {}".format(tidx), xlog)
            otarfile = tinfo['otars'][tidx]
            if otarfile: PgFile.delete_local_file(otarfile, xlog)

   return None

#
# get a new request file record or with fields with changed values
#
def get_file_record(pgrec, finfo, pgrqst, wfile, i, stype):

   newrec = {}
   if pgrec:
      afmt = PgRqst.valid_archive_format(pgrqst['file_format'], pgrec['file_format'])
      if not pgrec['date'] or pgrec['date'] != finfo['date_modified']:
         newrec['date'] = finfo['date_modified']
      if not pgrec['time'] or pgrec['time'] != finfo['time_modified']:
         newrec['time'] = finfo['time_modified']
      if not pgrec['size'] or pgrec['size'] != finfo['data_size']:
         newrec['size'] = finfo['data_size']
      if not pgrec['gindex'] and pgrqst['gindex']:
         newrec['gindex'] = pgrqst['gindex']
      if not pgrec['status'] or pgrec['status'] != 'O':
         newrec['status'] = 'O'
      if not pgrec['srctype'] or pgrec['srctype'] != stype:
         newrec['srctype'] = stype
      if wfile and pgrec['wfile'] != wfile:
         newrec['wfile'] = wfile
      if 'checksum' in finfo and pgrec['checksum'] != finfo['checksum']:
         newrec['checksum'] = finfo['checksum']
      if pgrec['type'] == "D":
         if pgrqst['data_format'] and pgrec['data_format'] != pgrqst['data_format']:
            newrec['data_format'] = pgrqst['data_format']
         if afmt and pgrec['file_format'] != afmt:
            newrec['file_format'] = afmt
   else:
      newrec['rindex'] = pgrqst['rindex']
      newrec['gindex'] = pgrqst['gindex']
      newrec['srctype'] = 'W'
      newrec['size'] = finfo['data_size']
      newrec['date'] = finfo['date_modified']
      newrec['time'] = finfo['time_modified']
      if 'checksum' in finfo: newrec['checksum'] = finfo['checksum']
      newrec['status'] = "O"
      newrec['wfile'] = wfile
      if pgrqst['data_format']: newrec['data_format'] = pgrqst['data_format']
      if pgrqst['file_format']: newrec['file_format'] = pgrqst['file_format']

   return newrec

#
#  Return: a file record for update
#
def set_file_record(wfile, fstat, pgrec, pgfiles, cnts, i, pgrqst, stype, rstr):

   fmsg = "{}-{}".format(rstr, pgrec['wfile'])
   errmsg = ""

   if (fstat == 'O' and pgrec['wfile'] != wfile and not op.isfile( pgrec['wfile']) and
       not PgFile.convert_files(pgrec['wfile'], wfile)):
      fstat = 'E'
      cnts['E'] += 1
      errmsg = "{}: error convert from {}\n".format(fmsg, wfile)

   checksum = PgRqst.get_requested_checksum(pgrqst['dsid'], pgrec)
   finfo = PgFile.check_local_file(pgrec['wfile'], 1)
   if finfo:
      if finfo['data_size'] == 0:
         fstat = 'E'
         cnts['E'] += 1
         errmsg += fmsg + ": empty file\n"
      else:
         if checksum: finfo['checksum'] = checksum
         record = get_file_record(pgrec, finfo, pgrqst, None, i, stype)

   record = {'status' : fstat, 'pid' : 0}
   if PgDBI.pgupdt("wfrqst", record, "findex = {}".format(pgrec['findex']), PgOPT.PGOPT['extlog']):
      for fld in record:
         pgfiles[fld][i] = record[fld]   # record the changes
   else:
      errmsg += fmsg + ": error update wfrqst record\n"
      cnts['E'] += 1

   return errmsg

#
# check and purge the requests
#
def purge_requests():

   cdate = PgUtil.curdate()
   ctime = PgUtil.curtime()
   if ALLCNT > 0:
      rcnt = ALLCNT
      indices = PgOPT.params['RI']
   else:
      rcnd = ("specialist = '{}' AND (status = 'P' OR status = 'O') AND ".format(PgOPT.params['LN']) +
              "(date_purge < '{}' OR date_purge = '{}' AND time_purge < '{}')".format(cdate, cdate, ctime))
      pgrecs = PgDBI.pgmget("dsrqst", "rindex", rcnd, PgOPT.PGOPT['extlog'])
      rcnt = len(pgrecs['rindex']) if pgrecs else 0
      if not rcnt:
          return PgLOG.pglog("No Request owned by '{}' due to be purged by {} {}".format(PgOPT.params['LN'], cdate, ctime), PgOPT.PGOPT['wrnlog'])
      indices = pgrecs['rindex']

   s = "s" if rcnt > 1 else ""
   PgLOG.pglog("Purge {} Request{} ...".format(rcnt, s), PgLOG.WARNLG)
   dcnt = 0
   for i in range(rcnt):
      dcnt += purge_one_request(indices[i], cdate, ctime, 1)

   PgLOG.pglog("{} of {} request{} Purged by '{}' at {}".format(dcnt, rcnt , s, PgOPT.params['LN'], PgUtil.curtime(1)), PgOPT.PGOPT['wrnlog'])
   return rcnt

#
# purge one request
#
# dppurge: <=0 record purge info only, > 0 record purge info and delete request
#
def purge_one_request(ridx, cdate, ctime, dopurge = 0):

   cnd = "rindex = {}".format(ridx)
   pgrqst = PgDBI.pgget("dsrqst", "*", cnd, PgOPT.PGOPT['extlog'])
   if not pgrqst: return PgLOG.pglog("can not get Request info for " + cnd, PgOPT.PGOPT['errlog'])
   rstr = "Request {} of {}".format(ridx, pgrqst['dsid'])
   if ALLCNT > 0 and dopurge > 0:
      if pgrqst['specialist'] != PgOPT.params['LN']:
         return PgLOG.pglog("{}: Specialist '{}' to purge {}".format(PgOPT.params['LN'], pgrqst['specialist'], rstr), PgOPT.PGOPT['errlog'])
      if 'POH'.find(pgrqst['status']) < 0:
         return PgLOG.pglog("{} in Status '{}' and cannot be purged".format(rstr, pgrqst['status']), PgOPT.PGOPT['errlog'])
      elif 'FP' not in PgOPT.params: 
         pstr = ", adds Mode option -FP (-ForcePurge) to force purge"
         if pgrqst['status'] == 'O':
            pdt = '{} {}'.format(pgrqst['date_purge'], pgrqst['time_purge'])
            cdt = '{} {}'.format(cdate, ctime)
            if PgUtil.difftime(pdt, cdt) > 0:
               return PgLOG.pglog("{} is not due for purge{}".format(rstr, pstr), PgOPT.PGOPT['errlog'])
         elif pgrqst['status'] == 'H':
            return PgLOG.pglog("{} is on Hold{}".format(rstr, pstr), PgOPT.PGOPT['errlog'])

   if pgrqst['fcount'] == None: pgrqst['fcount'] = 0
   s = "s" if pgrqst['fcount'] > 1 else ""
   if dopurge > 0:
      if PgLock.lock_request(ridx, 1, PgOPT.PGOPT['extlog']) <= 0: return 0
      PgLOG.pglog("Purge {} with {} file{} ...".format(rstr, pgrqst['fcount'], s), PgLOG.WARNLG)
      PgFile.check_local_writable(PgOPT.params['WH'], "Purge Requested Data", PgOPT.PGOPT['extlog'])
   if PgOPT.request_type(pgrqst['rqsttype'], 1):
      record_purge_files(cnd)
   
   # record request info into
   pgrec = {}
   pgrec['date_purge'] = cdate
   pgrec['time_purge'] = ctime
   pgrec['size_request'] = pgrqst['size_request']
   pgrec['size_input'] = pgrqst['size_input'] if pgrqst['size_input'] else pgrqst['size_request']
   pgrec['fcount'] = pgrqst['fcount']
   if pgrqst['ptcount'] > 1: pgrec['ptcount'] = pgrqst['ptcount']
   pgrec['exectime'] = pgrqst['exectime']
   pgrec['date_ready'] = pgrqst['date_ready']
   pgrec['time_ready'] = pgrqst['time_ready']
   pgrec['rqsttype'] = pgrqst['rqsttype']
   pgrec['dsid'] = pgrqst['dsid']
   pgrec['gindex'] = pgrqst['gindex']
   pgrec['date_rqst'] = pgrqst['date_rqst'] if pgrqst['date_rqst'] else pgrqst['date_ready']
   pgrec['time_rqst'] = pgrqst['time_rqst'] if pgrqst['time_rqst'] else pgrqst['time_ready']
   pgrec['specialist'] = pgrqst['specialist']
   pgrec['email'] = pgrqst['email']
   pgrec['wuid_request'] = PgDBI.check_wuser_wuid(pgrqst['email'], pgrqst['date_rqst'])
   pgrec['fromflag'] = pgrqst['fromflag']
   if pgrqst['subflag']: pgrec['subflag'] = pgrqst['subflag']
   if pgrqst['location']: pgrec['location'] = pgrqst['location']
   if pgrqst['data_format']: pgrec['data_format'] = pgrqst['data_format']
   if pgrqst['file_format']: pgrec['file_format'] = pgrqst['file_format']
   pgrec['ip'] = pgrqst['ip']
   if pgrqst['note']: pgrec['note'] = pgrqst['note']
   if pgrqst['task_id']: pgrec['task_id'] = pgrqst['task_id']
   if pgrqst['rinfo']:
      pgrec['rinfo'] = pgrqst['rinfo']
   elif pgrqst['note']:
      pgrec['rinfo'] = pgrqst['note']
   
   if dopurge < 0: pgrec['hostname'] = PgLOG.PGLOG['HOSTNAME']
   pgrec['quarter'] = 1
   ms = re.search(r'-(\d+)-', str(pgrqst['date_rqst']))
   if ms: pgrec['quarter'] += int((int(ms.group(1)) - 1)/3)
   if PgDBI.pgget("dspurge", "", cnd, PgOPT.PGOPT['extlog']):   # update purge request record
      ret = PgDBI.pgupdt("dspurge", pgrec, cnd, PgOPT.PGOPT['extlog'])
   else:
      pgrec['rindex'] = ridx
      ret = PgDBI.pgadd("dspurge", pgrec, PgOPT.PGOPT['extlog'])

   PgRqst.fill_request_metrics(ridx, pgrec)

   if ret:
      if dopurge > 0:
         dcnt = [0]*3
         delete_one_request(ridx, dcnt)
         PgLOG.pglog("{}/{} of {} request file{} purged from RDADB/Disk".format(dcnt[1], dcnt[2], pgrqst['fcount'], s), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
         PgLOG.pglog("{} purged by {}".format(rstr, PgUtil.curtime(1)), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
      else:
         PgLOG.pglog("{} recorded into dspurge at {}".format(rstr, PgUtil.curtime(1)), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

   return ret

#
# saved the purged files in tabl wfpurge
#
def record_purge_files(cnd):

   # gather all file records for the request
   fields = "rindex, gindex, srcid, srctype, size, type, data_format, file_format, wfile"
   pgfiles = PgDBI.pgmget("wfrqst", fields, cnd, PgOPT.PGOPT['extlog'])
   fcnt = len(pgfiles['wfile']) if pgfiles else 0
   pcnt = 0
   for i  in range(fcnt):
      pgrec = PgUtil.onerecord(pgfiles, i)
      if not PgDBI.pgget("wfpurge", "", "{} AND wfile = '{}'".format(cnd, pgrec['wfile']), PgOPT.PGOPT['extlog']):
         # add purge file record only if not created yet
         pcnt += PgDBI.pgadd("wfpurge", pgrec, PgOPT.PGOPT['extlog'])

   s = "s" if fcnt > 1 else ""
   PgLOG.pglog("{} of {} request file{} recorded for usage".format(pcnt, fcnt, s), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

#
# modify purge date/time information
#
def reset_purge_time():

   tname = "dsrqst"
   hash = PgOPT.TBLHASH[tname]
   s = 's' if ALLCNT > 1 else ''
   PgLOG.pglog("Reset purge time{} for {} request{} ...".format(s, ALLCNT, s), PgLOG.WARNLG)
   PgFile.check_local_writable(PgOPT.params['WH'], "Reset Purge Time", PgOPT.PGOPT['extlog'])

   addcnt = modcnt = 0
   flds = PgOPT.get_field_keys(tname, "XY")
   PgOPT.validate_multiple_values(tname, ALLCNT, flds)

   for i in range(ALLCNT):
      ridx = PgLock.lock_request(PgOPT.params['RI'][i], 1, PgOPT.PGOPT['extlog'])
      if ridx <= 0: continue
      cnd = "rindex = {}".format(ridx)
      pgrec = PgDBI.pgget(tname, "*", cnd, PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("Error get Request for " + cnd)
      rstr = "Request {} of {}".format(ridx, pgrec['dsid'])
      if pgrec['specialist'] != PgOPT.params['LN']:
         PgLOG.pglog("{}: specialist '{}' to reset purge time for {}".format(PgOPT.params['LN'], pgrec['specialist'], rstr), PgOPT.PGOPT['errlog'])
         PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])
         continue

      rstat = pgrec['status']
      if rstat != 'O':
         PgLOG.pglog("Status '{}' of {}, status 'O' only to Reset Purge Time/Repulish filelist".format(rstat, rstr), PgOPT.PGOPT['errlog'])
         PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])
         continue

      record = PgOPT.build_record(flds, pgrec, tname, i)
      if record:
         record['pid'] = 0
         record['lockhost'] = ''
         if not (pgrec['fcount'] or 'fcount' in record): pgrec['fcount'] = PgRqst.set_request_count(cnd, pgrec, 1)
         if not (pgrec['date_ready'] or 'date_ready' in record): record['date_ready'] = PgUtil.curdate()
         if not (pgrec['time_ready'] or 'time_ready' in record): record['time_ready'] = PgUtil.curtime()
         if not (pgrec['date_purge'] or 'date_purge' in record):
            record['date_purge'] = PgUtil.adddate((record['date_ready'] if 'date_ready' in record else pgrec['date_ready']), 0, 0, PgOPT.PGOPT['VP'])
         if not (pgrec['time_purge'] or 'time_purge' in record):
            record['time_purge'] = record['time_ready'] if 'time_ready' in record else pgrec['time_ready']

         modcnt += PgDBI.pgupdt(tname, record, cnd, PgOPT.PGOPT['extlog'])
         if 'date_purge' in record: pgrec['date_purge'] = record['date_purge']
         if 'time_purge' in record: pgrec['time_purge'] = record['time_purge']
         if 'fcount' in record: pgrec['fcount'] = record['fcount']
      else:
         PgLock.lock_request(ridx, 0, PgOPT.PGOPT['extlog'])

      PgOPT.PGOPT['VP'] = PgUtil.diffdate(pgrec['date_purge'], pgrec['date_ready'])
      addcnt += 1
      if 'WE' in PgOPT.params: PgOPT.send_request_email_notice(pgrec, None, pgrec['fcount'], rstat, (PgOPT.PGOPT['ready'] if pgrec['location']  else ""))

   PgLOG.pglog("{}/{} of {} request{} modified!".format(modcnt, addcnt, ALLCNT, s), PgOPT.PGOPT['wrnlog'])

#
# get queued requests for host
# get requests with pid values on host too if not nopid
#
def get_queued_requests(host, nopid = 0):

   cnd = "specialist = '{}' AND status = 'Q' AND rqsttype <> 'C'".format(PgOPT.params['LN'])
   if nopid:
      cnd += " AND pid = 0 AND (hostname = '' OR hostname = '{}')".format(host)
   else:
      cnd += " AND (lockhost = '{}' OR hostname = '' AND pid = 0)".format(host)

   pgrecs = PgDBI.pgmget("dsrqst", "rindex, dsid, rqsttype, email, priority", cnd + " ORDER BY priority, rindex", PgOPT.PGOPT['extlog'])
   mcnt = len(pgrecs['rindex']) if pgrecs else 0
   if mcnt > 0:
      return reorder_requests(pgrecs, mcnt)
   else:
      return PgLOG.pglog("No Request Queued for '{}' on {} at {}".format(PgOPT.params['LN'], host, PgUtil.curtime(1)), PgOPT.PGOPT['wrnlog'])

#
# reorder requests in a fair order
#
def reorder_requests(pgrecs, mcnt):

   m = lcnt = ncnt = dcnt = rcnt = 0
   pgnows = pglats = pgruns = None

   while True:
      pgrec = PgUtil.onerecord(pgrecs, m)
      m += 1
      addnow = 1
      if ncnt > dcnt:
         for n in range(dcnt, ncnt):
            if pgrec['email'] == pgnows['email'][n] and pgrec['dsid'] == pgnows['dsid'][n]:
               addnow = 2
      if addnow == 1 and rcnt > 0:
         for n in range(rcnt):
            if pgrec['email'] == pgruns['email'][n] and pgrec['dsid'] == pgruns['dsid'][n]:
               addnow = 2
               break
      if addnow == 2:
         if m < mcnt:
            for n in range(m, mcnt):
               if pgrec['priority'] > pgrecs['priority'][n]: break
               if pgrec['email'] != pgrecs['email'][n] or pgrec['dsid'] != pgrecs['dsid'][n]:
                  addnow = 0
                  break
         elif lcnt > 0:
            addnow = 0

      if addnow > 0:
         pgnows = PgUtil.addrecord(pgnows, pgrec, ncnt)
         ncnt += 1
      elif addnow < 0:
         pgruns = PgUtil.addrecord(pgruns, pgrec, rcnt)
         rcnt += 1
      else:
         pglats = PgUtil.addrecord(pglats, pgrec, lcnt)
         lcnt += 1

      if m < mcnt: continue
      if lcnt == 0: break   # all done
      mcnt = lcnt
      dcnt = ncnt
      m = lcnt = rcnt = 0
      pgrecs = pglats
      pglats = pgruns = None

   return pgnows

#
# clean the data files in data/dsnnn.n dirctories that are not included in any request in RDADB
#
def clean_unused_data():

   PgFile.check_local_writable(PgOPT.params['WH'], "Delete Data Files for Requests Purged Already", PgOPT.PGOPT['extlog'])
   PgFile.change_local_directory(PgLOG.join_paths(PgOPT.params['WH'], "data"), PgOPT.PGOPT['wrnlog'])

   dsids = PgOPT.params['DS'] if 'DS' in PgOPT.params else glob.glob("ds*.*")
   if not dsids: return PgLOG.pglog("Nothing to clean", PgLOG.LOGWRN)

   dcnt = acnt = 0
   for dsid in dsids:
      fcnt = clean_dataset_data(dsid)
      if fcnt > 0:
         acnt += fcnt
         dcnt += 1

   if acnt == 0:
      PgLOG.pglog("No unused File found", PgLOG.LOGWRN)
   else:
      s = "s" if acnt > 1 else ""
      ss = "s" if dcnt > 1 else ""
      if 'FP' in PgOPT.params:
         PgLOG.pglog("{} unused File{} cleaned for {} Dataset".format(acnt, s, dcnt, ss), PgLOG.LOGWRN)
      else:
         PgLOG.pglog("{} unused File{} found for {} Dataset{}".format(acnt, s, dcnt, ss), PgLOG.LOGWRN)
         PgLOG.pglog("Add Mode option -FP to clean the data", PgLOG.WARNLG)

#
# clean unused data for one dataset
#
def clean_dataset_data(dsid):

   files = glob.glob(dsid + "/*")
   if not files: return 0
   cnt = 0
   for file in files:
      wfile = op.basename(file)
      if PgDBI.pgget("wfrqst", "", "wfile = '{}'".format(wfile), PgLOG.LGEREX): continue
      if PgDBI.pgget("wfrqst", "", "ofile = '{}'".format(wfile), PgLOG.LGEREX): continue
      if 'FP' in PgOPT.params:
         PgLOG.pgsystem("rm -rf " + file, PgLOG.LGWNEX, 5)
      else:
         PgLOG.pglog(file + " unused", PgLOG.WARNLG)
      cnt += 1

   if cnt > 0:
      s = "s" if cnt > 1 else ""
      PgLOG.pglog("{} unused File{} {} for {}".format(cnt, s, ('cleaned' if 'FP' in PgOPT.params else 'found'), dsid), PgLOG.LOGWRN)

   return cnt

#
# clean the request directories on disk that are not in RDADB
#
def clean_unused_requests():

   PgFile.check_local_writable(PgOPT.params['WH'], "Delete Directories for Requested Purged Already", PgOPT.PGOPT['extlog'])
   PgFile.change_local_directory(PgOPT.params['WH'], PgOPT.PGOPT['extlog'])

   rids = PgOPT.params['RN'] if 'RN' in PgOPT.params else glob.glob("*")
   rcnt = 0
   for rid in rids:
      ms = re.match(r'^[A-Z]+(\d+)$', rid)
      if ms:
         ridx = ms.group(1)
         if PgDBI.pgget("dsrqst", "", "rindex = {}".format(ridx), PgOPT.PGOPT['extlog']): continue
         if 'FP' in PgOPT.params:
            PgLOG.pgsystem("rm -rf " + rid, PgOPT.PGOPT['extlog'], 5)
         else:
            PgLOG.pglog(rid + " unused", PgLOG.WARNLG)
         rcnt += 1

   s = "ies" if rcnt > 1 else "y"
   if 'FP' in PgOPT.params:
      PgLOG.pglog("{} unused Request Director{} cleaned".format(rcnt, s), PgLOG.LOGWRN)
   else:
      PgLOG.pglog("{} unused Request Director{} found{}".format(rcnt, s, ("; add Mode option -FP to clean" if rcnt > 0 else "")), PgLOG.WARNLG)

#
# reset request file status for files are not on disk
#
def reset_all_file_status():

   pgrecs = PgDBI.pgmget("dsrqst", 'rindex, dsid, rqstid, rqsttype', "status = 'E' AND pid = 0", PgOPT.PGOPT['extlog'])
   cnt = len(pgrecs['rindex']) if pgrecs else 0
   if not cnt: return

   PgFile.check_local_accessible(PgOPT.params['WH'], "Reset Request File Status for Files Not Staged", PgOPT.PGOPT['extlog'])
   PgFile.change_local_directory(PgOPT.params['WH'], PgOPT.PGOPT['extlog'])
   rcnt = mcnt = 0
   for i in range(cnt):
      pgrqst = PgUtil.onerecord(pgrecs, i)
      ridx = pgrqst['rindex']
      pgfiles = PgDBI.pgmget("wfrqst", 'findex, wfile, size', "rindex = {} AND status = 'O'".format(ridx), PgOPT.PGOPT['extlog'])
      if not pgfiles: continue
      rcnt += 1
      dpath = "data/" + pgrqst['dsid'] if PgOPT.request_type(pgrqst['rqsttype'], 1) else pgrqst['rqstid']
      mcnt += reset_request_file_status(ridx, dpath, pgrqst['dsid'], pgfiles)

   if mcnt == 0:
      PgLOG.pglog("No file record needs to set status to 'R' from 'O'", PgLOG.LOGWRN)
   elif rcnt > 1 and mcnt > 1 and PgOPT.params['FP']:
      PgLOG.pglog("Total {} request file records set status to 'R' from 'O'".format(mcnt), PgLOG.LOGWRN)

#
# reset the status for all provided request files
#
def reset_request_file_status(ridx, dpath, dsid, pgrecs):

   rstr = "{}-RQST{}".format(dsid, ridx)
   cnt = len(pgrecs['findex'])
   if PgFile.check_local_file(dpath):
      s = 's' if cnt > 1 else ''
      PgLOG.pglog("{}: checking {} online file record{}...".format(rstr, cnt, s), PgLOG.WARNLG)
      mcnt = 0
      for i in range(cnt):
         pgrec = PgUtil.onerecord(pgrecs, i)
         file = PgRqst.get_file_path(pgrec['wfile'], dpath)
         info = PgFile.check_local_file(file, 1, PgOPT.PGOPT['wrnlog'])
         if not (info and info['data_size'] == pgrec['size']):
            if 'FP' in PgOPT.params:
               mcnt += PgDBI.pgexec("UPDATE wfrqst SET status = 'R' WHERE findex = {}".format(pgrec['findex']), PgOPT.PGOPT['extlog'])
            else:
               mcnt += 1
   elif 'FP' in PgOPT.params:
      mcnt += PgDBI.pgexec("UPDATE wfrqst SET status = 'R' WHERE rindex = {} AND status ='O'".format(ridx), PgOPT.PGOPT['extlog'])
   else:
      mcnt = cnt

   if mcnt > 0:
      s = 's' if mcnt > 1 else ''
      if 'FP' in PgOPT.params:
         PgLOG.pglog("{}: set {} file record{} to status 'R'".format(rstr, mcnt, s), PgLOG.LOGWRN)
      else:
         PgLOG.pglog("{}: add Mode option -FP to set {} file record{} to status 'R' from 'O'".format(rstr, mcnt, s), PgLOG.WARNLG)

   return mcnt

#
# clean the reuqest usage saved previously
#
def clean_request_usage(ridx, cnd):
   
   pgrec = PgDBI.pgget("dspurge", "*", cnd, PgOPT.PGOPT['extlog'])
   if pgrec:
      if PgOPT.request_type(pgrec['rqsttype'], 1):
         PgDBI.pgdel("wfpurge", cnd, PgOPT.PGOPT['extlog'])
      PgDBI.pgdel("dspurge", cnd, PgOPT.PGOPT['extlog'])
      rdate = str(pgrec['date_rqst'])
      ms = re.match(r'^(\d\d\d\d)', rdate)
      atable = "allusage_{}".format(ms.group(1) if ms else 2004)
      PgDBI.pgdel("ousage", "order_number = 'r-{}'".format(ridx), PgOPT.PGOPT['extlog'])
      acnd = "email = '{}' AND method = 'R-{}' AND date = '{}' AND time = '{}'".format(
              pgrec['email'], pgrec['rqsttype'], rdate, pgrec['time_rqst'])
      PgDBI.pgdel(atable, acnd, PgOPT.PGOPT['extlog'])
      PgLOG.pglog("Pre-recorded usage information cleaned for Request Index {}".format(ridx), PgOPT.PGOPT['wrnlog'])

#
# email notice for request information
#
def email_request_status():

   tname = 'dsrqst'
   cnd = PgOPT.get_hash_condition(tname, None, None, 1)
   ocnd = PgOPT.get_order_string((PgOPT.params['ON'] if 'ON' in PgOPT.params else "r"), tname)
   pgrecs = PgDBI.pgmget(tname, "*", cnd + ocnd, PgOPT.PGOPT['extlog'])

   ALLCNT = len(pgrecs['rindex']) if pgrecs else 0
   if ALLCNT == 0:
      return PgLOG.pglog("{}: No Request Information Found to send email for {}".format(PgLOG.PGLOG['CURUID'], cnd), PgLOG.LOGWRN)

   if ALLCNT > 1:
      s = 's'
      ss = "are"
   else:
      s = ''
      ss = "is"

   subject = "{} Request Record{}".format(ALLCNT, s)
   if 'EL' in PgOPT.params and ALLCNT > PgOPT.params['EL']:
      mbuf = "{} of {}".format(PgOPT.params['EL'], subject)
      ALLCNT = PgOPT.params['EL']
   else:
      mbuf = subject
   mbuf += " {} listed:\n".format(ss)
   pgrecs['rstat'] = PgRqst.get_request_status(pgrecs, ALLCNT)
   for i in range(ALLCNT):
      mbuf += build_request_message(PgUtil.onerecord(pgrecs, i))

   if 'CC' in PgOPT.params: PgLOG.add_carbon_copy(PgOPT.params['CC'])
   subject += " found"
   PgLOG.send_email(subject, PgOPT.params['LN'], mbuf)
   PgLOG.pglog("Email sent to {} With Subject '{}'".format(PgOPT.params['LN'], subject), PgLOG.LOGWRN)

#
# build email message for a given request record
#
def build_request_message(pgrec):

   msg = ("\nIndex {} of {} for {}".format(pgrec['rindex'], pgrec['dsid'], PgOPT.request_type(pgrec['rqsttype'])) +
          " by {} on {}".format(pgrec['email'], pgrec['date_rqst']))
   if pgrec['status'] == 'O' or pgrec['status'] == 'H' and pgrec['date_ready']:
      msg += "\nCurrent status {} at {}/#dsrqst/{}/,".format(pgrec['rstat'], PgLOG.PGLOG['DSSURL'], pgrec['rqstid'])
      if pgrec['date_ready'] and pgrec['date_ready'] != pgrec['date_rqst']:
         msg += "built by {}, ".format(pgrec['date_ready'])
         msg += "with data sizes {} (out) / {} (in)".format(PgUtil.format_float_value(pgrec['size_request']),
                                                            PgUtil.format_float_value(pgrec['size_input']))
   else:
      msg += ", current status {}\n".format(pgrec['rstat'])

   return msg

#
# restore ALLCNT purged requests for reprocessing
#
def restore_requests():

   s = "s" if ALLCNT > 1 else ""
   PgLOG.pglog("Restore {} Request{} ...".format(ALLCNT, s), PgLOG.WARNLG)
   pcnt = 0
   for i in range(ALLCNT):
      pcnt += restore_one_request(PgOPT.params['RI'][i])

   PgLOG.pglog("{} of {} request{} retored at {}".format(pcnt, ALLCNT, s, PgUtil.curtime(1)), PgOPT.PGOPT['wrnlog'])

#
# restore a purge request
#
def restore_one_request(ridx):

   cnd = "rindex = {}".format(ridx)
   if PgDBI.pgget("dsrqst", "", cnd, PgOPT.PGOPT['extlog']):
      return PgLOG.pglog("RQST{}: not purged yet".format(ridx), PgOPT.PGOPT['errlog'])
   pgrqst = PgDBI.pgget("dspurge", "*", cnd, PgOPT.PGOPT['extlog'])
   if not pgrqst:
      return PgLOG.pglog("RQST{}: No purge info found".format(ridx), PgOPT.PGOPT['errlog'])
   rstr = "RQST{} of {}".format(ridx, pgrqst['dsid'])
   if pgrqst['specialist'] != PgOPT.params['LN']:
      return PgLOG.pglog("{}: Specialist '{}' to restore {}".format(PgOPT.params['LN'], pgrqst['specialist'], rstr), PgOPT.PGOPT['errlog'])

   if PgOPT.request_type(pgrqst['rqsttype'], 1):   # restore file records
      pgfiles = PgDBI.pgmget("wfpurge", "*", cnd, PgOPT.PGOPT['extlog'])
      fcnt = len(pgfiles['wfile']) if pgfiles else 0
      if fcnt > 0:
         cnt = 0
         for i in range(fcnt):
            pgrec = PgUtil.onerecord(pgfiles, i)
            pgrec = web_request_file(pgrec, pgrqst['dsid'])
            if not pgrec: continue
            pgrec['status'] = "R"
            cnt += PgDBI.pgadd("wfrqst", pgrec, PgOPT.PGOPT['extlog'])

         s = "s" if cnt > 1 else ""
         PgLOG.pglog("{} file record{} restored for {}".format(cnt, s, rstr), PgOPT.PGOPT['wrnlog'])
         PgDBI.pgdel("wfpurge", cnd, PgOPT.PGOPT['extlog'])   # clean purged file records
   
   # restore request record
   pgrec = {}
   pgrec['size_request'] = pgrqst['size_request']
   pgrec['size_input'] = pgrqst['size_input']
   pgrec['fcount'] = pgrqst['fcount']
   pgrec['status'] = PgOPT.params['RS'][0] if ('RS' in PgOPT.params and PgOPT.params['RS'][0]) else "W"
   pgrec['rqsttype'] = pgrqst['rqsttype']
   pgrec['dsid'] = pgrqst['dsid']
   pgrec['gindex'] = pgrqst['gindex']
   pgrec['date_rqst'] = pgrqst['date_rqst']
   pgrec['time_rqst'] = pgrqst['time_rqst']
   pgrec['specialist'] = pgrqst['specialist']
   pgrec['email'] = pgrqst['email']
   pgrec['fromflag'] = pgrqst['fromflag']
   if pgrqst['subflag']: pgrec['subflag'] = pgrqst['subflag']
   if pgrqst['location']: pgrec['location'] = pgrqst['location']
   if pgrqst['data_format']: pgrec['data_format'] = pgrqst['data_format']
   if pgrqst['file_format']: pgrec['file_format'] = pgrqst['file_format']
   pgrec['ip'] = pgrqst['ip']
   if pgrqst['note']: pgrec['note'] = pgrqst['note']
   if pgrqst['rinfo']: pgrec['rinfo'] = pgrqst['rinfo']
   pgrec['rindex'] = pgrqst['rindex']
   unames = PgDBI.get_ruser_names(pgrec['email'])
   if unames:
      lname = PgLOG.convert_chars(unames['lstname'], 'RQST')
      pgrec['rqstid'] = '{}{}'.format(lname.upper(), pgrec['rindex'])

   if((PgRqst.cache_request_control(ridx, pgrec, PgOPT.PGOPT['CACT'], 0) and 
      (PgOPT.PGOPT['RCNTL']['ptlimit'] or PgOPT.PGOPT['RCNTL']['ptsize']))):
      pgrec['ptcount'] = 0

   return PgDBI.pgadd("dsrqst", pgrec, PgOPT.PGOPT['extlog'])

#
# recreate a web request file 
#
def web_request_file(record, dsid):

   pgrec = PgSplit.pgget_wfile(dsid, "wfile, data_size size, data_format, file_format",
                       "wid = {}".format(record['srcid']), PgOPT.PGOPT['extlog'])
   if not pgrec: return None

   # source file information
   record = {}
   record['ofile'] = op.basename(pgrec['wfile'])
   record['srctype'] = "W"
   record['size'] = pgrec['size']
   record['data_format'] = pgrec['data_format']
   record['file_format'] = pgrec['file_format']

   return record

#
# call main() to start program
#
if __name__ == "__main__": main()
