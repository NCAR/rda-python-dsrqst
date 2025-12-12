#!/usr/bin/env python3
##################################################################################
#     Title: dsrqst
#    Author: Zaihua Ji, zji@ucar.edu
#      Date: 10/10/2020
#            2025-02-10 transferred to package rda_python_dsrqst from
#            https://github.com/NCAR/rda-utility-programs.git
#   Purpose: python utility program to stage data files online temporarily for
#            public users to download, including subset and data format conversion
#    Github: https://github.com/NCAR/rda-python-dsrqst.git
##################################################################################
import sys
import os
import re
import glob
import time
from os import path as op
from .pg_rqst import PgRqst

class PgRqst(PgRqst):

   def __init__(self):
      super().__init__()  # initialize parent class
      self.ALLCNT = 0   # global counting variables
      self.ERRMSG = ''
      self.TFSIZE = 1610612736   # 1.5GB, average tar file size
      self.MFSIZE = 536870912   # 0.5GB, skip member file if size is larger
      self.TCOUNT = 3   # no tar if file count is less
      self.CMPLMT = 100   # minimal partition limit for compression
      self.CMPCNT = 0   # compression partition count after command call
      self.EMLMAX = 5   # limit file error numbers for email

   # read in parameters
   def read_parameters(self):
      self.parsing_input('dsrqst')
      self.check_enough_options(self.PGOPT['CACT'])

   # start actions of dsrqst
   def start_actions(self):
      if self.PGOPT['CACT'] == 'CR':
         self.ALLCNT = len(self.params['RI'])
         self.clean_request_info()
      elif self.PGOPT['CACT'] == 'DL':
         if 'CI' in self.params: # delete request controls
            if 'WF' in self.params:  # delete web files
               self.ALLCNT = len(self.params['WF'])
               self.delete_source_files()
            else:
               self.ALLCNT = len(self.params['CI'])
               self.delete_request_control()
         elif 'WF' in self.params:  # delete web files
            self.ALLCNT = len(self.params['WF'])
            self.delete_web_files()
         elif 'RI' in self.params: # delete requests
            self.ALLCNT = len(self.params['RI'])
            self.delete_request_info()
         if 'UD' in self.params: self.clean_unused_data()
         if 'UF' in self.params: self.reset_all_file_status()
         if 'UR' in self.params: self.clean_unused_requests()
      elif self.PGOPT['CACT'] == 'ER':
         if not ('RI' in self.params or 'DS' in self.params):
            self.set_default_value("SN", self.params['LN'])
         self.email_request_status()
      elif self.PGOPT['CACT'] == 'GC':
         if not ('DS' in self.params or 'CI' in self.params):
            self.set_default_value("SN", self.params['LN'])
         self.get_request_control()
      elif self.PGOPT['CACT'] == 'GF':
         self.get_web_files()
      elif self.PGOPT['CACT'] == 'GP':
         if not ('DS' in self.params or 'RI' in self.params or 'PI' in self.params):
            self.set_default_value("SN", self.params['LN'])
         self.get_request_partitions()
      elif self.PGOPT['CACT'] == 'GR':
         if not ('RI' in self.params or 'DS' in self.params):
            self.set_default_value("SN", self.params['LN'])
         self.get_request_info()
      elif self.PGOPT['CACT'] == 'GT':
         self.get_tar_files()
      elif self.PGOPT['CACT'] == 'RP':
         self.ALLCNT = len(self.params['RI'])
         self.reset_purge_time()
      elif self.PGOPT['CACT'] == 'RR':
         self.ALLCNT = len(self.params['RI'])
         self.restore_requests()
      elif self.PGOPT['CACT'] == 'SC':
         self.ALLCNT = len(self.params['CI'])
         self.set_request_control()
      elif self.PGOPT['CACT'] == 'SF':
         if 'WF' in self.params:
            self.ALLCNT = len(self.params['WF'])
            self.set_web_files()
         else:
            self.reorder_request_files(self.params['ON'])
      elif self.PGOPT['CACT'] == 'SP':
         self.ALLCNT = len(self.params['PI']) if 'PI' in self.params else 0
         if self.ALLCNT > 0:
            self.set_request_partitions()
         else:
            self.ALLCNT = len(self.params['RI'])
            self.add_request_partitions()
      elif self.PGOPT['CACT'] == 'SR':
         self.ALLCNT = len(self.params['RI'])
         self.set_request_info()
      elif self.PGOPT['CACT'] == 'ST':
         if 'WF' in self.params:
            self.ALLCNT = len(self.params['WF'])
            self.set_tar_files()
         else:
            self.reorder_tar_files(self.params['ON'])
      elif self.PGOPT['CACT'] == 'UL':
         if 'PI' in self.params:
            self.ALLCNT = len(self.params['PI'])
            self.unlock_partition_info()
         else:
            self.ALLCNT = len(self.params['RI'])
            self.unlock_request_info()
      elif self.PGOPT['CACT'] == 'IR':
         self.ALLCNT = len(self.params['RI'])
         self.interrupt_requests()
      elif self.PGOPT['CACT'] == 'IP':
         self.ALLCNT = len(self.params['PI'])
         self.interrupt_partitions(self.params['PI'], self.ALLCNT)
      elif self.PGOPT['CACT'] == 'BR':
         self.ALLCNT = len(self.params['RI'])
         self.build_requests()
      elif self.PGOPT['CACT'] == 'PP':
         self.ALLCNT = len(self.params['PI'])
         self.process_partitions()
      elif self.PGOPT['CACT'] == 'PR':
         self.ALLCNT = len(self.params['RI'])
         self.purge_requests()
      if self.PGLOG['DSCHECK']:
         if self.ERRMSG:
            self.record_dscheck_error(self.ERRMSG)
         else:
            self.record_dscheck_status("D")
      if self.OPTS[self.PGOPT['CACT']][2]: self.cmdlog()   # log end time if not getting action

   # clean requests for given request indices
   def clean_request_info(self):
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("clean {} request{} ...".format(self.ALLCNT, s), self.WARNLG)
      self.check_local_writable(self.params['WH'], "Clean Request", self.PGOPT['extlog'])
      self.validate_multiple_options(self.ALLCNT, ['DS', 'RS'])
      for i in range(self.ALLCNT):
         ridx = self.params['RI'][i]
         rcnd = "rindex = {}".format(ridx)
         pgrec = self.pgget("dsrqst", "*", rcnd, self.PGOPT['extlog'])
         if not pgrec: continue
         record = {}
         if pgrec['ptcount'] > 1 or pgrec['pid'] and pgrec['lockhost'] == 'partition':
             if not self.clean_partition_info(ridx, rcnd, pgrec): continue
         if self.lock_request(ridx, 1, self.PGOPT['extlog']) <= 0: continue
         if pgrec['rqstid']:     # clean the request directory
            rdir = self.get_file_path(None, pgrec['rqstid'], None, 1)
            if op.isdir(rdir) and self.pgsystem("rm -rf " + rdir):
               self.pglog(rdir + ": Directory is removed", self.LOGWRN)
         if self.pgget("wfrqst", "", rcnd):
            if self.request_type(pgrec['rqsttype'], 1):
               cnt = self.pgexec("UPDATE wfrqst SET status = 'R', pindex = 0 WHERE " + rcnd, self.PGOPT['extlog'])
               s = 's' if cnt > 1 else ''
               self.pglog("{} file record{} set to status 'R' for {}".format(cnt, s, rcnd), self.LOGWRN)
            else:
               cnt = self.pgdel("wfrqst", rcnd, self.PGOPT['extlog'])
               s = 's' if cnt > 1 else ''
               self.pglog("{} file record{} removed for {}".format(cnt, s, rcnd), self.LOGWRN)
         if self.pgget("tfrqst", "", rcnd):
            cnt = self.pgdel("tfrqst", rcnd, self.PGOPT['extlog'])
            s = 's' if cnt > 1 else ''
            self.pglog("{} tar file record{} removed for {}".format(cnt, s, rcnd), self.LOGWRN)
         if pgrec['pcount']: record['pcount'] = 0
         if not self.request_type(pgrec['rqsttype'], 1):
             if pgrec['fcount']: record['fcount'] = 0
             if pgrec['size_request']: record['size_request'] = 0
         if (self.cache_request_control(ridx, pgrec, self.PGOPT['CACT'], 0) and 
            (self.PGOPT['RCNTL']['ptlimit'] or self.PGOPT['RCNTL']['ptsize'])):
            if pgrec['ptcount']: record['ptcount'] = 0
         else:
            if pgrec['ptcount'] != 1: record['ptcount'] = 1
         if pgrec['tarcount']: record['tarcount'] = 0
         record['ecount'] = record['exectime'] = record['pid'] = 0
         record['lockhost'] = ''
         if 'RS' in self.params and self.params['RS'][i]:
            record['status'] = self.params['RS'][i]
         if 'RN' in self.params and self.params['RN'][i]:
            record['rqstid'] = self.params['RN'][i]
         if self.pgupdt("dsrqst", record, rcnd, self.PGOPT['extlog']):
            self.clean_request_usage(ridx, rcnd)
            self.pglog("{} Request {} is cleaned".format(self.request_type(pgrec['rqsttype']), ridx), self.LOGWRN)

   # clean request partitions for given request index
   def clean_partition_info(self, ridx, cnd, pgrqst):
      pgrecs = self.pgmget("ptrqst", "pindex", cnd, self.PGOPT['extlog'])
      pcnt = len(pgrecs['pindex']) if pgrecs else 0
      if pcnt > 0:
         s = 's' if pcnt > 1 else ''
         self.pglog("clean {} request partition{} for {} ...".format(pcnt, s, cnd), self.WARNLG)
         for i in range(pcnt):
            pidx = pgrecs['pindex'][i]
            pcnd = "pindex = {}".format(pidx)
            if self.lock_partition(pidx, 1, self.PGOPT['extlog']) <= 0:
               return self.pglog("RQST{}: Cannot clean partition, {} is locked".format(ridx, pcnd), self.PGOPT['errlog'])
            self.pgdel("ptrqst", pcnd, self.PGOPT['extlog'])
         if self.pgget('dsrqst', '', cnd + " AND lockhost = 'partition' AND pid > 0"):
            self.pgexec("UPDATE dsrqst SET pid = 0 WHERE " + cnd, self.PGOPT['extlog'])
      return 1

   # delete one request for given request indix
   def delete_one_request(self, ridx, dcnt, cleanusage = 0):
      cnd = "rindex = {}".format(ridx)
      pgrqst = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
      if not pgrqst: self.action_error("Error get Request Record for " + cnd)
      shared = self.request_type(pgrqst['rqsttype'], 1)
      if pgrqst['rqstid'] and not pgrqst['location']:  # clean the request directory
         dpath = self.get_file_path(None, pgrqst['rqstid'], None, 1)
         if dpath != self.params['WH'] and op.isdir(dpath):
            if shared:
               cnt = 0
            else:
               files = glob.glob(dpath + "/*")
               cnt = len(files)
               if cnt > 0:
                  file = dpath + "/index.html"
                  if file in files: cnt -= 1
            self.pgsystem("rm -rf " + dpath, self.PGOPT['extlog'], 5)
            if cnt > 0:
               s = 's' if cnt > 1 else ''
               self.pglog("Directory {} and {} file{} under it are removed".format(dpath, cnt, s), self.LOGWRN)
               dcnt[2] += cnt
            else:
               self.pglog("Directory {} is removed".format(dpath), self.LOGWRN)
      if shared:
         pgrecs = self.pgmget("wfrqst", "wfile, ofile", cnd, self.PGOPT['extlog'])
         cnt = len(pgrecs['wfile']) if pgrecs else 0
         if cnt > 0:
            s = 's' if cnt > 1 else ''
            self.pglog("Delete {} associated file{} for Request Index {} ...".format(cnt, s, ridx), self.WARNLG)
            dpath = "data/" + pgrqst['dsid']
            for j in range(cnt):
               self.delete_one_file(pgrqst, pgrecs['wfile'][j], pgrecs['ofile'][j], dpath, 1, dcnt)
      else:
         cnt = self.pgdel("wfrqst", cnd, self.PGOPT['extlog'])
         if cnt > 0:
            s = 's' if cnt > 1 else ''
            self.pglog("{} file record{} removed from RDADB".format(cnt, s), self.LOGWRN)
            dcnt[0] += cnt
            dcnt[1] += cnt
      if pgrqst['ptcount'] > 1 or self.pgget("ptrqst", "", cnd):
         cnt = self.pgdel("ptrqst", cnd, self.PGOPT['extlog'])
         s = 's' if cnt > 1 else ''
         self.pglog("{} partition record{} removed from RDADB".format(cnt, s), self.LOGWRN)
      if self.pgget("tfrqst", "", cnd):
         cnt = self.pgdel("tfrqst", cnd, self.PGOPT['extlog'])
         s = 's' if cnt > 1 else ''
         self.pglog("{} tar file record{} removed from RDADB".format(cnt, s), self.LOGWRN)
      if self.pgdel("dsrqst", cnd, self.PGOPT['extlog']):
         if cleanusage: self.clean_request_usage(ridx, cnd)
         return 1
      else:
         return 0

   # delete requests for given request indices
   def delete_request_info(self):
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Delete {} request{} ...".format(self.ALLCNT, s), self.WARNLG)
      self.check_local_writable(self.params['WH'], "Delete Request", self.PGOPT['extlog'])
      self.validate_multiple_options(self.ALLCNT, ["DS"])
      dcnt = [0]*3
      delcnt = 0
      for i in range(self.ALLCNT):
         ridx = self.lock_request(self.params['RI'][i], 1, self.PGOPT['extlog'])
         if ridx <= 0: continue
         delcnt += self.delete_one_request(ridx, dcnt, 1)
      self.pglog("{} of {} request{} deleted".format(delcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])
      if dcnt[0] > 0:
         s = 's' if dcnt[0] > 1 else ''
         self.pglog("{}/{} of {} request file{} deleted from RDADB/Disk".format(dcnt[1], dcnt[2], dcnt[0], s), self.PGOPT['wrnlog'])

   # delete request controls for given request control indices
   def delete_request_control(self):
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Delete {} request control{} ...".format(self.ALLCNT, s), self.WARNLG)
      delcnt = 0
      for i in range(self.ALLCNT):
         cnd = "cindex = {}".format(self.params['CI'][i])
         delcnt += self.pgdel("rcrqst", cnd, self.PGOPT['extlog'])
      self.pglog("{} of {} request control{} deleted".format(delcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # delete online files for given request indices
   def delete_web_files(self):
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Delete {} request file{} ...".format(self.ALLCNT, s), self.WARNLG)
      self.validate_multiple_options(self.ALLCNT, ["RI"])
      ridx = 0
      dcnt = [0]*3
      for i in range(self.ALLCNT):
         if ridx != self.params['RI'][i]:
            ridx = self.lock_request(self.params['RI'][i], 1, self.PGOPT['extlog'])
            if ridx <= 0: continue
            cnd = "rindex = {}".format(ridx)
            pgrqst = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
            if not pgrqst: self.action_error("Error get Request Record for " + cnd)
            shared = self.request_type(pgrqst['rqsttype'], 1)
            dpath = "data/" + pgrqst['dsid'] if shared else pgrqst['rqstid']
         pgrec = self.pgget("wfrqst", "ofile", "{} AND wfile = '{}'".format(cnd, self.params['WF'][i]), self.PGOPT['extlog'])
         self.delete_one_file(pgrqst, self.params['WF'][i], pgrec['ofile'] if pgrec else None, dpath, shared, dcnt)
         if i > (self.ALLCNT - 2) or  ridx != self.params['RI'][i+1]:
            self.set_request_count(cnd, pgrec)
            self.lock_request(ridx, 0, self.PGOPT['extlog'])   # unlock requests
      self.pglog("{}/{} of {} request file{} deleted from RDADB/Disk".format(dcnt[1], dcnt[2], self.ALLCNT, s), self.PGOPT['wrnlog'])

   # delete including source files for given request control indices
   def delete_source_files(self):
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Delete {} including source file{} ...".format(self.ALLCNT, s), self.WARNLG)
      self.validate_multiple_options(self.ALLCNT, ["CI"])
      cidx = dcnt = 0
      for i in range(self.ALLCNT):
         if cidx != self.params['CI'][i]:
            cnd = "cindex = {}".format(cidx)
            pgrec = self.pgget("rcrqst", "*", cnd, self.PGOPT['extlog'])
            if not pgrec:
               self.action_error("Error get Request Control Record for " + cnd)
         dcnt += self.pgdel("sfrqst", "{} AND wfile = '{}'".format(cnd, self.params['WF'][i]), self.PGOPT['extlog'])
      self.pglog("{} of {} source file{} deleted from RDADB".format(dcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # Remove file record in RDADB and delete file physically on disk if needed
   def delete_one_file(self, pgrqst, wfile, ofile, dpath, shared, cnts):
      ridx = pgrqst['rindex']
      cnd = "rindex = {}".format(ridx)
      cnts[0] += 1
      cnts[1] += self.pgdel("wfrqst", "{} AND wfile = '{}'".format(cnd, wfile), self.PGOPT['extlog'])
      file = self.get_file_path(wfile, dpath, None, 1)
      info = self.check_local_file(file, 1, self.PGOPT['wrnlog'])
      if info:
         retain = 0
         if shared:
            pgrecs = self.pgmget("wfrqst", "*", "wfile = '{}'".format(wfile), self.PGOPT['extlog'])
            cnt = len(pgrecs['rindex']) if pgrecs else 0
            for i in range(cnt):
               pgrec = self.onerecord(pgrecs, i)
               rcnd = "rindex = {}".format(pgrec['rindex'])
               if not self.pgget("dsrqst", "", "{} AND dsid = '{}'".format(cnd, pgrqst['dsid']), self.PGOPT['extlog']): continue
               if pgrec['status'] == "O":
                  retain += 1
                  continue
               else:
                  record = {'status' : 'O', 'size' : pgrec['size'], 'date' : pgrec['date'], 'time' : pgrec['time']}
                  retain += self.pgupdt("wfrqst", record, "{} AND wfile = '{}'".format(cnd, wfile), self.PGOPT['extlog'])
         if not retain and self.pgsystem("rm -f " + file):
            self.pglog(file + ": deleted", self.PGOPT['wrnlog'])
            cnts[2] += 1
      if ofile and ofile != wfile:
         file = self.get_file_path(ofile, dpath, None, 1)
         info = self.check_local_file(file, 1, self.PGOPT['wrnlog'])
         if info:
            if not (shared and self.pgget("wfrqst, dsrqst", "", "wfrqst.rindex = dsrqst.rindex AND ofile = '{}' AND dsid = '{}'".format(ofile, pgrqst['dsid']))):
               if self.pgsystem("rm -f " + ofile):
                  self.pglog(ofile + ": deleted", self.PGOPT['wrnlog'])
                  cnts[2] += 1

   # get request information
   def get_request_info(self):
      tname = "dsrqst"
      hash = self.TBLHASH[tname]
      self.pglog("Get request information from RDADB ...", self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['dsall'])
      if 'CS' in self.params:
          if 'A' not in fnames: fnames += "A"
          if 'R' not in fnames: fnames = "R" + fnames
      onames = self.params['ON'] if 'ON' in self.params else "R"
      condition = self.get_hash_condition(tname, None, None, 1)
      if 'ON' in self.params and 'OB' in self.params:
         oflds = onames
      else:
         condition += self.get_order_string(onames, tname)
      pgrecs = self.pgmget(tname, "*", condition, self.PGOPT['extlog'])
      if self.PGOPT['CACT'] == "GB": self.OUTPUT.write("[DSRQST]\n")
      if pgrecs:
          if 'CS' in self.params: pgrecs['status'] = self.get_request_status(pgrecs)
          if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
          if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = "s" if cnt > 1 else ""
         self.pglog("{} request{} retrieved".format(cnt, s), self.PGOPT['wrnlog'])
      else:
         self.pglog("No request information retrieved", self.PGOPT['wrnlog'])

   # get request control information
   def get_request_control(self):
      tname = "rcrqst"
      hash = self.TBLHASH[tname]
      self.pglog("Get request control information from RDADB ...", self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['rcall'])
      onames = self.params['ON'] if 'ON' in self.params else "BIT"
      condition = self.get_hash_condition(tname, None, None, 1)
      if 'ON' in self.params and self.params['OB']:
         oflds = onames
      else:
         condition += self.get_order_string(onames, tname)
      pgrecs = self.pgmget(tname, "*", condition, self.PGOPT['extlog'])
      if pgrecs:
          if 'FO' in self.params: lens = self.all_column_widths(pgrecs,fnames, hash)
          if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = 's' if cnt > 1 else ''
         self.pglog("{} request control{} retrieved".format(cnt, s), self.PGOPT['wrnlog'])
      else:
         self.pglog("No request control information retrieved", self.PGOPT['wrnlog'])

   # get request partition information
   def get_request_partitions(self):
      tname = "ptrqst"
      hash = self.TBLHASH[tname]
      self.pglog("Get request partition information from RDADB ...", self.WARNLG)
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['ptall'])
      if 'CS' in self.params:
          if 'A' not in fnames: fnames += "A"
          if 'P' not in fnames: fnames = "P" + fnames
      onames = self.params['ON'] if 'ON' in self.params else "P"
      condition = self.get_hash_condition(tname, None, None, 1)
      if 'ON' in self.params and 'OB' in self.params:
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      pgrecs = self.pgmget(tname, "*", condition, self.PGOPT['extlog'])
      if pgrecs:
          if 'CS' in self.params: pgrecs['status'] = self.get_partition_status(pgrecs)
          if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
          if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = 's' if cnt > 1 else ''
         self.pglog("{} request partition{} retrieved".format(cnt, s), self.PGOPT['wrnlog'])
      else:
         self.pglog("No request partition information retrieved", self.PGOPT['wrnlog'])

   # get online file information
   def get_web_files(self):
      tables = "wfrqst INNER JOIN dsrqst ON wfrqst.rindex = dsrqst.rindex"
      tname = 'wfrqst'
      hash = self.TBLHASH[tname]
      self.pglog("Get request file information from RDADB ...", self.WARNLG)
      dojoin = 0
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['wfall'])
      if 'WD' in self.params:
         fnames += "B"
         dojoin = 1
      qnames = fnames
      if 'R' not in qnames: qnames += 'R'
      onames = self.params['ON'] if 'ON' in self.params else "RO"
      qnames += self.append_order_fields(onames, fnames, tname)
      if not dojoin and 'DS' in self.params: dojoin = 1
      condition = self.get_hash_condition(tname, None, None, 1)
      if 'ON' in self.params and ('OB' in self.params):
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      pgrecs = self.pgmget(tables if dojoin else tname, self.get_string_fields(qnames, tname), condition, self.PGOPT['extlog'])
      if self.PGOPT['CACT'] == "GB": self.OUTPUT.write("[{}]\n".format(tname.upper()))
      if pgrecs:
         if 'srcid' in pgrecs:
            dsids = self.get_request_dsids(pgrecs['rindex'])
            pgrecs['srcid'] = self.fid2fname(pgrecs['srcid'], dsids, pgrecs['srctype'])
         if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
         if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = "s" if cnt > 1 else ""
         self.pglog("{} request file record{} retrieved".format(cnt, s), self.PGOPT['wrnlog'])
      else:
         self.pglog("no request file record retrieved", self.PGOPT['wrnlog'])

   # get online file information
   def get_tar_files(self):
      tables = "tfrqst INNER JOIN dsrqst ON tfrqst.rindex = dsrqst.rindex"
      tname = "tfrqst"
      hash = self.TBLHASH[tname]
      self.pglog("Get tar file information from RDADB ...", self.WARNLG)
      dojoin = 0
      lens = oflds = fnames = None
      if 'FN' in self.params: fnames = self.params['FN']
      fnames = self.fieldname_string(fnames, self.PGOPT[tname], self.PGOPT['tfall'])
      if 'WD' in self.params:
         fnames += "B"
         dojoin = 1
      qnames = fnames
      onames = self.params['ON'] if 'ON' in self.params else "RO"
      qnames += self.append_order_fields(onames, fnames, tname)
      if not dojoin and 'DS' in self.params: dojoin = 1
      condition = self.get_hash_condition(tname, None, None, 1)
      if 'ON' in self.params and self.params['OB']:
         oflds = self.append_order_fields(onames, None, tname)
      else:
         condition += self.get_order_string(onames, tname)
      pgrecs = self.pgmget(tables if dojoin else tname, self.get_string_fields(qnames, tname), condition, self.PGOPT['extlog'])
      if pgrecs:
         if 'FO' in self.params: lens = self.all_column_widths(pgrecs, fnames, hash)
         if oflds: pgrecs = self.sorthash(pgrecs, oflds, hash, self.params['OB'])
      self.OUTPUT.write(self.get_string_titles(fnames, hash, lens) + "\n")
      if pgrecs:
         cnt = self.print_column_format(pgrecs, fnames, hash, lens)
         s = "s" if cnt > 1 else ""
         self.pglog("{} tar file record{} retrieved".format(cnt, s), self.PGOPT['wrnlog'])
      else:
         self.pglog("no tar file record retrieved", self.PGOPT['wrnlog'])

   # add or modify request information
   def set_request_info(self):
      tname = "dsrqst"
      hash = self.TBLHASH[tname]
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Set information of {} request{} ...".format(self.ALLCNT, s), self.WARNLG)
      addcnt = modcnt = 0
      if 'SN' not in self.params:
         self.params['SN'] = [self.params['LN']]
         self.OPTS['SN'][2] |= 2
      if 'WN' in self.params:
         if 'FC' not in self.params: self.params['FC'] = [0]*self.ALLCNT
         for i in range(self.ALLCNT):
            self.params['FC'][i] = self.pgget("wfrqst", "", "rindex = {}".format(self.params['RI'][i]), self.PGOPT['extlog'])
      flds = self.get_field_keys(tname, None, "R")
      self.validate_multiple_values(tname, self.ALLCNT, flds)
      if 'GU' in self.params and not self.params['RS']: flds += 'A'
      for i in range(self.ALLCNT):
         ridx = self.params['RI'][i]
         if ridx > 0:
            if self.lock_request(ridx, 1, self.PGOPT['extlog']) <= 0: continue
            cnd = "rindex = {}".format(ridx)
            pgrec = self.pgget(tname, "*", cnd, self.PGOPT['extlog'])
            if pgrec:
               if 'MD' not in self.params and pgrec['specialist'] != self.params['LN'] and self.params['LN'] != self.PGLOG['GDEXUSER']:
                  self.action_error("{}: Must be '{}' to set request index {}".format(self.params['LN'], pgrec['specialist'], cnd))
               if 'GU' in self.params:
                  if "POH".find(pgrec['status']) > -1:
                     self.purge_one_request(ridx, self.curdate(), self.curtime(), 0)
                  else:
                     self.pglog("Status '{}' of Request {} must be in ('O', 'P', 'H') to gather usage".format(pgrec['status'], ridx), self.PGOPT['wrnlog'])
                     continue
            else:
               self.action_error("Miss request record for " + cnd)
         else:
            email = self.params['EM'][i] if 'EM' in self.params else None
            if not email: self.action_error("Miss user email to add new Request")
            unames = self.get_ruser_names(email)
            if not unames: continue
            pgrec = None
         if 'RS' in self.params and self.params['RS'][i] and len(self.params['RS'][i]) > 1:
            self.params['RS'][i] = self.params['RS'][i][0]   # just in case
         record = self.build_record(flds, pgrec, tname, i)
         if record:
            if 'dsid' in record: self.validate_dsowner("dsrqst", record['dsid'])
            if 'location' in record:
               rtype = record['rqsttype'] if 'rqsttype' in record else (pgrec['rqsttype'] if pgrec['rqsttype'] else 'U')
               if 'FHQST'.find(rtype) < 0:
                  self.pglog("Can not set output location for Type '{}' Request {}".format(rtype, ridx), self.PGOPT['wrnlog'])
                  continue
            if pgrec:
               record['pid'] = 0
               record['lockhost'] = ''
               if not ('rqstid' in record or pgrec['rqstid']):
                  record['rqstid'] = self.add_request_id(ridx, pgrec['email'])
               if 'status' in record and record['status'] == 'O':
                  if not ('fcount' in record or pgrec['fcount']): pgrec['fcount'] = self.set_request_count(cnd, pgrec, 1)
                  if not (pgrec['date_ready'] or 'date_ready' in record): record['date_ready'] = self.curdate()
                  if not (pgrec['time_ready'] or 'time_ready' in record): record['time_ready'] = self.curtime()
                  if not (pgrec['date_purge'] or 'date_purge' in record):
                     record['date_purge'] = self.adddate(record['date_ready'] if 'date_ready' in record else pgrec['date_ready'], 0, 0, self.PGOPT['VP'])
                  if not (pgrec['time_purge'] or 'time_purge' in record):
                     record['time_purge'] = record['time_ready'] if 'time_ready' in record else pgrec['time_ready']
               pcnt = 0
               if 'status' in record and record['status'] == 'Q':
                  if pgrec['ptcount'] == -1: record['ptcount'] = 1
                  if pgrec['status'] == 'E':
                     if pgrec['ptcount'] > 1: pcnt = self.pgexec("UPDATE ptrqst SET status = 'Q' WHERE {} AND status = 'E'".format(cnd), self.PGOPT['extlog'])
                     record['ecount'] = 0
               modcnt += self.pgupdt(tname, record, cnd, self.PGOPT['extlog']|self.DODFLT)
               if pcnt: self.pglog("RQST{}: SET {} Partition Status E to Q".format(ridx, pcnt), self.PGOPT['wrnlog'])
            else:
               if 'specialist' not in record:
                  record['specialist'] = self.params['LN']
               elif 'MD' not in self.params and record['specialist'] != self.params['LN'] and self.params['LN'] != self.PGLOG['GDEXUSER']:
                  self.action_error("Must be '{}' to add request record".format(record['specialist']))
               if 'rqsttype' not in record: record['rqsttype'] = "C"  # default to customized request type
               nidx = self.new_request_id()
               lname = self.convert_chars(unames['lstname'], 'RQST')
               record['rqstid'] = "{}{}".format(lname.upper(), nidx)   # auto set request ID
               record['fromflag'] = 'M'
               if 'date_rqst' not in record: record['date_rqst'] = self.curdate()
               if 'time_rqst' not in record: record['time_rqst'] = self.curtime()
               ridx = self.pgadd(tname, record, self.PGOPT['extlog']|self.AUTOID|self.DODFLT)
               if ridx > 0:
                  cnd = "rindex = {}".format(ridx)
                  record = {}
                  pgrec = self.pgget(tname, "*", cnd, self.PGOPT['extlog'])
                  if ridx != nidx: record['rqstid'] = "{}{}".format(lname.upper(), ridx)   # auto reset request ID
                  if self.cache_request_control(ridx, pgrec, self.PGOPT['CACT'], 0):
                     if (self.PGOPT['RCNTL']['ptlimit'] or self.PGOPT['RCNTL']['ptsize']):
                        record['ptcount'] = 0
                        record['size_request'] = 0
                     if 'RS' not in self.params:
                        stat = 'Q' if self.PGOPT['RCNTL']['control'] == 'A' else 'W'
                        if pgrec['status'] != stat: record['status'] = stat
                  if pgrec['date_ready']: record['date_ready'] = None
                  if pgrec['time_ready']: record['time_ready'] = None
                  if pgrec['date_purge']: record['date_purge'] = None
                  if pgrec['time_purge']: record['time_purge'] = None
                  if record: self.pgupdt(tname, record, cnd, self.PGOPT['extlog'])
                  self.pglog("{}: Request Index {} added for <{}> {}".format(self.params['DS'][i], ridx, unames['name'], email), self.PGOPT['wrnlog'])
                  addcnt += 1
         elif pgrec: # unlock request
            self.lock_request(ridx, 0, self.PGOPT['extlog'])
      self.pglog("{}/{} of {} request{} added/modified in RDADB!".format(addcnt, modcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # find a unique request name/ID from given user break name
   # by appending (existing maximum rindex + 1) 
   def new_request_id(self):
      pgrec = self.pgget("dsrqst", "MAX(rindex) maxid", '', self.LOGERR)
      if pgrec:
         return (pgrec['maxid'] + 1)
      else:
         return 0

   # modify request partition information
   def set_request_partitions(self):
      tname = "ptrqst"
      hash = self.TBLHASH[tname]
      s = 's' if self.ALLCNT > 1 else ''
      ridx = 0
      ridxs = {}
      self.pglog("Set information of {} request partition{} ...".format(self.ALLCNT, s), self.WARNLG)
      modcnt = 0
      flds = self.get_field_keys(tname)
      self.validate_multiple_values(tname, self.ALLCNT, flds)
      for i in range(self.ALLCNT):   
         pidx = self.params['PI'][i]
         cnd = "pindex = {}".format(pidx)
         pgrec = self.pgget(tname, "*", cnd, self.PGOPT['extlog'])
         if not pgrec: self.action_error("Error get Request Partition for " + cnd)
         record = self.build_record(flds, pgrec, tname, i)
         if record and self.pgupdt(tname, record, cnd, self.PGOPT['extlog']):
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
         pgrqst = self.pgget(tname, "ecount, status", rcnd, self.PGOPT['extlog'])
         if not pgrqst: self.pglog("{}: request record not in RDADB".format(ridx), self.PGOPT['extlog'])
         record['ecount'] = pgrqst['ecount'] - ridxs[ridx]
         if record['ecount'] < 0: record['ecount'] = 0
         if pgrqst['status'] == 'E': record['status'] = 'Q'
         if self.pgupdt(tname, record, rcnd, self.PGOPT['extlog']):
            if 'status' in record: self.pglog("RQST{}: SET Request Status E to Q".format(ridx), self.PGOPT['wrnlog'])
      self.pglog("{} of {} request partition{} modified in RDADB!".format(modcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # add or modify request control information
   def set_request_control(self):
      tname = "rcrqst"
      hash = self.TBLHASH[tname]
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Set information of {} request control{} ...".format(self.ALLCNT, s), self.WARNLG)
      addcnt = modcnt = 0
      flds = self.get_field_keys(tname, None, 'C')
      self.validate_multiple_values(tname, self.ALLCNT, flds)
      dsids = {}
      pcnts = {}
      for i in range(self.ALLCNT):  
         cidx = self.params['CI'][i] if 'CI' in self.params else 0
         if cidx > 0:
            cnd = "cindex = {}".format(cidx)
            pgrec = self.pgget(tname, "*", cnd, self.PGOPT['extlog'])
            if not pgrec: self.action_error("Miss control record for " + cnd)
            if 'MD' not in self.params and pgrec['specialist'] != self.params['LN'] and self.params['LN'] != self.PGLOG['GDEXUSER']:
               self.action_error("{}: Must be '{}' to set reuqest control {}".format(self.params['LN'], pgrec['specialist'], cnd))
         else:
            pgrec  = None
         record = self.build_record(flds, pgrec, tname, i)
         if record:
            dsid = record['dsid'] if 'dsid' in record else pgrec['dsid']
            if 'gindex' in record and record['gindex']:
               grec = self.pgget("dsgroup", "pindex", "dsid = '{}' AND gindex = {}".format(dsid, record['gindex']), self.PGOPT['extlog'])
               if not grec:
                  self.pglog("Group Index {}: not exists in '{}'".format(record['gindex'], dsid), self.LOGERR)
                  continue
               elif grec['pindex']:
                  self.pglog("Group Index {}: not a top group in '{}'".format(record['gindex'], dsid), self.LOGERR)
                  continue
            if pgrec:
               if dsid != pgrec['dsid']:
                  self.pglog("pgrec['dsid']-pgrec['cindex']: Cannot change dataset to pgrec['dsid'] for existing Request Control", self.LOGERR)
                  continue
               modcnt += self.pgupdt(tname, record, cnd, self.PGOPT['extlog'])
            else:
               if 'rqsttype' not in record:
                  self.pglog("Missing Request Type to add Request Control", self.LOGERR)
                  continue
               if 'specialist' not in record:
                  record['specialist'] = self.params['LN']
               elif 'MD' not in self.params and record['specialist'] != self.params['LN'] and self.params['LN'] != self.PGLOG['GDEXUSER']:
                  self.action_error("{}: Must be '{}' to add request control record".format(self.params['LN'], record['specialist']))
               cidx = self.pgadd(tname, record, self.PGOPT['extlog']|self.AUTOID)
               if cidx:
                  self.pglog("Request Control Index {} added".format(cidx), self.PGOPT['wrnlog'])
                  addcnt += 1
            if 'rqsttype' in record and dsid not in dsids:
               rtype = record['rqsttype']
               if dsid not in pcnts: pcnts[dsid] = {}
               if rtype not in pcnts[dsid]:
                  pcnts[dsid][rtype] = self.pgget(tname, "", "dsid = '{}' AND rqsttype = '{}'".format(dsid, rtype))
                  if pcnts[dsid][rtype] == 1: dsids[dsid] = 1
      self.pglog("{}/{} of {} request control{} added/modified in RDADB!".format(addcnt, modcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # add or modify information for tar files of requested online web files
   def set_tar_files(self, rindex):
      tname = "tfrqst"
      hash = self.TBLHASH[tname]
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Set information of {} tar file{} ...".format(self.ALLCNT, s), self.WARNLG)
      modcnt = 0
      flds = self.get_field_keys(tname, None, "B")   # exclude dataset
      self.validate_multiple_values(tname, self.ALLCNT, flds)
      if 'RO' in self.params and not self.params['DO']: flds += 'O'   
      fields = self.get_string_fields(flds, tname)
      dsids = self.get_request_dsids(self.params['RI'])
      ridx = rindex if rindex else 0
      for i in range(self.ALLCNT):
         if not rindex and ridx != self.params['RI'][i]:
            ridx = self.lock_request(self.params['RI'][i], 1, self.PGOPT['extlog'])
            if ridx <= 0: continue
         if 'TI' in self.params:
            tcnd = "tindex = {}".format(self.params['TI'][i])
         else:
            tcnd = "wfile = '{}'".format(self.params['WF'][i])
         cnd = "rindex = {} AND {}".format(ridx, tcnd)
         pgrec = self.pgget(tname, fields, cnd, self.PGOPT['extlog'])
         if not pgrec: self.action_error("Error get Tar File info for " + cnd)
         if 'RO' in self.params: self.params['DO'][i] = self.get_next_disp_order(ridx, tname)
         record = self.build_record(flds, pgrec, tname, i)
         if record:
            modcnt += self.pgupdt(tname, record, cnd, self.PGOPT['extlog']|self.DODFLT)
         if not rindex and (i > (self.ALLCNT - 2) or  ridx != self.params['RI'][i + 1]):
            self.set_request_count("rindex = {}".format(ridx))
            self.lock_request(ridx, 0, self.PGOPT['extlog'])   # unlock requests
      self.pglog("{} of {} tar file{} modified in RDADB!".format(modcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # add or modify requested online web file information
   def set_web_files(self, rindex = 0):
      tname = "wfrqst"
      stypes = 'CMW'
      hash = self.TBLHASH[tname]
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Set information of {} requested file{} ...".format(self.ALLCNT, s), self.WARNLG)
      addcnt = modcnt = 0
      flds = self.get_field_keys(tname, None, "B")   # exclude dataset
      self.validate_multiple_values(tname, self.ALLCNT, flds)
      if 'RO' in self.params and not self.params['DO']: flds += 'O'   
      fields = self.get_string_fields(flds, tname)
      if 'SL' in self.params:
         dsids = self.get_request_dsids(self.params['RI'])
         self.params['SL'] = self.fname2fid(self.params['SL'], dsids, self.params['OT'])
      ridx = rindex if rindex else 0
      for i in range(self.ALLCNT):
         if not rindex and ridx != self.params['RI'][i]:
            ridx = self.lock_request(self.params['RI'][i], 1, self.PGOPT['extlog'])
            if ridx <= 0: continue
         cnd = "rindex = {} AND wfile = '{}'".format(ridx, self.params['WF'][i])
         pgrec = self.pgget(tname, fields, cnd, self.PGOPT['extlog'])
         if 'RO' in self.params: self.params['DO'][i] = self.get_next_disp_order(ridx, tname)
         record = self.build_record(flds, pgrec, tname, i)
         if record:
            if 'srctype' in record and stypes.find(record['srctype']) < 0:
              self.action_error("{}: Source type must be one of '{}'".format(record['srctype'], stypes))
            if pgrec:
               modcnt += self.pgupdt(tname, record, cnd, self.PGOPT['extlog']|self.DODFLT)
            else:
               if not ('disp_order' in record and record['disp_order']): record['disp_order'] = self.get_next_disp_order(ridx, tname)
               addcnt += self.pgadd(tname, record, self.PGOPT['extlog']|self.DODFLT)
         if not rindex and (i > (self.ALLCNT-2) or ridx != self.params['RI'][i+1]):
            self.set_request_count("rindex = {}".format(ridx))
            self.lock_request(ridx, 0, self.PGOPT['extlog'])   # unlock requests
      self.pglog("{}/{} of {} request file{} added/modified in RDADB!".format(addcnt, modcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # unlock requests for given request indices
   def unlock_request_info(self):
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Unlock {} request{} ...".format(self.ALLCNT, s), self.WARNLG)
      modcnt = 0
      for ridx in self.params['RI']:
         pgrec = self.pgget("dsrqst", "pid, lockhost", "rindex = {}".format(ridx), self.PGOPT['extlog'])
         if not pgrec:
            self.pglog("Request {}: Not exists".format(ridx), self.PGOPT['errlog'])
         elif not pgrec['pid']:
            self.pglog("Request {}: Not locked".format(ridx), self.PGOPT['wrnlog'])
         elif pgrec['lockhost'] == "partition":
            self.pglog("Request {}: Partition of the request are under processing".format(ridx), self.PGOPT['wrnlog'])
         elif self.lock_request(ridx, 0, self.PGOPT['extlog']) > 0:
            modcnt += 1
            self.pglog("Request ridx: Unlocked {}/{}".format(ridx, pgrec['pid'], pgrec['lockhost']), self.PGOPT['wrnlog'])
         elif (self.check_host_down(None, pgrec['lockhost']) and
               self.lock_request(ridx, -2, self.PGOPT['extlog']) > 0):
            modcnt += 1
            self.pglog("Request {}: Force unlocked {}/{}".format(ridx, pgrec['pid'], pgrec['lockhost']), self.PGOPT['wrnlog'])
         else:
            self.pglog("Request {}: Unable to unlock {}/{}".format(ridx, pgrec['pid'], pgrec['lockhost']), self.PGOPT['wrnlog'])
      if self.ALLCNT > 1: self.pglog("{} of {} request{} unlocked from RDADB".format(modcnt, self.ALLCNT), self.LOGWRN) 

   # unlock request partitions for given partition indices
   def unlock_partition_info(self):
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Unlock {} request partition{} ...".format(self.ALLCNT, s), self.WARNLG)
      modcnt = 0
      for pidx in self.params['PI']:
         pgrec = self.pgget("ptrqst", "pid, lockhost", "pindex = {}".format(pidx), self.PGOPT['extlog'])
         if not pgrec:
            self.pglog("Request Paritition {}: Not exists".format(pidx), self.PGOPT['errlog'])
         elif not pgrec['pid']:
            self.pglog("Request Partition {}: Not locked".format(pidx), self.PGOPT['wrnlog'])
         elif self.lock_partition(pidx, 0, self.PGOPT['extlog']) > 0:
            modcnt += 1
            self.pglog("Request Paritition {}: Unlocked {}/{}".format(pidx, pgrec['pid'], pgrec['lockhost']), self.PGOPT['wrnlog'])
         elif (self.check_host_down(None, pgrec['lockhost']) and
               self.lock_partition(pidx, -2, self.PGOPT['extlog']) > 0):
            modcnt += 1
            self.pglog("Request Paritition {}: Force unlocked {}/{}".format(pidx, pgrec['pid'], pgrec['lockhost']), self.PGOPT['wrnlog'])
         else:
            self.pglog("Request Paritition {}: Unable to unlock {}/{}".format(pidx, pgrec['pid'], pgrec['lockhost']), self.PGOPT['wrnlog'])
      if self.ALLCNT > 1: self.pglog("{} of {} request partition{} unlocked from RDADB".format(modcnt, self.ALLCNT, s), self.LOGWRN) 

   # interrupt requests for given request indices
   def interrupt_requests(self):
      s = 's' if self.ALLCNT > 1 else ''
      delcnt = 0
      for i in range(self.ALLCNT) :
         ridx = self.params['RI'][i]
         cnd = "rindex = {}".format(ridx)
         pgrec = self.pgget("dsrqst", "dsid, pid, lockhost, status", cnd, self.PGOPT['extlog'])
         if not pgrec: self.pglog("{}: Request Index not in RDADB".format(ridx), self.PGOPT['extlog'])
         rstr = "Request {} of {}".format(ridx, pgrec['dsid'])
         if pgrec['status'] != "Q":
            self.pglog("{}: Status '{}'; must be 'Q' to interrupt".format(rstr, pgrec['status']), self.PGOPT['errlog'])
            continue
         pid = pgrec['pid']
         if pid == 0:
            self.pglog(rstr + ": Request is not under process; no interruption", self.PGOPT['wrnlog'])
            continue
         host = pgrec['lockhost']
         if host == "partition":
            pgparts = self.pgmget("ptrqst", "pindex", cnd + " AND pid > 0", self.PGOPT['extlog'])
            if pgparts:
               self.interrupt_partitions(pgparts['pindex'])
         else:
            if not self.local_host_action(host, "interrupt request", rstr, self.PGOPT['errlog']): continue
            opts = "-h {} -p {}".format(host, pid)
            buf = self.pgsystem("rdaps " + opts, self.LOGWRN, 20)   # 21 = 4 + 16
            if buf:
               ms = re.match(r'^\s*(\w+)\s+', buf)
               if ms:
                  uid = ms.group(1)
                  if uid != self.params['LN']:
                     self.pglog("{}: Must be '{}' to interrupt {}".format(self.params['LN'], uid, rstr), self.PGOPT['wrnlog'])
                     continue
                  if 'FI' not in self.params:
                     self.pglog(": locked by {}/{}; must add Mode option -FI (-ForceInterrupt) to interrupt".format(rstr, pid, host), self.PGOPT['wrnlog'])
                     continue
                  if not self.pgsystem("rdakill " + opts, self.LOGWRN, 7):
                     self.pglog("{}: Failed to interrupt Request locked by {}/{}".format(rstr, pid, host), self.PGOPT['errlog'])
                     continue
            else:
               self.pglog("{}: Request process stopped already for {}/{}".format(rstr, pid, host), self.PGOPT['wrnlog'])
            pgrec = self.pgget("dsrqst", "pid, lockhost", cnd, self.PGOPT['extlog'])
            if not pgrec['pid']:
               if self.lock_request(ridx, 1, self.PGOPT['extlog']) <= 0: continue
            elif pid != pgrec['pid'] or host != pgrec['lockhost']:
               self.pglog("{}: Request is relocked by {}/{}".format(rstr, pgrec['pid'], pgrec['lockhost']), self.PGOPT['errlog'])
               continue
         record = {'pid' : 0, 'status' : 'I'}
         if self.pgupdt("dsrqst", record, cnd, self.PGOPT['extlog']):
            pgrec = self.pgget("dscheck", "*", "oindex = {} AND command = 'dsrqst' AND otype <> 'P'".format(ridx), self.PGOPT['extlog'])
            if pgrec:
               pgrec['status'] = 'I'
               self.delete_dscheck(pgrec, None, self.PGOPT['extlog'])
            delcnt += 1
      if self.ALLCNT > 1: self.pglog("{} of {} request{} interrupted".format(delcnt, self.ALLCNT, s), self.LOGWRN)

   # interrupt request partitions for given partition indices
   def interrupt_partitions(self, pindices = None, pcnt = 0):
      if not pindices: pindices = self.params['PI']
      if not pindices: return
      if not pcnt: pcnt = len(pindices)
      s = "s" if (pcnt > 1) else ""
      dcnt = 0
      for i in range(pcnt):
         pidx = pindices[i]
         cnd = "pindex = {}".format(pidx)
         pgrec = self.pgget("ptrqst", "dsid, pid, lockhost, status", cnd, self.PGOPT['extlog'])
         if not pgrec: self.pglog("Request Paritition {}: not in RDADB".format(pidx), self.PGOPT['extlog'])
         pstr = "Request Paritition {} of {}".format(pidx, pgrec['dsid'])
         if pgrec['status'] != "Q":
            self.pglog("{}: Status '{}'; must be 'Q' to interrupt".format(pstr, pgrec['status']), self.PGOPT['errlog'])
            continue
         pid = pgrec['pid']
         if pid == 0:
            self.pglog(pstr + ": not under process; no interruption", self.PGOPT['wrnlog'])
            continue
         host = pgrec['lockhost']
         if not self.local_host_action(host, "interrupt partition", pstr, self.PGOPT['errlog']):
            continue
         opts = "-h {} -p {}".format(host, pid)
         buf = self.pgsystem("rdaps " + opts, self.LOGWRN, 20)   # 21 = 4 + 16
         if buf:
            ms = re.match(r'^\s*(\w+)\s+', buf)
            if ms:
               uid = ms.group(1)
               if uid != self.params['LN']:
                  self.pglog("{}: Must be '{}' to interrupt {}".format(self.params['LN'], uid, pstr), self.PGOPT['wrnlog'])
                  continue
               if 'FI' not in self.params:
                  self.pglog("{}: Locked by {}/{}; must add Mode option -FI (-ForceInterrupt) to interrupt".format(pstr, pid, host), self.PGOPT['wrnlog'])
                  continue
               if not self.pgsystem("rdakill " + opts, self.LOGWRN, 7):
                  self.pglog("{}: Failed to interrupt, Request Partition locked by {}/{}".format(pstr, pid, host), self.PGOPT['errlog'])
                  continue
         else:
            self.pglog("{}: Request process stopped already for {}/{}".format(pstr, pid, host), self.PGOPT['wrnlog'])
         pgrec = self.pgget("ptrqst", "pid, lockhost", cnd, self.PGOPT['extlog'])
         if not pgrec['pid']:
            if self.lock_partition(pidx, 1, self.PGOPT['extlog']) <= 0: continue
         elif pid != pgrec['pid'] or host != pgrec['lockhost']:
            self.pglog("{}: Relocked by {}/{}".format(pstr, pgrec['pid'], pgrec['lockhost']), self.PGOPT['errlog'])
            continue
         record = {'status' : 'I', 'pid' : 0}
         if (self.pgupdt("ptrqst", record, cnd, self.PGOPT['extlog']) and
             self.lock_partition(pidx, 0, self.PGOPT['extlog']) > 0):
            pgrec = self.pgget("dscheck", "*", "oindex = {} AND command = 'dsrqst' and otype = 'P'".format(pidx), self.PGOPT['extlog'])
            if pgrec:
               pgrec['status'] = 'I'
               self.delete_dscheck(pgrec, None, self.PGOPT['extlog'])
            dcnt += 1
      if pcnt > 1: self.pglog("{} of {} request partition{} interrupted".format(dcnt, pcnt, s), self.LOGWRN)

   # add request partitions
   def add_request_partitions(self):
      s = "s" if self.ALLCNT > 1 else ""
      self.pglog("Add partitions to {} Request{} ...".format(self.ALLCNT, s), self.WARNLG)
      indices = self.params['RI']
      mcnt = 0
      for i in range(self.ALLCNT):
         ridx = indices[i]
         cnd = "rindex = {}".format(ridx)
         pgrec = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
         if not pgrec: return self.pglog("can not get Request info for " + cnd, self.PGOPT['errlog'])
         if not self.cache_request_control(ridx, pgrec, 'SP'): continue
         if self.lock_request(ridx, 1, self.PGOPT['errlog']) <= 0: continue
         pcnt = self.add_one_request_partitions(ridx, cnd, pgrec)
         if not pcnt: continue   # adding partitions failed
         if pcnt > 1: mcnt += pcnt
         if self.ALLCNT > 1: continue      
   #       continue if(self.request_limit())
         if pcnt == 1 and self.finish_one_request(ridx):
            self.pglog("RQST{}: request is built after no partition added".format(ridx), self.PGOPT['wrnlog'])
      if mcnt > 1:
         msg = "{} partitions added to {} request{} Successfully by {}".format(mcnt, self.ALLCNT, s, self.PGLOG['CURUID'])
         if self.PGLOG['CURUID'] != self.params['LN']: msg += " for {}".format(self.params['LN'])
         self.pglog(msg, self.PGOPT['wrnlog'])
      return mcnt

   # unlock request and display/log error
   def request_error(self, ridx, errmsg):
      self.lock_request(ridx, 0, self.PGOPT['extlog'])
      return self.pglog(errmsg, self.PGOPT['errlog'])

   # unlock partition and display/log error
   def partition_error(self, pidx, errmsg):
      self.lock_partition(pidx, 0, self.PGOPT['extlog'])
      return self.pglog(errmsg, self.PGOPT['errlog'])

   # add partitions to one request
   def add_one_request_partitions(self, ridx, cnd, pgrqst, ptcmp = 0):
      rstat = pgrqst['status']
      rtype = pgrqst['rqsttype']
      rstr = "RQST{}-{}".format(ridx, pgrqst['dsid'])
      if pgrqst['specialist'] != self.params['LN']:
         return self.request_error(ridx, "{}: Must be '{}' to add partitions for {}".format(self.params['LN'], pgrqst['specialist'], rstr))
      if rstat != 'Q':
         if ('RS' in self.params and self.params['RS'][0] == 'Q' and
             self.pgexec("UPDATE dsrqst set status = 'Q' WHERE " + cnd, self.PGOPT['extlog'])):
            rstat = pgrqst['status'] = 'Q'
         else:
            return self.request_error(ridx, ": Status '{}', must be 'Q' to add partitions".format(rstr, rstat))
      pgcntl = self.PGOPT['RCNTL']
      if ptcmp:
         ptlimit = self.CMPLMT
         ptsize = 0
         cmd = None
      else:
         cmd = pgcntl['command']
         ptcmp = -1 if (pgrqst['tarflag'] == 'Y' or pgrqst['file_format']) and 'NP'.find(pgcntl['ptflag']) > -1 else 0
         if pgcntl['ptlimit']:
            ptlimit = self.get_partition_limit(pgcntl['ptlimit'], ptcmp)
            ptsize = 0
         else:
            ptlimit = 0
            ptsize = self.get_partition_size(pgcntl['ptsize'], ptcmp)
      if not (ptlimit or ptsize):
         if pgrqst['ptcount'] != 1: self.pgexec("UPDATE dsrqst set ptcount = 1 WHERE " + cnd, self.PGOPT['extlog'])
         return self.request_error(ridx, "{}: Not configured for partitioning by RC{}".format(rstr, pgcntl['cindex']))
      pcnt = self.pgget("ptrqst", "", cnd)
      if pcnt > 1:
         if pgrqst['ptcount'] != pcnt: self.pgexec("UPDATE dsrqst set ptcount = {} WHERE {}".format(pcnt, cnd), self.PGOPT['extlog'])
         return self.request_error(ridx, rstr + ": partitions added already")
      elif pcnt == 1:
         self.pgexec("UPDATE dsrqst set ptcount = 0 WHERE " + cnd, self.PGOPT['extlog'])
         self.pgdel("ptrqst", cnd, self.PGOPT['extlog'])
      syserr = ''
      if cmd:
         self.create_request_directory(pgrqst)   # create directory before set partitions 
         self.PGLOG['ERR2STD'] = ["Warning: "]
         self.pgsystem(cmd, self.LOGWRN, 261)   # 261 = 256 + 4 + 1
         self.PGLOG['ERR2STD'] = []
         if self.PGLOG['SYSERR']: syserr = "\n" + self.PGLOG['SYSERR']
         pgrqst = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
         if not pgrqst: return self.request_error(ridx, "{}: Error reget request record after {}{}".format(rstr, cmd, syserr))
         if pgrqst['status'] != 'Q':
            return self.request_error(ridx, "{}: status Q changed to {} after {}{}".format(rstr, pgrqst['status'], cmd,syserr))
      fields = 'findex, wfile, size, ofile'
      pgrecs = self.pgmget("wfrqst", fields, cnd + " ORDER BY wfile", self.PGOPT['extlog'])
      tcnt = len(pgrecs['wfile']) if pgrecs else 0
      if not tcnt and (syserr or pgcntl['empty_out'] != 'Y'):
         msg = "{}: NO file information found for partitioning{}".format(rstr, syserr)
         self.send_request_email_notice(pgrqst, msg, 0, 'E')
         self.pgexec("UPDATE dsrqst set status = 'E', ecount = ecount + 1 WHERE " + cnd, self.PGOPT['extlog'])
         return self.request_error(ridx, msg)
      # check and set input file size
      insize = 0
      for i in range(tcnt):
         if not pgrecs['size'][i]:
            finfo = self.check_local_file(pgrecs['wfile'][i])
            if not finfo and pgrecs['ofile'][i]:
               finfo = self.check_local_file(pgrecs['ofile'][i])
            if finfo and self.pgexec("UPDATE wfrqst SET size = {} WHERE findex = {}".format(finfo['data_size'], pgrecs['findex'][i]), self.PGOPT['extlog']):
               pgrecs['size'][i] = finfo['data_size']
         if pgrecs['size'][i]: insize += pgrecs['size'][i]
      if ptlimit:
         if tcnt <= ptlimit:
            self.pglog("{}: NO partition needed, file count {} < {}".format(rstr, tcnt, ptlimit), self.LOGWRN)
            pcnt = 1
         else:
            pcnt = int(tcnt/ptlimit)
            if pcnt > self.PGOPT['PTMAX']:
               self.pglog("{}: Too many partitions({}) for partition file count {}".format(rstr, pcnt, ptlimit), self.LOGWRN)
               ptlimit = int(tcnt/self.PGOPT['PTMAX'] + 1)
               self.pglog("{}: Increase partition file count to {} for total {}".format(rstr, ptlimit, tcnt), self.LOGWRN)
            else:
               ptlimit = int(tcnt/(int(tcnt/ptlimit)+1)+1)
            pcnt = 0
      else:
         if not insize and tcnt > 0: return self.request_error(ridx, "{}: NO size information found for partitioning{}".format(rstr, syserr))
         if insize <= ptsize:
            self.pglog("{}: NO partition needed, data size {} < {}".format(rstr, insize, ptsize), self.LOGWRN)
            pcnt = 1
         else:
            pcnt = int(insize/ptsize)
            if pcnt > self.PGOPT['PTMAX']:
               self.pglog("{}: Too many partitions({}) for partition data size {}".format(rstr, pcnt, ptsize), self.LOGWRN)
               ptsize = int(insize/self.PGOPT['PTMAX'] + 1)
               self.pglog("{}: Increase partition data size to {} for total {}".format(rstr, ptsize, insize), self.LOGWRN)
            pcnt = 0
      if pcnt == 0:   # add partitions
         addrec = {'rindex' : ridx, 'dsid' : pgrqst['dsid'], 'specialist' : pgrqst['specialist']}
         if ptcmp > 0: addrec['ptcmp'] = 'Y'
         modrec = {'status' : (self.params['PS'][0] if ('PS' in self.params and self.params['PS'][0]) else rstat)}
         pidx = 0
         for i in range(tcnt):
            if pidx == 0:
               addrec['ptorder'] = pcnt
               pcnt += 1
               pidx = self.pgadd("ptrqst", addrec, self.PGOPT['extlog']|self.AUTOID|self.DODFLT)
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
            self.pgexec("UPDATE wfrqst SET {} WHERE {}".format(pcnd, fcnd), self.PGOPT['extlog'])
            modrec['fcount'] = fcnt
            self.pgupdt("ptrqst", modrec, pcnd, self.PGOPT['extlog'])
            if pcnt == 1 and ptcmp < 1: self.add_dynamic_partition_options(pidx, pgrqst, modrec, pcnd)
            pidx = 0
         if pidx:
            fcnd += "'{}'".format(pgrecs['wfile'][tcnt-1])
            self.pgexec("UPDATE wfrqst SET {} WHERE {}".format(pcnd, fcnd), self.PGOPT['extlog'])
            modrec['fcount'] = fcnt
            self.pgupdt("ptrqst", modrec, pcnd, self.PGOPT['extlog'])
      record = {'ptcount' : pcnt, 'pid' : 0}
      if insize and insize > pgrqst['size_input']: record['size_input'] = insize
      if not pgrqst['fcount'] or pgrqst['fcount'] < 0: record['fcount'] = tcnt
      self.pgupdt("dsrqst", record, cnd, self.PGOPT['extlog'])
      if pcnt > 1: self.pglog("{}: {} partitions Added".format(rstr, pcnt), self.PGOPT['wrnlog']|self.FRCLOG)
      return pcnt

   # get the dynamic option values for a partition
   def add_dynamic_partition_options(self, pidx, pgrqst, modrec, pcnd):
      pgctl = self.get_dsrqst_control(pgrqst)
      if pgctl:
         pgptctl = {}
         for bkey in self.BOPTIONS:
            if bkey in pgctl and pgctl[bkey]:
               ms = re.match(r'^!(.+)$', pgctl[bkey])
               if ms:
                  options = self.get_dynamic_options(ms.group(1), pidx, 'P')
                  if options: pgptctl[bkey] = options
         if pgptctl:
            self.pgupdt("ptrqst", pgptctl, pcnd, self.PGOPT['extlog'])
            modrec.update(pgptctl)

   # reduce ptlimit for more partitions if compression
   def get_partition_limit(self, ptlimit, ptcmp = 0):
      if ptcmp and ptlimit > self.CMPLMT:
         ptlimit = int(ptlimit/6.0)
         if ptlimit < self.CMPLMT: ptlimit = self.CMPLMT
      return ptlimit

   # reduce ptsize for more partitions if compression
   def get_partition_size(self, ptsize, ptcmp = 0):
      minsize = 3000000000
      if ptcmp and ptsize > minsize:
         ptsize = int(ptsize/6.0)
         if ptsize < minsize: ptsize = minsize
      return ptsize

   # build requests
   def build_requests(self):
      s = "s" if self.ALLCNT > 1 else ""
      self.pglog("Build {} Request{} ...".format(self.ALLCNT, s), self.WARNLG)
      indices = self.params['RI']
      mcnt = 0
      for i in range(self.ALLCNT):
   #       break if(request_limit())   # exceed total request limit
         ridx = indices[i]
         cnd = "rindex = {}".format(ridx)
         pgrqst = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
         if not pgrqst:
            self.pglog("RQST{}: can not get Request info".format(ridx), self.PGOPT['errlog'])
            continue
         if pgrqst['ptcount'] == 0:
            if not self.cache_request_control(ridx, pgrqst, 'SP'): continue
            if self.lock_request(ridx, 1, self.PGOPT['errlog']) <= 0: continue
            pgrqst['ptcount'] = self.add_one_request_partitions(ridx, cnd, pgrqst)
            if not pgrqst['ptcount']: continue   # adding partitions failed
         if pgrqst['ptcount'] > 1:
            pidx = self.finish_one_partition(ridx, cnd)
            if pidx: self.pglog("RPT{}: procssed for Rqst{}".format(pidx, ridx), self.PGOPT['errlog'])
            mcnt += self.finish_one_request(ridx, pidx)
         else:
            if not self.cache_request_control(ridx, pgrqst, 'BR'): continue
            if self.lock_request(ridx, 1, self.PGOPT['errlog']) <= 0: continue
            mcnt += self.build_one_request(ridx, cnd, pgrqst)
      if mcnt > 1:
         msg = "{} of {} request{} built Successfully by {}".format(mcnt, self.ALLCNT, self.PGLOG['CURUID'])
         if self.PGLOG['CURUID'] != self.params['LN']: msg += " for " + self.params['LN']
         self.pglog(msg, self.PGOPT['wrnlog'])
      return mcnt

   # process request partitions
   def process_partitions(self):
      s = "s" if self.ALLCNT > 1 else ""
      self.pglog("Process {} Request Partition{} ...".format(self.ALLCNT, s), self.WARNLG)
      indices = self.params['PI']
      mcnt = 0
      for i in range(self.ALLCNT):
   #      if self.request_limit(): break   # exceed total request limit
         pidx = indices[i]
         cnd = "pindex = {}".format(pidx)
         pgpart = self.pgget("ptrqst", "*", cnd, self.PGOPT['extlog'])
         if not pgpart: return self.pglog("RPT{}: can not get Request Partition info".format(pidx), self.PGOPT['errlog'])
         ridx = pgpart['rindex']
         pgrqst = self.pgget("dsrqst", "*", "rindex = {}".format(ridx), self.PGOPT['extlog'])
         if not pgrqst: return self.pglog("RQST{}: can not get Request info".format(ridx), self.PGOPT['errlog'])
         if not self.cache_request_control(ridx, pgrqst, 'PP', pidx): continue
         if self.lock_partition(pidx, 1, self.PGOPT['errlog']) <= 0: continue
         mcnt += process_one_partition(pidx, cnd, pgpart, ridx, pgrqst)
         if self.ALLCNT == 1 and mcnt > 0 and self.finish_one_request(ridx, pidx):
            self.pglog("RQST{}: built after RPT{} is processed".format(ridx, pidx), self.PGOPT['wrnlog'])
      if mcnt > 1:
         msg = "{} of {} request partition{} processed by {}".format(mcnt, self.ALLCNT, s, self.PGLOG['CURUID'])
         if self.PGLOG['CURUID'] != self.params['LN']: msg += " for " + self.params['LN']
         self.pglog(msg, self.PGOPT['wrnlog'])
      return mcnt

   # try to finish building a request after its partions are all processed
   def finish_one_request(self, ridx, pidx = 0):
      cnd = "rindex = {}".format(ridx)
      if self.pgget('ptrqst', "", cnd + " AND status <> 'O'", self.PGOPT['extlog']): return 0   # partition not done yet
      pgrqst = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
      if not pgrqst: return self.pglog("RQST{}: can not get Request info".format(ridx), self.PGOPT['errlog'])
      if pidx: self.change_dscheck_oinfo(pidx, 'P', ridx, 'R')
      if not self.cache_request_control(ridx, pgrqst, 'BR'): return 0
      if self.lock_request(ridx, 1, self.PGOPT['errlog']) <= 0: return 0
      return self.build_one_request(ridx, cnd, pgrqst)

   #  finish one partition for a given request index
   def finish_one_partition(self, ridx, cnd):
      pgrqst = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
      if not pgrqst: return self.pglog("RQST{}: can not get Request info".format(ridx), self.PGOPT['errlog'])
      # get the first queued partition
      pgpart = self.pgget("ptrqst", "*", cnd + " AND status = 'Q' AND pid = 0 ORDER by ptorder", self.PGOPT['extlog'])
      if not pgpart: return self.pglog("RQST{}: No queued Partition found to be processed".format(ridx), self.PGOPT['wrnlog'])
      pidx = pgpart['pindex']
      self.change_dscheck_oinfo(ridx, 'R', pidx, 'P')
      if not self.cache_request_control(ridx, pgrqst, 'PP', pidx): return 0
      if self.lock_partition(pidx, 1, self.PGOPT['errlog']) <= 0: return 0
      return self.process_one_partition(pidx, "pindex = {}".format(pidx), pgpart, ridx, pgrqst)

   # build one request
   def build_one_request(self, ridx, cnd, pgrqst):
      rstat = pgrqst['status']
      rtype = pgrqst['rqsttype']
      rstr = "RQST{}-{}".format(ridx, pgrqst['dsid'])
      if pgrqst['specialist'] != self.params['LN']:
         return self.request_error(ridx, "{}: Must be '{}' to build {}".format(self.params['LN'], pgrqst['specialist'], rstr))
      if rstat != 'Q':
         if ('RS' in self.params and self.params['RS'][0] == 'Q' and
             self.pgexec("UPDATE dsrqst set status = 'Q' WHERE " + cnd)):
            rstat = pgrqst['status'] = 'Q'
            if pgrqst['ptcount'] == -1 and self.pgexec("UPDATE dsrqst set ptcount = 1 WHERE " + cnd):
               pgrqst['ptcount'] = 1
         else:
            return self.request_error(ridx, rstr + ": Status '{}', must be 'Q' to build".format(rstat))
      pgcntl = self.PGOPT['RCNTL']
      fcount = pgrqst['fcount']
      errmsg = ""
      rstat = "O"
      if pgrqst['location']:
         self.PGLOG['FILEMODE'] = 0o666
         self.PGLOG['EXECMODE'] = 0o777
      if pgrqst['ptcount'] == 0 and (pgcntl['ptlimit'] or pgcntl['ptsize']):
         return self.pglog("Set Partitions for partition-controlled request: dsrqst SP -NP -RI {}".format(ridx), self.PGOPT['errlog'])
      if pgrqst['ptcount'] < 2 or 'BF'.find(pgcntl['ptflag']) > -1:
         etime = time.time()
         cmd = pgcntl['command']
         if not (fcount or cmd or rtype == "C" and 'LF' in self.params):  # should not happen normally
            record = {'status' : 'E', 'pid' : 0}
            self.pgupdt("dsrqst", record, cnd, self.PGOPT['extlog'])
            return self.pglog("No enough information to build " + rstr, self.PGOPT['errlog'])
         if rtype == "F" or rtype == "A":
            (rstat, errmsg) = self.stage_convert_files(ridx, cnd, rstr, pgrqst, errmsg, cmd, rtype)
            cmd = None   # do not call command any more
         elif rtype == "C":
            self.stage_local_files(ridx, cnd, rstr, pgrqst)
         if rstat == 'O' and cmd:
            cret = self.call_command(ridx, cnd, cmd, rstr, pgrqst, 0, None)
            if 'pgrqst' in cret: pgrqst = cret['pgrqst']
            if 'errmsg' in cret:
               rstat = 'E'
               if cret['errmsg']: errmsg += cret['errmsg'] + "\n"
            elif self.CMPCNT > 0:
               self.CMPCNT = 0
               rstat = 'Q'
               fcount = pgrqst['fcount']
         etime = int(time.time() - etime)
      else:
         etime = 0
      cdate = self.curdate()
      ctime = self.curtime()
      if self.pgget("dsrqst", "", cnd + " AND status = 'I'"):
         rstat = 'I'
         errmsg = rstr + ": is interrupted during process\n"
      elif rstat == 'O':
         fcount = self.set_request_count(cnd, pgrqst, 1)
         pgrqst['date_ready'] = cdate
         pgrqst['time_ready'] = ctime
         pgrqst['date_purge'] = self.adddate(pgrqst['date_ready'], 0, 0, self.PGOPT['VP'])
         pgrqst['time_purge'] = pgrqst['time_ready']
      if 'NE' not in self.params and 'IQ'.find(rstat) < 0:
         if 'NO' not in self.params or errmsg:
            rstat = self.send_request_email_notice(pgrqst, errmsg, fcount, rstat, (self.PGOPT['ready'] if pgrqst['location'] else ""))
      elif errmsg:
         self.pglog(errmsg, self.PGOPT['errlog'])
      # set status and date/time
      record = {'status' : rstat, 'pid' : 0}
      if etime: record['exectime'] = etime + pgrqst['exectime']
      if rstat == 'O':
         record['date_ready'] = pgrqst['date_ready']
         record['time_ready'] = pgrqst['time_ready']
         if 'NO' in self.params:
            record['status'] = 'N'
            record['date_purge'] = record['time_purge'] = None
         else:
            record['date_purge'] = pgrqst['date_purge']
            record['time_purge'] = pgrqst['time_purge']
      else:
         self.ERRMSG += errmsg
         record['ecount'] = pgrqst['ecount'] + 1
      if self.pgupdt("dsrqst", record, cnd, self.PGOPT['extlog']) and rstat == 'O':
         if fcount > 0:
            rstr += " built successfully" 
            if 'NO' in self.params:
               rstr += ", but data not online,"
            else:
               self.purge_one_request(ridx, cdate, ctime, -1)
            rstr += " by " + self.PGLOG['CURUID']
            if self.PGLOG['CURUID'] != self.params['LN']: rstr += " for " + self.params['LN']
         else:
            rstr += " processed with No data by " + self.PGLOG['CURUID']
            if self.PGLOG['CURUID'] != self.params['LN']: rstr += " for " + self.params['LN']
         self.pglog("{} at {}".format(rstr, self.curtime(1)), self.PGOPT['wrnlog']|self.FRCLOG)
         return 1
      else:
         return 0

   # process one request partition
   def process_one_partition(self, pidx, cnd, pgpart, ridx, pgrqst):
      ret = 0
      rstat = pgpart['status']
      rtype = pgrqst['rqsttype']
      rstr = "RPT{}-RQST{}-{}".format(pidx, ridx, pgpart['dsid'])
      rcnd = "rindex = {}".format(ridx)
      if pgpart['specialist'] != self.params['LN']:
         return self.partition_error(pidx, "{}: Must be '{}' to process {}".format(self.params['LN'], pgrqst['specialist'], rstr))
      if rstat != 'Q':
         if ('PS' in self.params and self.params['PS'][0] == 'Q' and
             self.pgexec("UPDATE ptrqst set status = 'Q' WHERE " + cnd, self.PGOPT['extlog'])):
            rstat = pgpart['status'] = 'Q'
         else:
            return self.partition_error(pidx, "{}: Status '{}', must be 'Q' to process".format(rstr, rstat))
      etime = time.time()
      pgcntl = self.PGOPT['RCNTL']
      cmd = pgcntl['command']
      fcount = pgpart['fcount']
      errmsg = ""
      rstat = "O"
      if pgrqst['location']:
         self.PGLOG['FILEMODE'] = 0o666
         self.PGLOG['EXECMODE'] = 0o777
      if rtype == "F" or rtype == "A":
         (rstat, errmsg) = self.stage_convert_files(ridx, cnd, rstr, pgrqst, errmsg, cmd, rtype)
         cmd = ""   # do not call command any more
      if rstat == 'O' and cmd:
         cret = self.call_command(ridx, cnd, cmd, rstr, pgrqst, pidx, pgpart)
         if 'pgrqst' in cret: pgrqst = cret['pgrqst']
         if 'pgpart' in cret: pgpart = cret['pgpart']
         if 'errmsg' in cret:
            rstat = 'E'
            if cret['errmsg']: errmsg += cret['errmsg'] + "\n"
      etime = int(time.time() - etime)
      if self.pgget("ptrqst", "", cnd + " AND status = 'I'"):
         rstat = 'I'
         errmsg = rstr + ": is interrupted during process\n"
      if errmsg:
         if not ('NE' in self.params or rstat == "I"):
            self.send_request_email_notice(pgrqst, errmsg, fcount, rstat, '', pgpart)
         else:
            if errmsg: self.pglog(errmsg, self.PGOPT['errlog'])
      # set status and date/time
      record = {}
      record['status'] = rstat
      if etime:
         record['exectime'] = etime + pgpart['exectime']
         self.pgexec("UPDATE dsrqst SET exectime = exectime + {} WHERE {}".format(etime, rcnd), self.PGOPT['extlog'])
      if rstat != 'O': self.ERRMSG += errmsg
      if self.pgupdt("ptrqst", record, cnd, self.PGOPT['extlog']):
         if self.lock_partition(pidx, 0, self.PGOPT['extlog']) > 0 and rstat == 'O':
            rstr += " built Successfully by {}".format(self.PGLOG['CURUID'])
            if self.PGLOG['CURUID'] != self.params['LN']: rstr += " for {}".format(self.params['LN'])
            self.pglog("{} at {}".format(rstr, self.curtime(1)), self.PGOPT['wrnlog']|self.FRCLOG)
            ret = 1
         ecnt = 0
         qcnt = self.pgget('ptrqst', '', rcnd + " AND status = 'Q'", self.PGOPT['extlog'])
         if rstat == 'E':
            self.pgexec("UPDATE dsrqst SET ecount = ecount + 1 WHERE " + rcnd, self.PGOPT['extlog'])
            if not qcnt: ecnt = 1
         elif not qcnt:
            ecnt = self.pgget('ptrqst', '', rcnd + " AND status = 'E'", self.PGOPT['extlog'])
         if ecnt and self.pgget('dsrqst', '', rcnd + " AND status = 'Q'", self.PGOPT['extlog']):
            self.pgexec("UPDATE dsrqst SET status = 'E' WHERE " + rcnd, self.PGOPT['extlog'])
            self.pglog("RQST{}: SET Request Status Q to E for Failed Partition process".format(ridx), self.PGOPT['wrnlog'])
            ret = 0
      return ret

   # convert file formats and stage online for download
   def stage_convert_files(self, ridx, cnd, rstr, pgrqst, errmsg, cmd, rtype):
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
      pgfiles = self.pgmget("wfrqst", "*", cnd, self.PGOPT['extlog'])
      cnts['F'] = len(pgfiles['wfile']) if pgfiles else 0
      if cnts['F'] == 0:  # should not happen normally
         errmsg += rstr + ": No file to build"
         return ("E", errmsg)
      s = "s" if cnts['F'] > 1 else ""
      self.pglog("Convert {} file{} for {} ...".format(cnts['F'], s, rstr), self.WARNLG)
      cnts['P'] = cnts['O'] = cnts['E'] = emlcnt = 0
      self.change_local_directory(self.get_file_path(None, "data/" + pgrqst['dsid'], None, 1), self.PGOPT['extlog']|self.FRCLOG)
      ecnt = (cnts['F'] if cnts['F'] > 10 else (cnts['F']+1))
      efiles = [1]*ecnt
      if self.PGLOG['DSCHECK']:
         self.set_dscheck_fcount(cnts['F'], self.PGOPT['errlog'])
         self.set_dscheck_dcount(0, 0, self.PGOPT['errlog'])
      while True:
         for i in range(cnts['F']):
            if emlcnt == self.EMLMAX:  # skip for too many errors
               errmsg += "\n..."
               emlcnt += 1
            if not efiles[i]: continue
            fstat = 'O'
            pgrec = self.onerecord(pgfiles, i)
            wfile = pgrec['wfile']
            pstat = self.check_processed(wfile, pgrec, pgrqst['dsid'], ridx, rstr)
            if pstat > 0:
               self.pglog("{}-{}: converted already".format(pgrec['wfile'], rstr), self.PGOPT['wrnlog']|self.FRCLOG)
               doconv = 0
            elif pstat < 0:
               cnts['P'] += 1
               continue
            else:
               if rtype == 'F':
                  (wfile, msg) = self.convert_data_format(pgrec, pgrqst, cmd, rstr)
               else:
                  (wfile, msg) = self.convert_archive_format(pgrec, pgrqst, cmd, rstr)
               if msg:
                  if emlcnt < self.EMLMAX or (i+1) == cnts['F']:  errmsg += msg
                  emlcnt += 1
                  cnts['E'] += 1
                  fstat = 'E'
               elif wfile is None:
                  self.pgexec("UPDATE wfrqst SET pid = 0 WHERE findex = {}".format(pgrec['findex']), self.PGOPT['extlog'])
                  cnts['P'] += 1
                  continue
            msg = self.set_file_record(wfile, fstat, pgrec, pgfiles, cnts, i, pgrqst, pgrec['srctype'], rstr)
            if msg:
               if emlcnt < self.EMLMAX or (i+1) == cnts['F']:  errmsg += msg
               emlcnt += 1
            elif fstat == 'O':
               efiles[i] = 0
               cnts['O'] += 1
               if self.PGLOG['DSCHECK']:
                  self.add_dscheck_dcount(1, pgfiles['size'][i], self.PGOPT['errlog'])
         if cnts['P'] == 0 and (cnts['E'] == 0 or cnts['E'] >= ecnt): break
         ecnt = cnts['E'] + cnts['P']
         errmsg += self.pglog("{}: Reconvert {}/{} file{} in {} seconds".format(rstr, ecnt, cnts['F'], s, self.PGSIG['ETIME']), self.PGOPT['wrnlog']|self.FRCLOG|self.RETMSG)
         cnts['P'] = cnts['E'] = 0
         time.sleep(self.PGSIG['ETIME'])
      self.pglog("{}/{} of {} file{} staged Online/Error for {}".format(cnts['O'], cnts['E'], cnts['F'], s, rstr), self.PGOPT['wrnlog']|self.FRCLOG)
      if cnts['E'] > 0:
         errmsg += self.pglog("{}/{} file{} failed conversion for {}".format(cnts['E'], cnts['F'], s, rstr), self.PGOPT['errlog']|self.RETMSG)
         return ("E", errmsg)
      else:
         return ("O", '')

   # cp local files online, fatal if error
   def stage_local_files(self, ridx, cnd, rstr, pgrqst):
      lcnt = len(self.params['LF']) if 'LF' in self.params else 0
      if lcnt == 0: return
      rdir = self.get_file_path(None, pgrqst['rqstid'], pgrqst['location'], 1)
      self.make_local_directory(rdir, self.PGOPT['extlog'])
      # check and set web formats, group ids
      if 'DF' not in self.params: self.params['DF'] = [pgrqst['data_format']]
      if 'OT' not in self.params: self.params['OT'] = ['C']
      if 'AF' not in self.params:
         if pgrqst['file_format']:
            if 'AF' not in self.params: self.params['AF'][0] = pgrqst['file_format']
         else:
            self.set_file_format(lcnt)
      self.params['FD'] = [None]*lcnt
      self.params['FT'] = [None]*lcnt
      self.params['FS'] = ['O']*lcnt
      self.params['SZ'] = self.local_file_sizes(self.params['LF'])
      if 'WF' not in self.params:
         self.params['WF'] = [None]*lcnt
         for i in range(lcnt):
            self.params['WF'][i] = op.basename(self.params['LF'][i])
      s = "s" if (lcnt > 1) else ""
      self.pglog("Stage {} local file{} online for {} ...".format(lcnt, s, rstr), self.WARNLG)
      self.check_local_writable(self.params['WH'], "Stage Requested Data", self.PGOPT['extlog'])
      cnd = "rindex = {} AND wfile = ".format(pgrqst['rindex'])
      scnt = 0
      for i in range(lcnt):
         file = self.join_paths(rdir, self.params['WF'][i])
         info = self.check_local_file(file, 1, self.PGOPT['wrnlog'])
         if info:
            linfo = self.check_local_file(self.params['LF'][i], 0, self.PGOPT['wrnlog'])
            if not linfo:
               self.pglog(self.params['LF'][i] + ": Local file not exists", self.PGOPT['extlog'])
            elif info['data_size'] == linfo['data_size']:
               self.pglog("web:{} STAGED already at {}:{}".format(file, info['date_modified'], info['time_modified']), self.WARNLG)
               self.params['FD'][i] = info['date_modified']
               self.params['FT'][i] = info['time_modified']
               continue
         if self.local_copy_local(file, self.params['LF'][i], self.PGOPT['wrnlog']):
            info = self.check_local_file(file, 1, self.PGOPT['wrnlog'])
            if info:
               self.params['FD'][i] = info['date_modified']
               self.params['FT'][i] = info['time_modified']
               scnt += 1
      self.pglog("{} of {} file{} staged Online for {}".format(scnt, lcnt, s, rstr), self.PGOPT['wrnlog'])
      scnt = self.ALLCNT
      self.ALLCNT = lcnt
      self.set_web_files(ridx)
      self.ALLCNT = scnt

   # create a working data storage directory for a given request record
   def create_request_directory(self, pgrqst):
      rdir = self.get_file_path(None, pgrqst['rqstid'], pgrqst['location'], 1)
      self.make_local_directory(rdir, self.PGOPT['extlog'])
      if pgrqst['tarflag'] == 'Y':
         self.make_local_directory("{}/{}".format(rdir, self.PGOPT['TARPATH']), self.PGOPT['extlog'])

   #  call a command to build a customized request, such as subsetting 
   def call_command(self, ridx, cnd, cmd, rstr, pgrqst, pidx, pgpart):
      rdir = self.get_file_path(None, pgrqst['rqstid'], pgrqst['location'], 1)
      cret = {}   # a dict to hold return info for this command call
      callcmd = 1
      fields = ("findex, wfile, gindex, tindex, type, srctype, size, date, time, " +
                "status, command, disp_order, data_format, file_format, ofile, checksum")
      self.change_local_directory(rdir, self.PGOPT['extlog']|self.FRCLOG)
      if pgrqst['location'] and op.isfile(self.PGOPT['ready']): self.delete_local_file(self.PGOPT['ready'])
      cmddump = ''
      pgcntl = self.PGOPT['RCNTL']
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
            cmddump = self.pgsystem(cmd, self.PGOPT['wrnlog'], cmdopt)
            if loop < 2 and self.PGLOG['SYSERR'] and 'Connection timed out' in self.PGLOG['SYSERR']:
               time.sleep(self.PGSIG['ETIME'])
            else:
               break
         cmddump = "\nCommand dump for {}:\n{}".format(cmd, cmddump) if cmddump else ""
         if empty_out and self.PGLOG['SYSERR']: empty_out = self.check_empty_error(self.PGLOG['SYSERR'])
         if pidx:
            pgrec = self.pgget("ptrqst", "*", cnd, self.PGOPT['extlog'])
            if not pgrec:
               cret['errmsg'] = "{}: Error reget partition record{}".format(rstr, cmddump)
               return cret
            cret['pgpart'] = pgpart = pgrec   # partition record refreshed
            if pgrec['status'] not in 'OQ':
               cret['errmsg'] = "{}: status Q changed to {}{}".format(rstr, pgrec['status'], cmddump)
               return cret
         else:
            pgrec = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
            if not pgrec:
               cret['errmsg'] = "{}: Error reget request record{}".format(rstr, cmddump)
               return cret
            cret['pgrqst'] = pgrqst = pgrec   # request record refreshed
            if pgrec['status'] not in 'OQ':
               cret['errmsg'] = "{}: status Q changed to {}{}".format(rstr, pgrec['status'], cmddump)
               return cret
      pgrecs = self.pgmget("wfrqst", fields, cnd + " ORDER BY wfile", self.PGOPT['extlog'])
      fcnt = len(pgrecs['findex']) if pgrecs else 0
      if pidx or not callcmd:
         cnt = 0
      else:
         finfo = self.local_glob("*", 256)
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
            self.pgexec("UPDATE dsrqst SET ptcount = -1 WHERE " + cnd, self.PGOPT['extlog'])
      if 'NO' in self.params: return cret
      lastcmd = 0 if pidx and 'FB'.find(pgcntl['ptflag']) > -1 else 1
      rfmt = tinfo = None
      if lastcmd:
         rfmt = pgrqst['file_format']
   #      if pgrecs and rfmt and callcmd and not pidx and pgrqst['ptcount'] < 2 and fcnt > 2*self.CMPLMT:
   #         wfile = None
   #         for i in range(fcnt):
   #            pgrec = self.onerecord(pgrecs, i)
   #            if pgrec['type'] != 'D': continue
   #            cfile = wfile = pgrec['wfile']
   #            ffmt = pgrec['file_format']
   #            break
   #         if wfile:
   #            afmt = self.valid_archive_format(rfmt, ffmt)
   #            if afmt: (cfile, tmpfmt) = self.compress_local_file(wfile, afmt, 3)
   #            if cfile != wfile:
   #               self.CMPCNT = self.add_one_request_partitions(ridx, cnd, pgrqst, 1)
   #               if self.CMPCNT > 2 and self.pgexec("UPDATE dsrqst SET fcount = {}".format(fcnt), self.PGOPT['extlog']):
   #                  pgrqst['fcount'] = fcnt
   #                  return cret
   #               self.CMPCNT = 0
         if pgrqst['tarflag'] == 'Y': tinfo = self.init_tarinfo(rstr, ridx, pidx, pgrqst)
      size = progress = 0
      if self.PGLOG['DSCHECK'] and pidx and not callcmd:
         progress = int(cnt/50)
         if progress == 0: progress = 1
         self.set_dscheck_fcount(cnt, self.PGOPT['errlog'])
         self.set_dscheck_dcount(0, 0, self.PGOPT['errlog'])
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
            if emlcnt == self.EMLMAX:  # skip for too many errors
               errmsg += "\n..."
               emlcnt += 1
            if not efiles[i]: continue
            if i and progress and (i%progress) == 0:
               self.set_dscheck_dcount(i, size, self.PGOPT['extlog'])
            if pgrecs:
               pgrec = self.onerecord(pgrecs, i)
               wfile = pgrec['wfile']
            else:
               wfile = wfiles[i]
               if re.search(r'index\d*\.html',  wfile) or re.match(r'^core\.\d+$', wfile):
                  efiles[i] = 0
                  continue
               pgrec = self.pgget("wfrqst", fields, "{} '{}'".format(fcnd, wfile), self.PGOPT['extlog'])
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
                     msg = self.build_tarfile(tinfo, fidx, wfile, pgrec['size'], ffmt, pgrec['tindex'])
                     if msg:
                        if emlcnt < self.EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                        emlcnt += 1
                        ecnt += 1
                        continue
                     efiles[i] = 0
                     continue   # included in a tar file already
               elif ostat and callcmd:
                  efiles[i] = 0
                  continue
            if dtype and (rfmt or ffmt):
               afmt = self.valid_archive_format(rfmt, ffmt)
               if afmt:
                  (cfile, tmpfmt) = self.compress_local_file(wfile, afmt, 3)
                  if cfile == wfile: afmt = None
            if callcmd and ostat and not afmt:
               if tinfo and dtype:
                  msg = self.build_tarfile(tinfo, fidx, wfile, pgrec['size'], ffmt)
                  if msg:
                     if emlcnt < self.EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                     emlcnt += 1
                     ecnt += 1
                     continue
               efiles[i] = 0
               continue   # file is built via call command and no check online
            finfo = self.check_local_file(wfile, chkopt)
            if finfo:
               if ostat and not afmt and finfo['data_size'] == pgrec['size']:
                  if tinfo and dtype:
                     msg = self.build_tarfile(tinfo, fidx, wfile, pgrec['size'], ffmt)
                     if msg:
                        if emlcnt < self.EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                        emlcnt += 1
                        ecnt += 1
                        continue
                  efiles[i] = 0
                  continue   # file is built and online already
            if afmt:
               cinfo = self.check_local_file(cfile, chkopt)
               cfrec = self.pgget("wfrqst", fields, "{} '{}'".format(fcnd, cfile), self.PGOPT['extlog'])
               if cinfo and cfrec and cfrec['status'] == 'O' and cfrec['size'] == cinfo['data_size']:
                  # file compressed already use this one
                  if finfo and self.delete_local_file(wfile): ddcnt += 1
                  if fidx and self.pgdel('wfrqst', "findex = {}".format(fidx)): dfcnt += 1 
                  if tinfo:
                     msg = self.build_tarfile(tinfo, cfrec['findex'], cfile, cfrec['size'], ffmt)
                     if msg:
                        if emlcnt < self.EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                        emlcnt += 1
                        ecnt += 1
                        continue
                  efiles[i] = 0
                  continue
               elif pgrec and (finfo or fcmd):
                  if cinfo and self.delete_local_file(cfile): ddcnt += 1
                  if cfrec and self.pgdel('wfrqst', "findex = {}".format(cfrec['findex'])): dfcnt += 1
               cinfo = cfrec = None
            empty_file = empty_out
            if fcmd and not (ostat and finfo and finfo['data_size']):
               # build file if not (exist and O-status)
               fcmd = self.get_file_command(fcmd, pgrec)
               cmdopt = 304 if empty_file else 48   # 48=16+32; 304=48+256
               cmddump = self.pgsystem(fcmd, self.PGOPT['wrnlog'], cmdopt)
               cmddump = "\nCommand dump for {}:\n{}".format(fcmd, cmddump) if cmddump else ""
               if empty_file and self.PGLOG['SYSERR']: empty_file = self.check_empty_error(self.PGLOG['SYSERR'])
               pgrec = self.pgget("wfrqst", fields, "findex = {}".format(fidx), self.PGOPT['extlog'])
               if not pgrec:
                  cret['errmsg'] = "{}-{}({}): file record removed by {}".format(rstr, wfile, fidx, self.break_long_string(fcmd, 80, "...", 1))
                  return cret
               cfile = wfile = pgrec['wfile']
               ffmt = afmt = None
               if lastcmd: ffmt = pgrec['file_format']
               dtype = 1 if pgrec['type'] == 'D' else 0
               if dtype and (rfmt or ffmt):
                  afmt = self.valid_archive_format(rfmt, ffmt)
                  if afmt:
                     if afmt: (cfile, tmpfmt) = self.compress_local_file(wfile, afmt, 3)
                     if cfile == wfile: afmt = None
               finfo = self.check_local_file(wfile, chkopt)
            if not finfo:
               if finfo != None:
                  if emlcnt < self.EMLMAX or (i+1) == cnt:
                     errmsg += "\n{}-{}: Error check file under {}".format(rstr, wfile, rdir)
                  emlcnt += 1
               elif empty_file:
                  if fidx: dindices.append(fidx)
                  miscnt += 1
               else:
                  if emlcnt < self.EMLMAX or (i+1) == cnt:
                     errmsg += "\n{}-{}: File not exists under {}{}".format(rstr, wfile, rdir, cmddump)
                  emlcnt += 1
                  miscnt += 1
               ecnt += 1
               continue
            elif finfo['data_size'] == 0:
               self.delete_local_file(wfile, self.PGOPT['extlog'])
               if empty_file:
                  if fidx: dindices.append(fidx)
               else:
                  if emlcnt < self.EMLMAX or (i+1) == cnt:
                     errmsg += "\n{}-{}: File is empty under {}{}".format(rstr, wfile, rdir, cmddump)
                  emlcnt += 1
               miscnt += 1
               ecnt += 1
               continue
            if afmt:
               if self.pgsystem("rdazip -f {} {}".format(afmt, wfile), self.PGOPT['wrnlog']|self.FRCLOG, 257):  # 257=1+256
                  cinfo = self.check_local_file(cfile, chkopt)
                  if cinfo:
                     wfile = cfile
                     finfo = cinfo
                     zfmt = afmt
                     zcnt += 1
                  elif finfo != None:
                     if emlcnt < self.EMLMAX or (i+1) == cnt:
                        errmsg += "\n{}-{}: Error check file under {}".format(rstr, cfile, rdir)
                     emlcnt += 1
                     ecnt += 1
                     continue
               else:
                  if emlcnt < self.EMLMAX or (i+1) == cnt:
                     errmsg += "\n{}-{}: {}".format(rstr, cfile, (self.PGLOG['SYSERR'] if self.PGLOG['SYSERR'] else "Error rdazip " + wfile))
                  emlcnt += 1
                  ecnt += 1
                  continue
            if progress: size += finfo['data_size']
            # record request file info
            self.set_local_mode(wfile, 1, self.PGLOG['FILEMODE'], finfo['mode'], finfo['logname'])
            record = self.get_file_record(pgrec, finfo, pgrqst, wfile, i, "W")
            if record:
               if fidx:
                  mcnt += self.pgupdt("wfrqst", record, "findex = {}".format(fidx), self.PGOPT['extlog'])
               else:
                  fidx = self.pgadd("wfrqst", record, self.AUTOID|self.PGOPT['extlog'])
                  if fidx: acnt += 1
            efiles[i] = 0
            if tinfo and dtype:
               msg = self.build_tarfile(tinfo, fidx, wfile, finfo['data_size'], ffmt)
               if msg:
                  if emlcnt < self.EMLMAX or (i+1) == cnt: errmsg += "\n" + msg
                  emlcnt += 1
                  ecnt += 1
                  continue
         if ecnt == 0 or ecnt >= errcnt or tinfo: break
         errmsg += "\n" + self.pglog(("{}: {} ".format(rstr, ("Recheck" if callcmd else "Reprocess")) +
                                       "{}/{} file{} in {} seconds".format(ecnt, cnt, s, self.PGSIG['ETIME'])),
                                      self.PGOPT['wrnlog']|self.FRCLOG|self.RETMSG)
         errcnt = ecnt
         time.sleep(self.PGSIG['ETIME'])
      if zcnt > 0:
         s = "s" if zcnt > 1 else ""
         self.pglog("{} file{} {} compressed for {}".format(zcnt, s, zfmt, rstr), self.PGOPT['wrnlog']|self.FRCLOG)
      if ecnt > 0:
         errmsg += "\n" + self.pglog("{}/{} files failed for {}".format(ecnt, cnt, rstr), self.PGOPT['errlog']|self.RETMSG)
         if not (empty_out and miscnt == ecnt):
            cret['errmsg'] = errmsg
            return cret
         dcnt = 0
         for didx in dindices:
            dcnt += self.pgdel("wfrqst", "findex = {}".format(didx), self.PGOPT['extlog'])
         if dcnt > 0:
            s = "s" if dcnt > 1 else ""
            self.pglog("{} empty file record{} removed for {}".format(dcnt, s, rstr), self.PGOPT['wrnlog']|self.FRCLOG)
      if (ddcnt+dfcnt) > 0:
         s = "s" if (ddcnt+dfcnt) > 1 else ""
         self.pglog("{}/{} File/Record duplication{} removed for {}".format(ddcnt, dfcnt, s, rstr), self.PGOPT['wrnlog']|self.FRCLOG)
      s = "s" if cnt > 1 else ""
      self.pglog("{}/{} of {} file record{} Added/Modified for {}".format(acnt, mcnt, cnt, s, rstr), self.PGOPT['wrnlog']|self.FRCLOG)
      if tinfo:
         msg = self.build_tarfile(tinfo)
         if msg:
            cret['errmsg'] = "{}\n{}".format(errmsg, msg)
            return cret
         if pidx:
            if tinfo['tcnt'] != pgpart['tarcount']:
               self.pgexec("UPDATE ptrqst SET tarcount = {} WHERE {}".format(tinfo['tcnt'], cnd), self.PGOPT['extlog'])
               pgpart['tarcount'] = tinfo['tcnt']
         elif tinfo['tcnt'] != pgrqst['tarcount']:
            self.pgexec("UPDATE dsrqst SET tarcount = {} WHERE {}".format(tinfo['tcnt'], cnd), self.PGOPT['extlog'])
            pgrqst['tarcount'] = tinfo['tcnt']
      if pidx:   # check and fix partition file count
         fcnt = self.pgget("wfrqst", "", cnd, self.PGOPT['extlog'])
         if fcnt != pgpart['fcount']:
            self.pgexec("UPDATE ptrqst set fcount = {} WHERE {}".format(fcnt, cnd),  self.PGOPT['extlog'])
            pgpart['fcount'] = fcnt
      if progress: self.set_dscheck_dcount(cnt, size, self.PGOPT['extlog'])
      return cret

   # return 1 if error message is ok for empty output
   def check_empty_error(self, errmsg):
      ret = 0
      if re.search(r'ncks: ERROR Domain .* brackets no coordinate values', errmsg): ret = 1
      return ret

   # specialist specified command for each file
   def get_file_command(self, cmd, pgrec):
      if cmd.find('$') > -1: cmd = self.replace_environments(cmd, None, self.PGOPT['emlerr'])
      cmd = re.sub(r'( -OF| -WF)', ' ' + pgrec['wfile'], cmd, 1)
      cmd = re.sub(r' -FI', ' {}'.format(pgrec['findex']), cmd, 1)
      if pgrec['ofile']: cmd = re.sub(r'( -IF| -RF)', ' ' + pgrec['ofile'], cmd, 1)
      return cmd

   # intialize the tarinfo dict for tarring small files
   def init_tarinfo(self, rstr, ridx, pidx, pgrqst):
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

   # tarring small files
   def build_tarfile(self, tinfo, fidx = 0, file = None, size = 0, afmt = None, tidx = 0):
      tn = tinfo['tcnt']
      fn = tinfo['fcnt']
      if fidx:
         if size > self.MFSIZE: return None  # skip file too big to tar
         # add file info to tarinfo for tarring later
         tinfo['tsize'] += size
         tinfo['fidxs'].append(fidx)
         tinfo['files'].append(file)
         tinfo['tidxs'].append(tidx if tidx else 0)
         tinfo['tfiles'][tn]['fn'] += 1
         if afmt: tinfo['tfiles'][tn]['fmt'] = afmt
         tinfo['fcnt'] = fn + 1
         if tinfo['tsize'] < self.TFSIZE: return None  # add more files to tar
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
         if fn < self.TCOUNT: return None   # too few files to tar
         if tn > 0:
            # check if the last tar file is needed
            fn = tinfo['tfiles'][tn]['fn']
            ti = tn - 1
            if fn < self.TCOUNT or 10*tinfo['tsize'] < self.TFSIZE:
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
      elog = self.PGOPT['errlog']
      xlog = self.PGOPT['extlog']
      if not self.make_local_directory(self.PGOPT['TARPATH'], elog):
         return "{}-{}: Cannot create directory for tarring files".format(tinfo['rstr'], self.PGOPT['TARPATH'])
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
         tfile = self.join_filenames(tinfo['files'][ii], tinfo['files'][ln - 1], "-", afmt, "tar")
         tarfile = self.PGOPT['TARPATH'] + tfile
         s = "s" if fn > 1 else ""
         self.pglog("{}-{}: Tarring {} file{}...".format(tinfo['rstr'], tfile, fn, s), self.PGOPT['wrnlog']|self.FRCLOG)
         tfrec = self.pgget("tfrqst", "*", "rindex = {} AND wfile = '{}'".format(tinfo['ridx'], tfile), xlog)
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
            tindex = self.pgadd("tfrqst", tfrec, xlog|self.AUTOID)
            isotrec = 0
         tarinfo = self.check_local_file(tarfile)
         isotar = 1 if tarinfo else 0
         tfcnt = 0
         for m in range(ii, ln):
            tidx = tinfo['tidxs'][m]
            file = tinfo['files'][m]
            info = self.check_local_file(file)
            # No further action if a file is tarred and removed
            if tidx == tindex and tarinfo and not info: continue
            if tidx and tidx != tindex:
               if tidx in tinfo['otars']:
                  otarfile = tinfo['otars'][tidx]
               else:
                  record = self.pgget("tfrqst", "wfile", "tindex = {}".format(tidx), xlog)
                  if record:
                     otarfile = self.PGOPT['TARPATH'] + record['wfile']
                     if not self.check_local_file(otarfile): otarfile = ''
                  else:
                     otarfile = ''
                  # save unused tar files to delete later
                  tinfo['otars'][tidx] = otarfile
                  tinfo['otcnt'] += 1
               if not info and otarfile:
                  # try to recover missing reuqest file from old tar file
                  if not self.pgsystem("tar -xvf {} {}".format(otarfile, file), elog, copt):
                     errmsg = "{}-{}: Cannot recover tar member file {}".format(tinfo['rstr'], otarfile, file)
                     if self.PGLOG['SYSERR']: errmsg += "\n" + self.PGLOG['SYSERR']
                     return errmsg
                  info = self.check_local_file(file)
            if info:
               errmsg = None
               if tarinfo:
                  if isotar:
                     finfo = self.check_tar_file(file, tarfile)
                     if finfo and finfo['data_size'] != info['data_size']:
                        # only retar a wrong size file
                        if not self.pgsystem("tar --delete -vf {} {}".format(tarfile, file), elog, copt):
                           errmsg = "Cannot delete tar"
                        elif not self.pgsystem("tar -uvf {} {}".format(tarfile, file), elog, copt):
                           errmsg = "Cannot update tar"
                  elif not self.pgsystem("tar -uvf {} {}".format(tarfile, file), elog, copt):
                     errmsg = "Cannot update tar"
               else:
                  if not self.pgsystem("tar -cvf {} {}".format(tarfile, file), elog, copt):
                     errmsg = "Cannot create tar file for"
                  else:
                     tarinfo = self.check_local_file(tarfile, 128)
               if errmsg:
                  errmsg = "{}-{}: {} member file {}".format(tinfo['rstr'], tarfile, errmsg, file)
                  if self.PGLOG['SYSERR']: errmsg += "\n" + self.PGLOG['SYSERR']
                  return errmsg
            elif not isotar:
               return self.pglog("{}-{}: MISS requested file {} to tar".format(tinfo['rstr'], tfile, file), elog|self.RETMSG)
            if tidx != tindex:   # update file recrod
               findex = tinfo['fidxs'][m]
               self.pgexec("UPDATE wfrqst set tindex = {} WHERE findex = {}".format(tindex, findex), xlog)
            # only delete a file after it is tarred and its db record is updated
            if info: self.delete_local_file(file, elog)
            tfcnt += 1
         if tfcnt > 0:
            # reset tar file record
            tarinfo = self.check_local_file(tarfile, 1)
            record = {'size' : tarinfo['data_size'], 'date' : tarinfo['date_modified'], 'time' : tarinfo['time_modified']}
            if isotrec:
               if tfrec['pindex'] != tinfo['pidx']: record['pindex'] = tinfo['pidx']
               if tfrec['fcount'] != fn: record['fcount'] = fn
               if tfrec['data_format'] != tinfo['dfmt']: record['data_format'] = tinfo['dfmt']
               if tfrec['file_format'] != tfmt: record['file_format'] = tfmt
            self.pgupdt("tfrqst", record, "tindex = {}".format(tindex), xlog)
            if tfcnt < fn:
               self.pglog("{}-{}: Tarred {} of {} file{} to an existing {}".format(tinfo['rstr'], tfile, tfcnt, fn, s, tarfile), self.PGOPT['wrnlog']|self.FRCLOG)
      if not fidx and tinfo['otcnt'] > 0:
         # delete unused old tar file info
         for tidx in tinfo['otars']:
            if not self.pgget("wfrqst", "", "tindex = {}".format(tidx), xlog):
               self.pgdel("tfrqst", "tindex = {}".format(tidx), xlog)
               otarfile = tinfo['otars'][tidx]
               if otarfile: self.delete_local_file(otarfile, xlog)
      return None

   # get a new request file record or with fields with changed values
   def get_file_record(self, pgrec, finfo, pgrqst, wfile, i, stype):
      newrec = {}
      if pgrec:
         afmt = self.valid_archive_format(pgrqst['file_format'], pgrec['file_format'])
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

   #  Return: a file record for update
   def set_file_record(self, wfile, fstat, pgrec, pgfiles, cnts, i, pgrqst, stype, rstr):
      fmsg = "{}-{}".format(rstr, pgrec['wfile'])
      errmsg = ""
      if (fstat == 'O' and pgrec['wfile'] != wfile and not op.isfile( pgrec['wfile']) and
          not self.convert_files(pgrec['wfile'], wfile)):
         fstat = 'E'
         cnts['E'] += 1
         errmsg = "{}: error convert from {}\n".format(fmsg, wfile)
      checksum = self.get_requested_checksum(pgrqst['dsid'], pgrec)
      finfo = self.check_local_file(pgrec['wfile'], 1)
      if finfo:
         if finfo['data_size'] == 0:
            fstat = 'E'
            cnts['E'] += 1
            errmsg += fmsg + ": empty file\n"
         else:
            if checksum: finfo['checksum'] = checksum
            record = self.get_file_record(pgrec, finfo, pgrqst, None, i, stype)
      record = {'status' : fstat, 'pid' : 0}
      if self.pgupdt("wfrqst", record, "findex = {}".format(pgrec['findex']), self.PGOPT['extlog']):
         for fld in record:
            pgfiles[fld][i] = record[fld]   # record the changes
      else:
         errmsg += fmsg + ": error update wfrqst record\n"
         cnts['E'] += 1
      return errmsg

   # check and purge the requests
   def purge_requests(self):
      cdate = self.curdate()
      ctime = self.curtime()
      if self.ALLCNT > 0:
         rcnt = self.ALLCNT
         indices = self.params['RI']
      else:
         rcnd = ("specialist = '{}' AND (status = 'P' OR status = 'O') AND ".format(self.params['LN']) +
                 "(date_purge < '{}' OR date_purge = '{}' AND time_purge < '{}')".format(cdate, cdate, ctime))
         pgrecs = self.pgmget("dsrqst", "rindex", rcnd, self.PGOPT['extlog'])
         rcnt = len(pgrecs['rindex']) if pgrecs else 0
         if not rcnt:
             return self.pglog("No Request owned by '{}' due to be purged by {} {}".format(self.params['LN'], cdate, ctime), self.PGOPT['wrnlog'])
         indices = pgrecs['rindex']
      s = "s" if rcnt > 1 else ""
      self.pglog("Purge {} Request{} ...".format(rcnt, s), self.WARNLG)
      dcnt = 0
      for i in range(rcnt):
         dcnt += self.purge_one_request(indices[i], cdate, ctime, 1)
      self.pglog("{} of {} request{} Purged by '{}' at {}".format(dcnt, rcnt , s, self.params['LN'], self.curtime(1)), self.PGOPT['wrnlog'])
      return rcnt

   # purge one request
   # dppurge: <=0 record purge info only, > 0 record purge info and delete request
   def purge_one_request(self, ridx, cdate, ctime, dopurge = 0):
      cnd = "rindex = {}".format(ridx)
      pgrqst = self.pgget("dsrqst", "*", cnd, self.PGOPT['extlog'])
      if not pgrqst: return self.pglog("can not get Request info for " + cnd, self.PGOPT['errlog'])
      rstr = "Request {} of {}".format(ridx, pgrqst['dsid'])
      if self.ALLCNT > 0 and dopurge > 0:
         if pgrqst['specialist'] != self.params['LN']:
            return self.pglog("{}: Specialist '{}' to purge {}".format(self.params['LN'], pgrqst['specialist'], rstr), self.PGOPT['errlog'])
         if 'POH'.find(pgrqst['status']) < 0:
            return self.pglog("{} in Status '{}' and cannot be purged".format(rstr, pgrqst['status']), self.PGOPT['errlog'])
         elif 'FP' not in self.params: 
            pstr = ", adds Mode option -FP (-ForcePurge) to force purge"
            if pgrqst['status'] == 'O':
               pdt = '{} {}'.format(pgrqst['date_purge'], pgrqst['time_purge'])
               cdt = '{} {}'.format(cdate, ctime)
               if self.difftime(pdt, cdt) > 0:
                  return self.pglog("{} is not due for purge{}".format(rstr, pstr), self.PGOPT['errlog'])
            elif pgrqst['status'] == 'H':
               return self.pglog("{} is on Hold{}".format(rstr, pstr), self.PGOPT['errlog'])
      if pgrqst['fcount'] == None: pgrqst['fcount'] = 0
      s = "s" if pgrqst['fcount'] > 1 else ""
      if dopurge > 0:
         if self.lock_request(ridx, 1, self.PGOPT['extlog']) <= 0: return 0
         self.pglog("Purge {} with {} file{} ...".format(rstr, pgrqst['fcount'], s), self.WARNLG)
         self.check_local_writable(self.params['WH'], "Purge Requested Data", self.PGOPT['extlog'])
      if self.request_type(pgrqst['rqsttype'], 1):
         self.record_purge_files(cnd)
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
      pgrec['wuid_request'] = self.check_wuser_wuid(pgrqst['email'], pgrqst['date_rqst'])
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
      if dopurge < 0: pgrec['hostname'] = self.PGLOG['HOSTNAME']
      pgrec['quarter'] = 1
      ms = re.search(r'-(\d+)-', str(pgrqst['date_rqst']))
      if ms: pgrec['quarter'] += int((int(ms.group(1)) - 1)/3)
      if self.pgget("dspurge", "", cnd, self.PGOPT['extlog']):   # update purge request record
         ret = self.pgupdt("dspurge", pgrec, cnd, self.PGOPT['extlog'])
      else:
         pgrec['rindex'] = ridx
         ret = self.pgadd("dspurge", pgrec, self.PGOPT['extlog'])
      self.fill_request_metrics(ridx, pgrec)
      if ret:
         if dopurge > 0:
            dcnt = [0]*3
            self.delete_one_request(ridx, dcnt)
            self.pglog("{}/{} of {} request file{} purged from RDADB/Disk".format(dcnt[1], dcnt[2], pgrqst['fcount'], s), self.PGOPT['wrnlog']|self.FRCLOG)
            self.pglog("{} purged by {}".format(rstr, self.curtime(1)), self.PGOPT['wrnlog']|self.FRCLOG)
         else:
            self.pglog("{} recorded into dspurge at {}".format(rstr, self.curtime(1)), self.PGOPT['wrnlog']|self.FRCLOG)
      return ret

   # saved the purged files in tabl wfpurge
   def record_purge_files(self, cnd):
      # gather all file records for the request
      fields = "rindex, gindex, srcid, srctype, size, type, data_format, file_format, wfile"
      pgfiles = self.pgmget("wfrqst", fields, cnd, self.PGOPT['extlog'])
      fcnt = len(pgfiles['wfile']) if pgfiles else 0
      pcnt = 0
      for i  in range(fcnt):
         pgrec = self.onerecord(pgfiles, i)
         if not self.pgget("wfpurge", "", "{} AND wfile = '{}'".format(cnd, pgrec['wfile']), self.PGOPT['extlog']):
            # add purge file record only if not created yet
            pcnt += self.pgadd("wfpurge", pgrec, self.PGOPT['extlog'])
      s = "s" if fcnt > 1 else ""
      self.pglog("{} of {} request file{} recorded for usage".format(pcnt, fcnt, s), self.PGOPT['wrnlog']|self.FRCLOG)

   # modify purge date/time information
   def reset_purge_time(self):
      tname = "dsrqst"
      hash = self.TBLHASH[tname]
      s = 's' if self.ALLCNT > 1 else ''
      self.pglog("Reset purge time{} for {} request{} ...".format(s, self.ALLCNT, s), self.WARNLG)
      self.check_local_writable(self.params['WH'], "Reset Purge Time", self.PGOPT['extlog'])
      addcnt = modcnt = 0
      flds = self.get_field_keys(tname, "XY")
      self.validate_multiple_values(tname, self.ALLCNT, flds)
      for i in range(self.ALLCNT):
         ridx = self.lock_request(self.params['RI'][i], 1, self.PGOPT['extlog'])
         if ridx <= 0: continue
         cnd = "rindex = {}".format(ridx)
         pgrec = self.pgget(tname, "*", cnd, self.PGOPT['extlog'])
         if not pgrec: self.action_error("Error get Request for " + cnd)
         rstr = "Request {} of {}".format(ridx, pgrec['dsid'])
         if pgrec['specialist'] != self.params['LN']:
            self.pglog("{}: specialist '{}' to reset purge time for {}".format(self.params['LN'], pgrec['specialist'], rstr), self.PGOPT['errlog'])
            self.lock_request(ridx, 0, self.PGOPT['extlog'])
            continue
         rstat = pgrec['status']
         if rstat != 'O':
            self.pglog("Status '{}' of {}, status 'O' only to Reset Purge Time/Repulish filelist".format(rstat, rstr), self.PGOPT['errlog'])
            self.lock_request(ridx, 0, self.PGOPT['extlog'])
            continue
         record = self.build_record(flds, pgrec, tname, i)
         if record:
            record['pid'] = 0
            record['lockhost'] = ''
            if not (pgrec['fcount'] or 'fcount' in record): pgrec['fcount'] = self.set_request_count(cnd, pgrec, 1)
            if not (pgrec['date_ready'] or 'date_ready' in record): record['date_ready'] = self.curdate()
            if not (pgrec['time_ready'] or 'time_ready' in record): record['time_ready'] = self.curtime()
            if not (pgrec['date_purge'] or 'date_purge' in record):
               record['date_purge'] = self.adddate((record['date_ready'] if 'date_ready' in record else pgrec['date_ready']), 0, 0, self.PGOPT['VP'])
            if not (pgrec['time_purge'] or 'time_purge' in record):
               record['time_purge'] = record['time_ready'] if 'time_ready' in record else pgrec['time_ready']
            modcnt += self.pgupdt(tname, record, cnd, self.PGOPT['extlog'])
            if 'date_purge' in record: pgrec['date_purge'] = record['date_purge']
            if 'time_purge' in record: pgrec['time_purge'] = record['time_purge']
            if 'fcount' in record: pgrec['fcount'] = record['fcount']
         else:
            self.lock_request(ridx, 0, self.PGOPT['extlog'])
         self.PGOPT['VP'] = self.diffdate(pgrec['date_purge'], pgrec['date_ready'])
         addcnt += 1
         if 'WE' in self.params: self.send_request_email_notice(pgrec, None, pgrec['fcount'], rstat, (self.PGOPT['ready'] if pgrec['location']  else ""))
      self.pglog("{}/{} of {} request{} modified!".format(modcnt, addcnt, self.ALLCNT, s), self.PGOPT['wrnlog'])

   # get queued requests for host
   # get requests with pid values on host too if not nopid
   def get_queued_requests(self, host, nopid = 0):
      cnd = "specialist = '{}' AND status = 'Q' AND rqsttype <> 'C'".format(self.params['LN'])
      if nopid:
         cnd += " AND pid = 0 AND (hostname = '' OR hostname = '{}')".format(host)
      else:
         cnd += " AND (lockhost = '{}' OR hostname = '' AND pid = 0)".format(host)
      pgrecs = self.pgmget("dsrqst", "rindex, dsid, rqsttype, email, priority", cnd + " ORDER BY priority, rindex", self.PGOPT['extlog'])
      mcnt = len(pgrecs['rindex']) if pgrecs else 0
      if mcnt > 0:
         return self.reorder_requests(pgrecs, mcnt)
      else:
         return self.pglog("No Request Queued for '{}' on {} at {}".format(self.params['LN'], host, self.curtime(1)), self.PGOPT['wrnlog'])

   # reorder requests in a fair order
   def reorder_requests(self, pgrecs, mcnt):
      m = lcnt = ncnt = dcnt = rcnt = 0
      pgnows = pglats = pgruns = None
      while True:
         pgrec = self.onerecord(pgrecs, m)
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
            pgnows = self.addrecord(pgnows, pgrec, ncnt)
            ncnt += 1
         elif addnow < 0:
            pgruns = self.addrecord(pgruns, pgrec, rcnt)
            rcnt += 1
         else:
            pglats = self.addrecord(pglats, pgrec, lcnt)
            lcnt += 1
         if m < mcnt: continue
         if lcnt == 0: break   # all done
         mcnt = lcnt
         dcnt = ncnt
         m = lcnt = rcnt = 0
         pgrecs = pglats
         pglats = pgruns = None
      return pgnows

   # clean the data files in data/dsnnn.n dirctories that are not included in any request in RDADB
   def clean_unused_data(self):
      self.check_local_writable(self.params['WH'], "Delete Data Files for Requests Purged Already", self.PGOPT['extlog'])
      self.change_local_directory(self.join_paths(self.params['WH'], "data"), self.PGOPT['wrnlog'])
      dsids = self.params['DS'] if 'DS' in self.params else glob.glob("ds*.*")
      if not dsids: return self.pglog("Nothing to clean", self.LOGWRN)
      dcnt = acnt = 0
      for dsid in dsids:
         fcnt = self.clean_dataset_data(dsid)
         if fcnt > 0:
            acnt += fcnt
            dcnt += 1
      if acnt == 0:
         self.pglog("No unused File found", self.LOGWRN)
      else:
         s = "s" if acnt > 1 else ""
         ss = "s" if dcnt > 1 else ""
         if 'FP' in self.params:
            self.pglog("{} unused File{} cleaned for {} Dataset".format(acnt, s, dcnt, ss), self.LOGWRN)
         else:
            self.pglog("{} unused File{} found for {} Dataset{}".format(acnt, s, dcnt, ss), self.LOGWRN)
            self.pglog("Add Mode option -FP to clean the data", self.WARNLG)

   # clean unused data for one dataset
   def clean_dataset_data(self, dsid):
      files = glob.glob(dsid + "/*")
      if not files: return 0
      cnt = 0
      for file in files:
         wfile = op.basename(file)
         if self.pgget("wfrqst", "", "wfile = '{}'".format(wfile), self.LGEREX): continue
         if self.pgget("wfrqst", "", "ofile = '{}'".format(wfile), self.LGEREX): continue
         if 'FP' in self.params:
            self.pgsystem("rm -rf " + file, self.LGWNEX, 5)
         else:
            self.pglog(file + " unused", self.WARNLG)
         cnt += 1
      if cnt > 0:
         s = "s" if cnt > 1 else ""
         self.pglog("{} unused File{} {} for {}".format(cnt, s, ('cleaned' if 'FP' in self.params else 'found'), dsid), self.LOGWRN)
      return cnt

   # clean the request directories on disk that are not in RDADB
   def clean_unused_requests(self):
      self.check_local_writable(self.params['WH'], "Delete Directories for Requested Purged Already", self.PGOPT['extlog'])
      self.change_local_directory(self.params['WH'], self.PGOPT['extlog'])
      rids = self.params['RN'] if 'RN' in self.params else glob.glob("*")
      rcnt = 0
      for rid in rids:
         ms = re.match(r'^[A-Z]+(\d+)$', rid)
         if ms:
            ridx = ms.group(1)
            if self.pgget("dsrqst", "", "rindex = {}".format(ridx), self.PGOPT['extlog']): continue
            if 'FP' in self.params:
               self.pgsystem("rm -rf " + rid, self.PGOPT['extlog'], 5)
            else:
               self.pglog(rid + " unused", self.WARNLG)
            rcnt += 1
      s = "ies" if rcnt > 1 else "y"
      if 'FP' in self.params:
         self.pglog("{} unused Request Director{} cleaned".format(rcnt, s), self.LOGWRN)
      else:
         self.pglog("{} unused Request Director{} found{}".format(rcnt, s, ("; add Mode option -FP to clean" if rcnt > 0 else "")), self.WARNLG)

   # reset request file status for files are not on disk
   def reset_all_file_status(self):
      pgrecs = self.pgmget("dsrqst", 'rindex, dsid, rqstid, rqsttype', "status = 'E' AND pid = 0", self.PGOPT['extlog'])
      cnt = len(pgrecs['rindex']) if pgrecs else 0
      if not cnt: return
      self.check_local_accessible(self.params['WH'], "Reset Request File Status for Files Not Staged", self.PGOPT['extlog'])
      self.change_local_directory(self.params['WH'], self.PGOPT['extlog'])
      rcnt = mcnt = 0
      for i in range(cnt):
         pgrqst = self.onerecord(pgrecs, i)
         ridx = pgrqst['rindex']
         pgfiles = self.pgmget("wfrqst", 'findex, wfile, size', "rindex = {} AND status = 'O'".format(ridx), self.PGOPT['extlog'])
         if not pgfiles: continue
         rcnt += 1
         dpath = "data/" + pgrqst['dsid'] if self.request_type(pgrqst['rqsttype'], 1) else pgrqst['rqstid']
         mcnt += self.reset_request_file_status(ridx, dpath, pgrqst['dsid'], pgfiles)
      if mcnt == 0:
         self.pglog("No file record needs to set status to 'R' from 'O'", self.LOGWRN)
      elif rcnt > 1 and mcnt > 1 and self.params['FP']:
         self.pglog("Total {} request file records set status to 'R' from 'O'".format(mcnt), self.LOGWRN)

   # reset the status for all provided request files
   def reset_request_file_status(self, ridx, dpath, dsid, pgrecs):
      rstr = "{}-RQST{}".format(dsid, ridx)
      cnt = len(pgrecs['findex'])
      if self.check_local_file(dpath):
         s = 's' if cnt > 1 else ''
         self.pglog("{}: checking {} online file record{}...".format(rstr, cnt, s), self.WARNLG)
         mcnt = 0
         for i in range(cnt):
            pgrec = self.onerecord(pgrecs, i)
            file = self.get_file_path(pgrec['wfile'], dpath)
            info = self.check_local_file(file, 1, self.PGOPT['wrnlog'])
            if not (info and info['data_size'] == pgrec['size']):
               if 'FP' in self.params:
                  mcnt += self.pgexec("UPDATE wfrqst SET status = 'R' WHERE findex = {}".format(pgrec['findex']), self.PGOPT['extlog'])
               else:
                  mcnt += 1
      elif 'FP' in self.params:
         mcnt += self.pgexec("UPDATE wfrqst SET status = 'R' WHERE rindex = {} AND status ='O'".format(ridx), self.PGOPT['extlog'])
      else:
         mcnt = cnt
      if mcnt > 0:
         s = 's' if mcnt > 1 else ''
         if 'FP' in self.params:
            self.pglog("{}: set {} file record{} to status 'R'".format(rstr, mcnt, s), self.LOGWRN)
         else:
            self.pglog("{}: add Mode option -FP to set {} file record{} to status 'R' from 'O'".format(rstr, mcnt, s), self.WARNLG)
      return mcnt

   # clean the reuqest usage saved previously
   def clean_request_usage(self, ridx, cnd):
      pgrec = self.pgget("dspurge", "*", cnd, self.PGOPT['extlog'])
      if pgrec:
         if self.request_type(pgrec['rqsttype'], 1):
            self.pgdel("wfpurge", cnd, self.PGOPT['extlog'])
         self.pgdel("dspurge", cnd, self.PGOPT['extlog'])
         rdate = str(pgrec['date_rqst'])
         ms = re.match(r'^(\d\d\d\d)', rdate)
         atable = "allusage_{}".format(ms.group(1) if ms else 2004)
         self.pgdel("ousage", "order_number = 'r-{}'".format(ridx), self.PGOPT['extlog'])
         acnd = "email = '{}' AND method = 'R-{}' AND date = '{}' AND time = '{}'".format(
                 pgrec['email'], pgrec['rqsttype'], rdate, pgrec['time_rqst'])
         self.pgdel(atable, acnd, self.PGOPT['extlog'])
         self.pglog("Pre-recorded usage information cleaned for Request Index {}".format(ridx), self.PGOPT['wrnlog'])

   # email notice for request information
   def email_request_status(self):
      tname = 'dsrqst'
      cnd = self.get_hash_condition(tname, None, None, 1)
      ocnd = self.get_order_string((self.params['ON'] if 'ON' in self.params else "r"), tname)
      pgrecs = self.pgmget(tname, "*", cnd + ocnd, self.PGOPT['extlog'])
      self.ALLCNT = len(pgrecs['rindex']) if pgrecs else 0
      if self.ALLCNT == 0:
         return self.pglog("{}: No Request Information Found to send email for {}".format(self.PGLOG['CURUID'], cnd), self.LOGWRN)
      if self.ALLCNT > 1:
         s = 's'
         ss = "are"
      else:
         s = ''
         ss = "is"
      subject = "{} Request Record{}".format(self.ALLCNT, s)
      if 'EL' in self.params and self.ALLCNT > self.params['EL']:
         mbuf = "{} of {}".format(self.params['EL'], subject)
         self.ALLCNT = self.params['EL']
      else:
         mbuf = subject
      mbuf += " {} listed:\n".format(ss)
      pgrecs['rstat'] = self.get_request_status(pgrecs, self.ALLCNT)
      for i in range(self.ALLCNT):
         mbuf += self.build_request_message(self.onerecord(pgrecs, i))
      if 'CC' in self.params: self.add_carbon_copy(self.params['CC'])
      subject += " found"
      self.send_email(subject, self.params['LN'], mbuf)
      self.pglog("Email sent to {} With Subject '{}'".format(self.params['LN'], subject), self.LOGWRN)

   # build email message for a given request record
   def build_request_message(self, pgrec):
      msg = ("\nIndex {} of {} for {}".format(pgrec['rindex'], pgrec['dsid'], self.request_type(pgrec['rqsttype'])) +
             " by {} on {}".format(pgrec['email'], pgrec['date_rqst']))
      if pgrec['status'] == 'O' or pgrec['status'] == 'H' and pgrec['date_ready']:
         msg += "\nCurrent status {} at {}/#dsrqst/{}/,".format(pgrec['rstat'], self.PGLOG['DSSURL'], pgrec['rqstid'])
         if pgrec['date_ready'] and pgrec['date_ready'] != pgrec['date_rqst']:
            msg += "built by {}, ".format(pgrec['date_ready'])
            msg += "with data sizes {} (out) / {} (in)".format(self.format_float_value(pgrec['size_request']),
                                                               self.format_float_value(pgrec['size_input']))
      else:
         msg += ", current status {}\n".format(pgrec['rstat'])
      return msg

   # restore self.ALLCNT purged requests for reprocessing
   def restore_requests(self):
      s = "s" if self.ALLCNT > 1 else ""
      self.pglog("Restore {} Request{} ...".format(self.ALLCNT, s), self.WARNLG)
      pcnt = 0
      for i in range(self.ALLCNT):
         pcnt += self.restore_one_request(self.params['RI'][i])
      self.pglog("{} of {} request{} retored at {}".format(pcnt, self.ALLCNT, s, self.curtime(1)), self.PGOPT['wrnlog'])

   # restore a purge request
   def restore_one_request(self, ridx):
      cnd = "rindex = {}".format(ridx)
      if self.pgget("dsrqst", "", cnd, self.PGOPT['extlog']):
         return self.pglog("RQST{}: not purged yet".format(ridx), self.PGOPT['errlog'])
      pgrqst = self.pgget("dspurge", "*", cnd, self.PGOPT['extlog'])
      if not pgrqst:
         return self.pglog("RQST{}: No purge info found".format(ridx), self.PGOPT['errlog'])
      rstr = "RQST{} of {}".format(ridx, pgrqst['dsid'])
      if pgrqst['specialist'] != self.params['LN']:
         return self.pglog("{}: Specialist '{}' to restore {}".format(self.params['LN'], pgrqst['specialist'], rstr), self.PGOPT['errlog'])
      if self.request_type(pgrqst['rqsttype'], 1):   # restore file records
         pgfiles = self.pgmget("wfpurge", "*", cnd, self.PGOPT['extlog'])
         fcnt = len(pgfiles['wfile']) if pgfiles else 0
         if fcnt > 0:
            cnt = 0
            for i in range(fcnt):
               pgrec = self.onerecord(pgfiles, i)
               pgrec = self.web_request_file(pgrec, pgrqst['dsid'])
               if not pgrec: continue
               pgrec['status'] = "R"
               cnt += self.pgadd("wfrqst", pgrec, self.PGOPT['extlog'])
            s = "s" if cnt > 1 else ""
            self.pglog("{} file record{} restored for {}".format(cnt, s, rstr), self.PGOPT['wrnlog'])
            self.pgdel("wfpurge", cnd, self.PGOPT['extlog'])   # clean purged file records
      # restore request record
      pgrec = {}
      pgrec['size_request'] = pgrqst['size_request']
      pgrec['size_input'] = pgrqst['size_input']
      pgrec['fcount'] = pgrqst['fcount']
      pgrec['status'] = self.params['RS'][0] if ('RS' in self.params and self.params['RS'][0]) else "W"
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
      unames = self.get_ruser_names(pgrec['email'])
      if unames:
         lname = self.convert_chars(unames['lstname'], 'RQST')
         pgrec['rqstid'] = '{}{}'.format(lname.upper(), pgrec['rindex'])
      if((self.cache_request_control(ridx, pgrec, self.PGOPT['CACT'], 0) and 
         (self.PGOPT['RCNTL']['ptlimit'] or self.PGOPT['RCNTL']['ptsize']))):
         pgrec['ptcount'] = 0
      return self.pgadd("dsrqst", pgrec, self.PGOPT['extlog'])

   # recreate a web request file 
   def web_request_file(self, record, dsid):
      pgrec = self.pgget_wfile(dsid, "wfile, data_size size, data_format, file_format",
                               "wid = {}".format(record['srcid']), self.PGOPT['extlog'])
      if not pgrec: return None
      # source file information
      record = {}
      record['ofile'] = op.basename(pgrec['wfile'])
      record['srctype'] = "W"
      record['size'] = pgrec['size']
      record['data_format'] = pgrec['data_format']
      record['file_format'] = pgrec['file_format']
      return record

# main function to excecute this script
def main():
   object = DsRqst()
   object.read_parameters()
   object.start_actions()
   object.pgexit(0)

# call main() to start program
if __name__ == "__main__": main()
