###############################################################################
#     Title : PgRqst.py
#    Author : Zaihua Ji,  zjiucar.edu
#      Date : 09/19/2020
#             2025-02-10 transferred to package rda_python_dsrqst from
#             https://github.com/NCAR/rda-shared-libraries.git
#             2025-12-12 convert to class PgRqst
#   Purpose : python library module for holding some common variables and
#             functions for dsrqst utility
#    Github : https://github.com/NCAR/rda-python-dsrqst.git
# 
###############################################################################
import os
import re
import time
import glob
from os import path as op 
from rda_python_common.pg_split import PgSplit
from rda_python_common.pg_cmd import PgCMD
from rda_python_common.pg_opt import PgOPT

class PgRqst(PgOPT, PgCMD, PgSplit):

   def __init__(self):
      super().__init__()  # initialize parent class
      self.CORDERS = {}
      self.OPTS.update({                         # (!= 0) - setting actions
         'BR' : [0x00000010, 'BuildRequest',   1], 
         'PR' : [0x00000020, 'PurgeRequest',   1], # clean missed requested files too
         'PP' : [0x00000040, 'ProcessPartition',  4], 
         'DL' : [0x00000080, 'Delete',         1], # delete requests, files, controls or partitions
         'CR' : [0x00000100, 'CleanRequest',   1], # clean update reuqest directory and file records if not type M
         'GR' : [0x00000200, 'GetRequest',     0],
         'GF' : [0x00000400, 'GetFile',        0],
         'GC' : [0x00000800, 'GetControl',     0],
         'GP' : [0x00001000, 'GetPartition',   0],
         'SR' : [0x00002000, 'SetRequest',     1],
         'SF' : [0x00004000, 'SetFile',        1],
         'SC' : [0x00008000, 'SetControl',     1],
         'SP' : [0x00010000, 'SetPartition',   4],
         'RP' : [0x00020000, 'ResetPurge',     1],
         'UL' : [0x00040000, 'UnLock',         1],
         'IR' : [0x00080000, 'InterruptRequest', 1],
         'IP' : [0x00100000, 'InterruptParition', 4],
         'RR' : [0x00200000, 'RestoreRequest',   1],
         'ER' : [0x00800000, 'EmailRequest',   0],
         'GT' : [0x04000000, 'GetTarfile',     0],
         'ST' : [0x08000000, 'SetTarfile',     1],
         'AW' : [0, 'AnyWhere',      0],
         'BG' : [0, 'BackGround',    0],
         'CS' : [0, 'CheckStatus',   0],
         'FO' : [0, 'FormatOutput',  0],
         'FI' : [0, 'ForceInterrrupt', 0],
         'FP' : [0, 'ForcePurge',    0],
         'GU' : [0, 'GetUsage',      0],
         'GZ' : [0, 'GMTZone',       0],
         'MD' : [0, 'PgDataset',     2],
         'NC' : [0, 'NewControl',    0],   # for SC, allow adding new request controls
         'NE' : [0, 'NoEmail',       0],
         'NO' : [0, 'NotOnline',     0],
         'NP' : [0, 'NewPartition',  0],   # for SP, allow adding new request partitions
         'NR' : [0, 'NewRequest',    0],   # for SR, allow adding new requests
         'RO' : [0, 'ResetOrder',    2],
         'UD' : [0, 'UnusedData',    2],
         'UF' : [0, 'UnstagedFile',  2],
         'UR' : [0, 'UnusedRequest', 2],
         'WD' : [0, 'WithDataset',   0],
         'WE' : [0, 'WithEmail',     0],
         'DV' : [1, 'Divider',       1],  # default to <:>
         'EL' : [1, 'EmailLimit',    1],  # default to 20
         'ES' : [1, 'EqualSign',     1],  # default to <=>
         'FN' : [1, 'FieldNames',    0],
         'LN' : [1, 'LoginName',     1],
         'OF' : [1, 'OutputFile',    0],
         'ON' : [1, 'OrderNames',    0],
         'AO' : [1, 'ActOption',     1],  # default to <!>
         'TS' : [1, 'totalSize',    17],
         'WH' : [1, 'WebHomeDir',    1],
         'AF' : [2, 'ArchiveFormat', 1],
         'BP' : [2, 'BatchProcess',  0, ''],
         'CC' : [2, 'CarbonCopy',    0],
         'CI' : [2, 'ControlIndex', 17],
         'CM' : [2, 'ControlMode',   1, "SA"],  # A-auto, S-specialist-control
         'DB' : [2, 'Debug',         0],
         'DE' : [2, 'Description',  64],
         'DF' : [2, 'DataFormat',    1],
         'DO' : [2, 'DisplayOrder', 16],
         'DP' : [2, 'DatePurge',   256],
         'DQ' : [2, 'DateRequest', 256],
         'DR' : [2, 'DateReady',   256],
         'DS' : [2, 'Dataset',       1],
         'EM' : [2, 'Email',         1],
         'EN' : [2, 'EmailNotice',   1],
         'EO' : [2, 'EmptyOutput',   1],
         'EV' : [2, 'Environments',  1],
         'FC' : [2, 'FileCount',    16],
         'FD' : [2, 'FileDate',    257],
         'FS' : [2, 'FileStatus',    1, "ROE"],
         'FT' : [2, 'FileTime',     33],
         'GI' : [2, 'GroupIndex',   17],
         'HN' : [2, 'HostName',      1],
         'IF' : [2, 'InputFile',     0],
         'LF' : [2, 'LocalFile',     0],
         'LM' : [2, 'RequestLimit', 17],
         'MO' : [2, 'Modules',       1],
         'MP' : [2, 'MaxPeriod',     1],
         'MR' : [2, 'MaxRequest',    1],
         'OB' : [2, 'OrderBy',       0],
         'OR' : [2, 'ORiginFile',    0],
         'OT' : [2, 'SourceType',    1, 'CMWDOS'],  # M-MSS or W-Web
         'PC' : [2, 'ProcessCommand',  0],
         'PF' : [2, 'PartitionFlag',   1],
         'PI' : [2, 'PartitionIndex', 16],
         'PL' : [2, 'PartitionLimit', 17],
         'PO' : [2, 'Priority',       17],
         'PS' : [2, 'PartitionStatus',  1, "WQPONHIE"],
         'PW' : [2, 'PurgeWait',      1],
         'PZ' : [2, 'PartitionsiZe', 17],
         'QS' : [2, 'QSubOptions',    0],
         'RF' : [2, 'RequestInfo',    1],
         'RI' : [2, 'RequestIndex',  17],
         'RL' : [2, 'RequestLocation',  1],
         'RN' : [2, 'RequestName',   1],
         'RS' : [2, 'RequestStatus', 1,  "WQPONHIE"],
         'RT' : [2, 'RequestType',   1,  "ACDFHMNQRST"],
         'SI' : [2, 'SizeInput',    16],
         'SL' : [2, 'SourceLink',    0],
         'SN' : [2, 'Specialist',    1],
         'SQ' : [2, 'SizeRequest',  16],
         'SZ' : [2, 'Size',         16],
         'TA' : [2, 'TarFlag',       1],
         'TF' : [2, 'ToFormat',      1],
         'TI' : [2, 'TarfileIndex', 16],
         'TP' : [2, 'TimePurge',    32],
         'TQ' : [2, 'TimeRequest',  32],
         'TR' : [2, 'TimeReady',    32],
         'UA' : [2, 'URL',           0],
         'VP' : [2, 'ValidPeriod',   1],
         'WF' : [2, 'WebFile',       0],
         'WT' : [2, 'WebFileType',   0],
      })
      self.ALIAS.update({
         'BG' : ['b'],
         'BP' : ['d', 'DelayedMode'],
         'BR' : ['ProcessRequest'],
         'CM' : ['ControlMethod'],
         'CS' : ['CheckReqeustStatus', 'CheckParitionStatus'],
         'DE' : ['Desc', 'Note'],
         'DF' : ['ContentFormat', 'InternalFormat'],
         'DL' : ['RM', 'Remove'],
         'DS' : ['Dsid', 'DatasetID'],
         'DV' : ['Delimiter', 'Separator'],
         'EM' : ['RequestEmail', 'RequestUserEmail'],
         'EV' : ['Envs'],
         'GZ' : ['GMT', 'GreenwichZone', 'UTC'],
         'HN' : ['HostMachine'],
         'IR' : ['InterRupt'],
         'LF' : ['LocFile'],
         'LM' : ['UpLimit'],
         'MO' : ['Mods'],
         'MP' : ['MaxrequestPeriod'],
         'MR' : ['MaximumRequest'],
         'OB' : ['OrderByPattern'],
         'PC' : ['Command', 'SpecialCommand'],
         'QS' : ['PBSOptions'],
         'RF' : ['RequestInformation'],
         'RL' : ['RequestHome', 'RequestPath'],
         'RN' : ['RequestID'],
         'RO' : ['Reorder'],
         'RP' : ['ResetPurgeTime', 'RePublish'],
         'SL' : ['SourceID'],
         'TF' : ['OutputFormat', 'ProductFormat'],
         'UA' : ['URLAddress', 'URLLink'],
         'UL' : ['UnLockRequest', 'UnLockParition'],
         'UZ' : ['Uncompress', 'UncompressData', 'Unzip'],
         'VP' : ['DataValidPeriod'],
         'WH' : ['WebHome', 'DownloadHome', 'OnlineHome'],
      })
      # single letter short names for option 'FN' (Field Names) to retrieve info
      # from RDADB only the fields can be manipulated by this application are listed
      #  SHORTNM KEYS(self.OPTS) DBFIELD
      self.TBLHASH['dsrqst'] = {           # condition flag, 0-int, 1-string, -1-exclude
         'R' : ['RI', "rindex",       0],
         'Q' : ['RN', "rqstid",       1],      # request directory, can only be auto set
         'E' : ['EM', "email",        1],
         'B' : ['DS', "dsid",         1],
         'I' : ['GI', "gindex",       0],
         'T' : ['RT', "rqsttype",     1],
         'O' : ['SQ', "size_request", 0],
         'W' : ['SI', "size_input",   0],
         'C' : ['FC', "fcount",       0],
         'J' : ['DQ', "date_rqst",    1],
         'K' : ['TQ', "time_rqst",   -1],
         'U' : ['DR', "date_ready",   1],
         'V' : ['TR', "time_ready",  -1],
         'X' : ['DP', "date_purge",   1],
         'Y' : ['TP', "time_purge",  -1],
         'A' : ['RS', "status",       1],
         'G' : ['TA', "tarflag",      1],
         'P' : ['PO', "priority",     0],
         'N' : ['DF', "data_format",  1],
         'M' : ['AF', "file_format",  1],
         'S' : ['SN', "specialist",   1],
         'L' : ['RL', "location",     1],
         'Z' : ['EN', "enotice",      1],
         'H' : ['HN', "hostname",     1],
         'D' : ['DE', "note",         1],
         'F' : ['RF', 'rinfo',        1]
      }
      self.TBLHASH['wfrqst'] = {
         'F' : ['WF', "wfile",          1],
         'R' : ['RI', "wfrqst.rindex",  0],
         'P' : ['PI', "wfrqst.pindex",  0],
         'T' : ['TI', "tindex",         0],
         'I' : ['GI', "wfrqst.gindex",  0],
         'B' : ['DS', "dsrqst.dsid",    1],
         'L' : ['SL', "srcid",         -1],
         'Z' : ['OT', "srctype",        1],
         'Y' : ['WT', "type",           1],
         'S' : ['SZ', "size",           0],
         'C' : ['PC', "wfrqst.command",        1],
         'M' : ['AF', "wfrqst.file_format",    1],
         'N' : ['DF', "wfrqst.data_format",    1],
         'O' : ['DO', "disp_order",    -1],
         'A' : ['FS', "wfrqst.status",  1],
         'J' : ['FD', "date",           1],
         'K' : ['FT', "time",           1],
         'D' : ['DE', "wfrqst.note",    1],
      }
      self.TBLHASH['tfrqst'] = {
         'T' : ['TI', "tindex",         0],
         'F' : ['WF', "wfile",          1],
         'R' : ['RI', "tfrqst.rindex",  0],
         'P' : ['PI', "tfrqst.pindex",  0],
         'I' : ['GI', "tfrqst.gindex",  0],
         'B' : ['DS', "dsrqst.dsid",    1],
         'S' : ['SZ', "size",           0],
         'M' : ['AF', "tfrqst.file_format",    1],
         'N' : ['DF', "tfrqst.data_format",    1],
         'O' : ['DO', "disp_order",    -1],
         'J' : ['FD', "date",           1],
         'K' : ['FT', "time",           1],
         'D' : ['DE', "tfrqst.note",    1],
      }
      self.TBLHASH['rcrqst'] = {
         'C' : ['CI', "cindex",         0],
         'T' : ['DS', "dsid",           1],
         'I' : ['GI', "rcrqst.gindex",  0],
         'R' : ['RT', "rqsttype",       1],
         'W' : ['CM', "control",        1],
         'V' : ['VP', "validperiod",    0],
         'J' : ['LM', "rqstlimit",      1],
         'K' : ['MP', "maxperiod",      1],
         'L' : ['PL', "ptlimit",        0],
         'Z' : ['PZ', "ptsize",         0],
         'F' : ['PF', "ptflag",         1],
         'D' : ['DF', "data_format",    1],
         'A' : ['AF', "file_format",    1],
         'X' : ['TF', "to_format",      1],
         'G' : ['TA', "tarflag",        1],
         'S' : ['SN', "specialist",     1],
         'P' : ['PC', "command",        1],
         'N' : ['MR', "maxrqst",        0],
         'O' : ['EO', "empty_out",      1],
         'U' : ['UA', "url",            1],
         'H' : ['HN', "hostname",       1],
         'M' : ['MO', "modules",        1],
         'B' : ['EV', "environments",   1],
         'Q' : ['QS', "qoptions",       1],
         'E' : ['EN', "enotice",        1],
         'Y' : ['CC', "ccemail",        1],
      }
      self.TBLHASH['ptrqst'] = {
         'P' : ['PI', "pindex",         0],
         'R' : ['RI', "rindex",         0],
         'B' : ['DS', "dsid",           1],
         'A' : ['PS', "status",         1],
         'O' : ['DO', "ptorder",       -1],
         'C' : ['FC', "fcount",         0],
         'S' : ['SN', "specialist",     1],
      }
      #default fields for getting info
      self.PGOPT['dsrqst'] = "REBITOCJUXAGS"
      self.PGOPT['wfrqst'] = "FRPTYSMNA"
      self.PGOPT['tfrqst'] = "TFRPSMN"
      self.PGOPT['rcrqst'] = "CTIRWGSPOUH"
      self.PGOPT['ptrqst'] = "PRBAOCS"
      #all fields for getting info
      self.PGOPT['dsall'] = "RQEBITOWCJKUVXYAGNMPSLZHDF"
      self.PGOPT['wfall'] = "FRPTILZYSCMNOAJKD"
      self.PGOPT['tfall'] = "TFRPISMNOJKD"
      self.PGOPT['rcall'] = "CTIRWVJKLZFDAXGSPNOUHMBQEY"
      self.PGOPT['ptall'] = "PRBAOCS"
      self.PGOPT['derr'] = ''
      self.PGOPT['ready'] = "request_ready.txt"
      # set default options
      self.PGOPT['DVP'] = self.PGOPT['VP'] = 5     # in days
      self.PGOPT['FLMT'] = 1000 
      self.PGOPT['PTMAX'] = 24    # max number of partitions for a signle request
      self.PGOPT['TARPATH'] = "TarFiles/"
      # set default parameters
      self.PGOPT['DTS'] = self.PGOPT['TS'] = 90000  # total size of all downloads, in GB
      self.params['WH'] = self.PGLOG['RQSTHOME']

   # check if enough information entered on command line and/or input file
   # for given action(s)
   def check_enough_options(self, cact):
      errmsg = [
         "Miss Request Index per -RI(-RequestIndex)",
         "Miss Online File Name per -WF(-WebFile)",
         "Miss Dataset ID per -DS(-Dataset) for new request",
         "Do not specify File Names for processing Requests",
         "Miss Partition Index per -PI(-PartitionIndex)",
         "Need Dataset ID per -DS and Request Type per -RT for new request control",
         "Miss Source type per -OT(-SourceType) to identify Source File Names per -SL(-SourceLink)",
         "Miss order field name string per option -ON (-OrderNames) for Re-ordering",
         "Miss Request Index per -RI(-RequestIndex) or Partition Index per -PI(-PartitionIndex)",
         "Miss Request Index per -RI(-RequestIndex) to add new Partitions",
         "Miss Control Index per -CI(-ControlIndex) for setting Source File",
         "Miss Tar File Index or Name per -TI(-TarfileIndex) or -WF(-WebFile)",
         "12",
      ]
      erridx = -1
      if ('RR' not in self.params and ('RI' in self.params or
          ('RN' in self.params and not ('DL' in self.params['DL'] and 'UR' in self.params['UR'])))):
         self.validate_requests()
      if 'DS' in self.params: self.validate_datasets()
      if cact == 'SC':
         self.validate_controls()
         if 'NC' in self.params and not ('DS' in self.params and 'RT' in self.params):
            erridx = 5
      elif self.OPTS[cact][2] == 4:
         if 'PI' in self.params: self.validate_partitions()
         if 'NP' in self.params:
            if 'RI' not in self.params: erridx = 9
         elif 'PI' not in self.params:
            erridx = 4
      elif self.OPTS[cact][2] > 0:
         if 'RI' not in self.params:
            if cact == 'UL':
               if 'PI' not in self.params: erridx = 8
            elif cact != 'DL' or not ('CI' in self.params or 'UD' in self.params or 'UR' in self.params or 'UF' in self.params):
               erridx = 0
         elif cact == 'SF':
            if not ('WF' in self.params or 'ON' in self.params):
               erridx = (7 if 'RO' in self.params else 1)
            elif 'SL' in self.params and 'OT' not in self.params:
               erridx = 6
         elif cact == 'SR':
            if 'NR' in self.params and 'DS' not in self.params:
               erridx = 2
         elif cact == 'ST':
            if not ('TI' in self.params or 'WF' in self.params or 'ON' in self.params):
               erridx = (11 if 'RO' in self.params else 1)
         elif 'WF' in self.params and (self.PGOPT['ACTS']&self.OPTS['HR'][0]):
            erridx = 3
      elif 'CI' in self.params and cact == 'GC':
         self.validate_controls()
      elif cact == 'GF' or cact == 'GT':
         if not ('PI' in self.params or 'RI' in self.params):
            erridx = 8
      if erridx >= 0:
         self.action_error(errmsg[erridx], cact)
      self.set_uid("dsrqst")
      if 'BP' in self.params:
         if 'DM' in self.params: self.params['DM'] = None
         oidx = 0
         otype = ''
         if self.OPTS[cact][2] == 4 and 'PI' in self.params:
            oidx = self.params['PI'][0]
            otype = 'P'
         elif 'RI' in self.params:
            oidx = self.params['RI'][0]
            otype = 'R'
         # set command line Batch options
         self.set_batch_options(self.params, 2, 1)
         self.init_dscheck(oidx, otype, "dsrqst", self.get_dsrqst_dataset(), cact,
                      ("" if 'AW' in self.params else self.PGLOG['CURDIR']), self.params['LN'], self.params['BP'], self.PGOPT['extlog'])
      if 'VP' in self.params: self.PGOPT['VP'] = self.params['VP'][0]
      self.start_none_daemon('dsrqst', cact, self.params['LN'], 1, 10, 1, 1)

   # get the associated dataset id
   def get_dsrqst_dataset(self):
      if 'DS' in self.params: return self.params['DS'][0]
      if 'RI' in self.params and self.params['RI'][0]:
         pgrec = self.pgget("dsrqst", "dsid", "rindex = {}".format(self.params['RI'][0]), self.PGOPT['extlog'])
         if pgrec: return pgrec['dsid']
      return None

   # get continue display order of an archived data file of given dataset (and group)
   def get_next_disp_order(self, idx = 0, table = None):
      if not idx:
         self.CORDERS = {}  # reinitial lize cached display orders
         return
      elif not table:
         self.CORDERS[idx] = 0
         return
      fld = ('cindex' if table == 'sfrqst' else 'rindex')
      if idx not in self.CORDERS:
         pgrec = self.pgget(table, "max(disp_order) max_order", "{} = {}".format(fld, idx), self.PGOPT['extlog'])
         self.CORDERS[idx] = pgrec['max_order'] if pgrec and pgrec['max_order'] else 0
      self.CORDERS[idx] += 1
      return self.CORDERS[idx]

   # reorder the files for request
   def reorder_request_files(self, onames):
      tname = "wfrqst"
      rcnt = len(self.params['RI'])
      hash = self.TBLHASH[tname]
      self.pglog("Reorder request files ...", self.PGOPT['wrnlog'])
      flds = self.append_order_fields(onames, "RO", tname)
      fields = "disp_order, "
      if onames.find('F') < 0: fields += "wfile, "
      fields +=  self.get_string_fields(flds, tname)
      if 'OB' in self.params or re.search(r'L', onames, re.I):
         ocnd = ''
      else:
         ocnd = self.get_order_string(onames, tname, "R")
      changed = 0
      for i in range(rcnt):
         rindex = self.params['RI'][i]
         if i > 0 and rindex == self.params['RI'][i-1]: continue  # sorted already
         rcnd = "rindex = {}".format(rindex)
         pgrecs = self.pgmget(tname, fields, rcnd + ocnd, self.PGOPT['extlog'])
         cnt = len(pgrecs['wfile']) if pgrecs else 0
         if not ocnd and cnt > 1: pgrecs = self.sorthash(pgrecs, flds, hash)
         record = {}
         for j in range(cnt):
            if (j+1) != pgrecs['disp_order'][j]:
               record['disp_order'] = j + 1
               changed += self.pgupdt(tname, record, "{} AND wfile = '{}'".format(rcnd, pgrecs['wfile'][j]), self.PGOPT['extlog'])
      s = 's' if changed > 1 else ''
      self.pglog("{} request file record{} reordered!".format(changed, s), self.PGOPT['wrnlog'])
      return changed

   # reorder the tar files for request
   def reorder_tar_files(self, onames):
      tname  = "tfrqst"
      rcnt = len(self.params['RI'])
      hash = self.TBLHASH[tname]
      self.pglog("Reorder tar files ...", self.PGOPT['wrnlog'])
      flds = self.append_order_fields(onames, "RO", tname)
      fields = "disp_order, "
      if onames.find('F') < 0: fields += "wfile, "
      fields +=  self.get_string_fields(flds, tname)
      if 'OB' in self.params:
         ocnd = ''
      else:
         ocnd = self.get_order_string(onames, tname, "R")
      changed = 0
      for i in range(rcnt):
         rindex = self.params['RI'][i]
         if i > 0 and rindex == self.params['RI'][i-1]: continue  # sorted already
         rcnd = "rindex = {}".format(rindex)
         pgrecs = self.pgmget(tname, fields, rcnd + ocnd, self.PGOPT['extlog'])
         cnt = len(pgrecs['wfile']) if pgrecs else 0
         if not ocnd and cnt > 1: pgrecs = self.sorthash(pgrecs, flds, hash)
         record = {}
         for j in range(cnt):
            if (j+1) != pgrecs['disp_order'][j]:
               record['disp_order'] = j + 1
               changed += self.pgupdt(tname, record, "{} AND wfile = '{}'".format(rcnd, pgrecs['wfile'][j]), self.PGOPT['extlog'])
      s = 's' if changed > 1 else ''
      self.pglog("{} tar file record{} reordered!".format(changed, s), self.PGOPT['wrnlog'])
      return changed

   # reorder the source files
   def reorder_source_files(self, onames):
      tname = "sfrqst"
      ccnt = len(self.params['CI'])
      hash = self.TBLHASH[tname]
      self.pglog("Reorder source files ...", self.PGOPT['wrnlog'])
      flds = self.append_order_fields(onames, "CO", tname)
      fields = "disp_order, "
      if onames.find('F') < 0: fields += "wfile, "
      fields +=  self.get_string_fields(flds, tname)
      if 'OB' in self.params or re.search(r'L', onames, re.I):
         ocnd = ''
      else:
         ocnd = self.get_order_string(onames, tname, "C")
      changed = 0
      for i in range(ccnt):
         cindex = self.params['CI'][i]
         if i > 0 and cindex == self.params['CI'][i-1]: continue  # sorted already
         ccnd = "cindex = {}".format(cindex)
         pgrecs = self.pgmget(tname, fields, ccnd + ocnd, self.PGOPT['extlog'])
         cnt = len(pgrecs['wfile']) if pgrecs else 0
         if not ocnd and cnt > 1: pgrecs = self.sorthash(pgrecs, flds, hash)
         record = {}
         for j in range(cnt):
            if (j+1) != pgrecs['disp_order'][j]:
               record['disp_order'] = j + 1
               changed += self.pgupdt(tname, record, "{} AND wfile = '{}'".format(ccnd, pgrecs['wfile'][j]), self.PGOPT['extlog'])
      s = 's' if changed > 1 else ''
      self.pglog("{} source file record{} reordered!".format(changed, s), self.PGOPT['wrnlog'])
      return changed

   # validate given dataset IDs
   def validate_datasets(self):
      if self.OPTS['DS'][2]&8: return  # already validated
      dcnt = len(self.params['DS'])
      for i in range(dcnt):
         dsid = self.params['DS'][i]
         if not dsid: self.action_error("Empty Dataset ID is not allowed")
         if i > 0 and dsid == self.params['DS'][i-1]: continue
         if not self.pgget("dataset", "", "dsid = '{}'".format(dsid), self.PGOPT['extlog']):
            self.action_error("Dataset {} is not in RDADB".format(dsid))
      self.OPTS['DS'][2] |= 8  # set validated flag

   # validate given request indices or request IDs
   def validate_requests(self):
      if (self.OPTS['RI'][2]&8) == 8: return   # already validated
      if 'RI' in self.params:
         rcnt = len(self.params['RI'])
         i = 0
         while i < rcnt:
            val = self.params['RI'][i]
            if val:
               if not isinstance(val, int):
                  if re.match(r'^(!|<|>|<>)$', val):
                     if self.OPTS[self.PGOPT['CACT']][2] > 0:
                        self.action_error("Invalid condition '{}' of Request index".format(val))
                     break
                  self.params['RI'][i] = int(val)
            else:
               self.params['RI'][i] = 0
            i += 1
         if i >= rcnt:  # normal request index given
            for i in range(rcnt):
               val = self.params['RI'][i]
               if not val:
                  if self.PGOPT['CACT'] != "SR":
                     self.action_error("Request Index 0 is not allowed\n" +
                                  "Use Action SR with Mode option -NR to add new record", self.PGOPT['CACT'])
                  elif 'NR' not in self.params:
                     self.action_error("Mode option -NR must be present to add new Request record", self.PGOPT['CACT'])
                  continue
               if i > 0 and val == self.params['RI'][i-1]: continue
               pgrec = self.pgget("dsrqst", "dsid, specialist", "rindex = {}".format(val), self.PGOPT['extlog'])
               if not pgrec:
                  self.action_error("Request Index {} is not in RDADB".format(val))
               elif self.OPTS[self.PGOPT['CACT']][2] > 0:
                  if pgrec['specialist'] == self.PGLOG['CURUID']:
                     self.params['MD'] = 1
                  else:
                     self.validate_dsowner("dsrqst", pgrec['dsid'])
         else: # found none-equal condition sign
            pgrec = self.pgmget("dsrqst", "rindex", self.get_field_condition("rindex", self.params['RI'], 0, 1), self.PGOPT['extlog'])
            if not pgrec: self.action_error("No Request matches given Index condition")
            self.params['RI'] = pgrec['rindex']
      elif 'RN' in self.params:
         self.params['RI'] = self.rid2rindex(self.params['RN'])
      self.OPTS['RI'][2] |= 8  # set validated flag

   # validate given request partition indices
   def validate_partitions(self):
      if (self.OPTS['PI'][2]&8) == 8: return   # already validated
      pcnt = len(self.params['PI']) if 'PI' in self.params else 0
      if not pcnt:
         if self.PGOPT['CACT'] == 'SP' and not self.params['NP']:
            self.action_error("Mode option -NP must be present to add new Request Partitions")
         return
      i = 0
      while i < pcnt:
         val = self.params['PI'][i]
         if val:
            if not isinstance(val, int):
               if re.match(r'^(!|<|>|<>)$', val):
                  if self.OPTS[self.PGOPT['CACT']][2] > 0:
                     self.action_error("Invalid condition '{}' of Request Partition index".format(val))
                  break
               self.params['PI'][i] = int(val)
         else:
            self.params['PI'][i] = 0
         i += 1
      if i >= pcnt: # normal request request partition given
         for i in range(pcnt):
            val = self.params['PI'][i]
            if not val: self.action_error("Request Partition Index 0 is not allowed", self.PGOPT['CACT'])
            if i and val == self.params['PI'][i-1]: continue
            pgrec = self.pgget("ptrqst", "dsid, specialist", "pindex = {}".format(val), self.PGOPT['extlog'])
            if not pgrec:
               self.action_error("Request Partition Index {} is not in RDADB".format(val))
            elif self.OPTS[self.PGOPT['CACT']][2] > 0:
               if pgrec['specialist'] == self.PGLOG['CURUID']:
                  self.params['MD'] = 1
               else:
                  self.validate_dsowner("dsrqst", pgrec['dsid'])
      else:   # found none-equal condition sign
         pgrec = self.pgmget("ptrqst", "pindex", self.get_field_condition("pindex", self.params['PI'], 0, 1), self.PGOPT['extlog'])
         if not pgrec: self.action_error("No Request Parition matches given Index condition")
         self.params['PI'] = pgrec['pindex']
      self.OPTS['PI'][2] |= 8   # set validated flag

   # validate given request control indices
   def validate_controls(self):
      if (self.OPTS['CI'][2]&8) == 8: return   # already validated
      ccnt = len(self.params['CI']) if 'CI' in self.params else 0
      if not ccnt:
         if self.PGOPT['CACT'] == 'SC':
            if 'NC' not in self.params:
               self.action_error("Mode option -NC must be present to add new Request Control")
            ccnt = self.get_max_count("DS", "RT", "GI")
            for i in range(ccnt):
               self.params['CI'][i] = 0
         return
      i = 0
      while i < ccnt:
         val = self.params['CI'][i]
         if val:
            if not isinstance(val, int):
               if re.match(r'^(!|<|>|<>)$', val):
                  if self.OPTS[self.PGOPT['CACT']][2] > 0:
                     self.action_error("Invalid condition '{}' of Request Control index".format(val))
                  break
               self.params['CI'][i] = int(val)
         else:
            self.params['CI'][i] = 0
         i += 1
      if i >= ccnt:   # normal request control index given
         for i in range(ccnt):
            val = self.params['CI'][i]
            if not val:
               if self.PGOPT['CACT'] != 'SC':
                  self.action_error("Request Control Index 0 is not allowed\n" +
                               "Use action SC with Mode option -NC to add new record")
               elif 'NC' not in self.params:
                  self.action_error("Mode option -NC must be present to add new Request Control")
               continue
            if i and val == self.params['CI'][i-1]: continue
            if not self.pgget("rcrqst", "", "cindex = {}".format(val), self.PGOPT['extlog']):
               self.action_error("Request Control Index {} is not in RDADB".format(val))
      else: # found none-equal condition sign
         pgrec = self.pgmget("rcrqst", "cindex", self.get_field_condition("cindex", self.params['CI'], 0, 1), self.PGOPT['extlog'])
         if not pgrec: self.action_error("No Request Control matches given Index condition")
         self.params['CI'] = pgrec['cindex']
      self.OPTS['CI'][2] |= 8  # set validated flag

   # get request index array from given request IDs
   def rid2rindex(self, rqstids):
      count = len(rqstids) if rqstids else 0
      if count == 0: return None
      i = 0
      while i < count:
         val = rqstids[i]
         if val and (re.match(r'^(!|<|>|<>)$', val) or val.find('%') > -1): break
         i += 1
      if i >= count:   # normal request id given
         indices = [0]*count
         for i in range(count):
            val = rqstids[i]
            if not val: continue
            if i > 0 and (val == rqstids[i-1]):
               indices[i] = indices[i-1]
            else:
               pgrec = self.pgget("dsrqst", "rindex", "rqstid = '{}'".format(val), self.PGOPT['extlog'])
               if pgrec:
                  indices[i] = pgrec['rindex']
               elif 'NR' in self.params and self.PGOPT['CACT'] == 'SR':
                  indices[i] = 0
               elif self.PGOPT['CACT'] == 'SR':
                  self.action_error("Request ID {} is not in RDADB,\nUse Mode Option ".format(val) +
                                "-NR (-NewRequest) to add new Request", 'SR')
               else:
                  self.action_error("Request ID {} is not in RDADB".format(val))
         return indices
      else:   # found wildcard and/or none-equal condition sign
         pgrec = self.pgmget("dsrqst", "rindex", self.get_field_condition("rqstid", rqstids, 1, 1), self.PGOPT['extlog'])
         if not pgrec: self.action_error("No Request matches given Request ID condition")
         return pgrec['rindex']

   # get request ID array from given request indices
   def rindex2rid(self, indices):
      count = len(indices) if indices else 0
      if count == 0: return None
      i = 0   
      while i < count:
         val = indices[i]
         if val:
            if not isinstance(val, int):
               if re.match(r'^(!|<|>|<>)$', val): break
               indices[i] = int(val)
         else:
            indices[i] = 0
         i += 1
      if i >= count:   # normal request index given
         rqstids = ['']*count
         for i in range(count):
            val = indices[i]
            if not val: continue
            if i > 0 and val == indices[i-1]:
               rqstids[i] = rqstids[i-1]
            else:
               pgrec = self.pgget("dsrqst", "rqstid", "rindex = {}".format(val), self.PGOPT['extlog'])
               if not pgrec: self.action_error("Request Index {} not in RDADB".format(val))
               rqstids[i] = pgrec['rqstid']
         return rqstids
      else:   # found none-equal condition sign
         pgrec = self.pgmget("dsrqst", "rqstid", self.get_field_condition("gindex", indices, 0, 1), self.PGOPT['extlog'])
         if not pgrec: self.action_error("No Request matches given Index condition")
         return pgrec['rqstid']

   # get dataset ids for given request indices
   def get_request_dsids(self, ridxs):
      count = len(ridxs) if ridxs else 0
      dsids = [None]*count
      for i in range(count):
         ridx = ridxs[i]
         if i == 0 or (ridx != ridxs[i-1]):
            pgrec = self.pgget("dsrqst", "dsid", "rindex = {}".format(ridx), self.PGOPT['extlog'])
            if not pgrec: self.action_error("Request Index {} not in RDADB".format(ridx))
         dsids[i] = pgrec['dsid']
      return dsids

   # get dataset ids for given request control indices
   def get_control_dsids(self, cidxs):
      count = len(cidxs) if cidxs else 0
      dsids = [None]*count
      for i in range(count):
         cidx = cidxs[i]
         if i == 0 or (cidx != cidxs[i - 1]):
            pgrec = self.pgget("rcrqst", "dsid", "cindex = {}".format(cidx), self.PGOPT['extlog'])
            if not pgrec: self.action_error("Request Control Index {} not in RDADB".format(cidx)) 
         dsids[i] = pgrec['dsid']
      return dsids

   # get file ids for given file names
   def fname2fid(self, files, dsids, stypes):
      count = len(files) if files else 0
      fids = [0]*count   
      for i in range(count):
         file = files[i]
         if not file: continue   # missing file name
         dsid = dsids[i]
         type = stypes[i]
         if not type or type == 'W': type = 'D'
         pgrec = self.pgget_wfile(dsid, 'wid', "wfile = '{}' AND type = '{}'".format(file, type), self.PGOPT['extlog'])
         if not pgrec: self.action_error("wfile_{}-{}: Error find Source File".format(dsid, file))
         fids[i] = pgrec['wid']
      return fids

   # get file names from given file ids
   def fid2fname(self, fids, dsids, stypes):
      count = len(fids) if fids else 0
      files = ['']*count
      for i in range(count):
         fid = fids[i]
         if not fid: continue   # missing file id
         stype = stypes[i] if stypes else ''
         dsid = dsids[i]
         condition = "wid = '{}'".format(fid)
         pgrec = self.pgget_wfile(dsid, 'wfile', condition, self.PGOPT['extlog'])
         if not pgrec: self.action_error("wfile_{}-{}: Error find Source File".format(dsid, fid))
         files[i] = pgrec['wfile']
      return files

   # get WEB file path for given dsid and file name
   # opt = 0 - relative path to self.params['WH']
   #       1 - absolute path
   #       2 - relative path to self.params['WH']/data/dsid
   def get_file_path(self, fname, dpath, rtpath, opt = 0):
      if not rtpath: rtpath = self.params['WH']
      if fname:
         if re.search(r'^/', fname):
            if opt != 1 and re.search(r'^{}/'.format(rtpath), fname):
               fname = self.join_paths(rtpath, fname, 1)   # remove rtpath if exists 
               if opt == 2: fname = self.join_paths(dpath, fname, 1)   # remove webpath if exists         
         elif opt == 2:
            fname = self.join_paths(dpath, fname, 1)   # remove webpath if exists 
         else:
            fname = self.join_paths(dpath, fname)
            if opt == 1: fname = self.join_paths(rtpath, fname)
      elif opt == 0:
         fname = dpath
      elif opt == 1:
         fname = self.join_paths(rtpath, dpath)
      return fname

   # check and see if enough disk space is allowed for the request
   def request_limit(self):
   #   pgrec = self.pgget("wfrqst", "round(sum(size)/1000000000, 0) s", "status = 'O'")
   #   if pgrec and pgrec['s'] and pgrec['s'] > self.PGOPT['TS']:
   #      self.pglog("Exceed Total Download Limit self.PGOPT['TS']GB", self.PGOPT['extlog'])
   #      return 1 # reach total request limit
   #   else:
         return 0 # OK to process request

   # return: converted file name and error message
   def convert_archive_format(self, pgfile, pgrqst, cmd, rstr):
      wfile = pgfile['wfile']
      ofile = pgfile['ofile']
      errmsg = None
      if pgrqst['file_format']: wfile = re.sub(r'\.{}'.format(pgrqst['file_format']), '', wfile, 1, re.I)
      fmsg = "{}-{}".format(rstr, wfile)
      pstat = self.check_processed(wfile, pgfile, pgrqst['dsid'], pgrqst['rindex'], rstr)
      if pstat > 0:
         self.pglog(fmsg + ": converted already", self.PGOPT['wrnlog']|self.FRCLOG)
         return (wfile, errmsg)
      elif pstat < 0:
         return (None, errmsg)
      pgsrc = self.pgget_wfile(pgrqst['dsid'], "wfile ofile, data_format, file_format", "wid = {}".format(pgfile['srcid']), self.LGEREX)
      if not pgsrc:
         errmsg = self.pglog("{}: Error get source record ({}-{})".format(fmsg, pgfile['srctype'], pgfile['srcid']), self.PGOPT['errlog']|self.RETMSG)
         return (wfile, errmsg)
      whome = self.join_paths(self.PGLOG['DSDHOME'], pgrqst['dsid'])
      ofile = self.join_paths(whome, pgsrc['ofile'])
      if not pgrqst['data_format']:
         if self.PGOPT['RCNTL'] and self.PGOPT['RCNTL']['data_format']: pgrqst['data_format'] = self.PGOPT['RCNTL']['data_format']
      if not pgrqst['file_format']:
         if self.PGOPT['RCNTL'] and self.PGOPT['RCNTL']['file_format']: pgrqst['file_format'] = self.PGOPT['RCNTL']['file_format']
      if not pgsrc['file_format']:
         errmsg = self.pglog("{}: miss original archive format to convert to {}".format(fmsg, pgrqst['data_format']), self.PGOPT['errlog']|self.RETMSG)
         return (wfile, errmsg)
      afmt = self.valid_archive_format(pgrqst['file_format'], pgsrc['file_format'], 1)
      if not cmd:
         errmsg = self.pglog("{}: miss archive format conversion command ({} to {})".format(fmsg, pgsrc['data_format'], pgrqst['data_format']), self.PGOPT['errlog']|self.RETMSG)
         return (wfile, errmsg)
      errmsg = ''
      syserr = "\n"
      acmd = "{} {}".format(cmd, ofile)
      wfile = self.pgsystem(acmd, self.PGOPT['wrnlog']|self.FRCLOG, 273)   # 273 = 1 +16 + 256
      if self.PGLOG['SYSERR']: syserr += self.PGLOG['SYSERR']
      if not wfile:
         errmsg = "{}: Error convert archive format{}".format(acmd, syserr)
         self.pglog(errmsg, self.PGOPT['errlog'])
      else:
         ms = re.match(r'^(.*)\n', wfile)
         if ms: wfile = ms.group(1)
         finfo = self.check_local_file(wfile, 0, self.PGOPT['wrnlog'])
         if not finfo:
            errmsg = "{}: no file converted{}".format(acmd, syserr)
            self.pglog(errmsg, self.PGOPT['errlog'])
         elif finfo['data_size'] == 0:
            errmsg = "Empty file " + wfile
      return (wfile, errmsg)

   # return: converted file name and error message
   def convert_data_format(self, pgfile, pgrqst, cmd, rstr):
      wfile = pgfile['wfile']
      ofile = pgfile['ofile']
      errmsg = None
      if pgrqst['file_format']: wfile = re.sub(r'\.{}'.format(pgrqst['file_format']), '', wfile, 1, re.I)
      fmsg = "{}-{}".format(rstr, wfile)
      pstat = self.check_processed(wfile, pgfile, pgrqst['dsid'], pgrqst['rindex'], rstr)
      if pstat > 0:
         self.pglog(fmsg + ": converted already", self.PGOPT['wrnlog']|self.FRCLOG)
         return (wfile, errmsg)
      elif pstat < 0:
         return (None, errmsg)
      pgsrc = self.pgget_wfile(pgrqst['dsid'], "wfile ofile, data_format, file_format", "wid = {}".format(pgfile['srcid']), self.LGEREX)
      if not pgsrc:
         errmsg = self.pglog("{}: Error get source record ({}-{})".format(fmsg, pgfile['srctype'], pgfile['srcid']), self.PGOPT['errlog']|self.RETMSG)
         return (wfile, errmsg)
      if pgfile['srctype'] == "W":
         whome = self.join_paths(self.PGLOG['DSDHOME'], pgrqst['dsid'])
         ofile = self.join_paths(whome, pgsrc['ofile'])
      if not pgrqst['data_format']:
         if self.PGOPT['RCNTL'] and self.PGOPT['RCNTL']['data_format']: pgrqst['data_format'] = self.PGOPT['RCNTL']['data_format']
      if not pgrqst['file_format']:
         if self.PGOPT['RCNTL'] and self.PGOPT['RCNTL']['file_format']: pgrqst['file_format'] = self.PGOPT['RCNTL']['file_format']
      if not pgsrc['data_format']:
         errmsg = self.pglog("{}: miss original data format to convert to {}".format(fmsg, pgrqst['data_format']), self.PGOPT['errlog']|self.RETMSG)
         return (wfile, errmsg)
      afmt = self.valid_archive_format(pgrqst['file_format'], pgsrc['file_format'], 1)
      if afmt: wfile = re.sub(r'\.{}'.format(afmt), '', wfile, 1, re.I)
      if not cmd:
         if re.search(r'netcdf$', pgrqst['data_format'], re.I):
            cmd = "format_to_netcdf"
         else:
            errmsg = self.pglog("{}: miss format conversion command ({} to {})".format(fmsg, pgsrc['data_format'], pgrqst['data_format']), self.PGOPT['errlog']|self.RETMSG)
            return (wfile, errmsg)
      if pgsrc['file_format']:
         ext = self.get_format_extension(pgrqst['data_format'])
         errmsg = self.multiple_conversion(cmd, ofile, pgsrc['data_format'].lower(), pgsrc['file_format'], ext, wfile)
      else:
         errmsg = self.do_conversion("{} {} {}".format(cmd, ofile, pgsrc['data_format'].lower()), wfile)
      if afmt and not errmsg:
         wfile = self.compress_local_file(wfile, afmt, 1)[0]
         finfo = self.check_local_file(wfile, 7, self.PGOPT['wrnlog'])
         if not finfo:
            errmsg = "Error check " + wfile
         elif finfo['data_size'] == 0:
            errmsg = "Empty file " + wfile
      return (wfile, errmsg)

   # get file extension for given data format
   def get_format_extension(self, dfmt):
      DEXTS = {'netcdf' : ".nc", 'nc' : ".nc", 'grib' : ".grb", 'grb' : ".grb", 'hdf' : ".hdf"}
      for dkey in DEXTS:
         if re.search(r'{}'.format(dkey), dfmt, re.I):
            return DEXTS[dkey]
      return ''

   # convert data format for a given file
   # return '' if sucessful error mesage otherwise
   def do_conversion(self, cmd, file):
      msg = ''
      err = "\n"
      self.PGLOG['STD2ERR'] = ["fatal:"]
      ret = self.pgsystem(cmd, self.PGOPT['wrnlog']|self.FRCLOG, 257)   # 257 = 1 + 256
      if self.PGLOG['SYSERR']: err += self.PGLOG['SYSERR']
      self.PGLOG['STD2ERR'] = []
      if not ret:
         msg = "{}: Error convert format{}".format(cmd, err)
         self.pglog(msg, self.PGOPT['errlog'])
      elif not self.check_local_file(file, 0, self.PGOPT['wrnlog']):
         msg = "{}: no file converted{}".format(cmd, err)
         self.pglog(msg, self.PGOPT['errlog'])
      return msg

   # convert data format for given file, keeping the archive format
   # return 0 if sucessful error mesage otherwise
   def multiple_conversion(self, cmd, ifile, dfmt, afmt, oext, ofile):
      iname = op.basename(ifile)
      wdir = iname + "_tmpdir"
      if op.exists(wdir): self.pgsystem("rm -rf " + wdir, self.PGOPT['extlog'], 5)
      self.local_copy_local("{}/{}".format(wdir, iname), ifile, self.PGOPT['extlog'])
      self.change_local_directory(wdir, self.PGOPT['extlog'])
      afmts = re.split(r'\.', afmt)
      acnt = len(afmts)
      acts = [None]*acnt
      files =[None]*(acnt+1)
      files[0] = [iname]
      cnts = [1]*(acnt+1)
      # untar/uncompress
      j = 0
      while acnt > 0:
         acnt -= 1
         fmt = afmts[acnt]
         tfiles = files[j]
         if re.search(r'^tar', fmt, re.I):
            tfile = tfiles[j]
            self.pgsystem("tar -xvf " + tfile, self.PGOPT['extlog'], 5)
            self.delete_local_file(tfile, self.PGOPT['extlog'])
            acts[j] = 'tar'
            j += 1
            files[j] = self.get_directory_files()
            cnts[j] = len(files[j])
         else:
            ms = re.search(r'^({})'.format(self.CMPSTR), fmt, re.I)
            if ms:
               ext = ms.group(1)
               acts[j] = ext
               j += 1
               cnts[j] = cnts[j-1]
               files[j] = [None]*cnts[j]
               for i in range(cnts[j]):
                  files[j][i] = self.compress_local_file(tfiles[i], ext, 0, self.PGOPT['extlog'])[0]
      # convert data format now
      tfiles = files[j]
      for i in range(cnts[j]):
         tfile = tfiles[i]
         file = tfile + oext
         msg = self.do_conversion("{} {} {}".format(cmd, tfile, dfmt), tfile)
         if msg:
            self.change_local_directory("../", self.PGOPT['extlog'])
            return msg
         dir = op.dirname(tfile)
         if dir and dir != ".":
            self.move_local_file(file, op.basename(file), self.PGOPT['extlog'])
         self.delete_local_file(tfile, self.PGOPT['extlog'])
         files[j][i] = file
      # tar/compress
      while j > 0:
         j -= 1
         if acts[j] == 'tar':
            file = files[j][0]
            ms = re.match(r'^(.+)\.tar$', file)
            if ms:
               file = "{}{}.tar".format(ms.group(1), oext)
            else:
               file += oext
            self.pgsystem("tar -cvf {} *".format(file), self.PGOPT['extlog'], 5)
            files[j][0] = file
         else:
            tfiles = files[j+1]
            for i in range(cnts[j]):
               files[j][i] = self.compress_local_file(tfiles[i], acts[j], 1, self.PGOPT['extlog'])[0]
      self.change_local_directory("../", self.PGOPT['extlog'])
      if op.exists(ofile): self.delete_local_file(ofile, self.PGOPT['extlog'])
      self.move_local_file(ofile, "{}/{}".format(wdir, files[0][0]), self.PGOPT['extlog'])
      self.delete_local_file(wdir, self.PGOPT['extlog'])
      return ''  

   # validate the given archive format (afmt) is needed or not
   # against existing format (format)
   # return the needed format if diff; otherwise, with the needed format appended 
   def valid_archive_format(self, afmt, format, diff = 0):
      if afmt and format and re.search(r'(^|\.){}(\.|$)'.format(afmt), format, re.I): afmt = None
      if diff: return afmt
      if afmt:
         if format:
            format += '.' + afmt
         else:
            format = afmt
      return format

   # format floating point values
   def format_floats(self, recs, info, idx1, idx2):
      vals = recs['size']
      total = 0
      for i in range(idx1, idx2):
         val = vals[i]
         info['SIZ'][i] = val
         total += val
         vals[i] = self.format_one_float(val)
      return total

   # format a float point value into string
   def format_one_float(self, val):
      units = ('B', 'K', 'M', 'G', 'T', 'P')
      idx = 0
      while val > 1000:
         val /= 1000
         idx += 1
         if idx >= 5: break
      if idx > 0:
         return "{:.2f}{}".format(val, units[idx])
      else:
         return "{}{}".format(val, units[idx])

   # format dates
   def format_dates(self, vals, idx1, idx2, fmt = None):
      if not fmt: fmt = "MM/DD/YYYY"
      for i in range(idx1, idx2):
         if not vals[i]: continue
         dates = re.split(r'-', str(vals[i]))
         vals[i] = self.fmtdate(int(dates[0]), int(dates[1]), int(dates[2]), fmt)

   # set request file counts and total sizes
   def set_request_count(self, rcnd, pgrqst = None, show = 0):
      record = {}
      if not pgrqst:
         # get the request count and size information if not given
         pgrqst = self.pgget("dsrqst", "fcount, size_request, tarflag, tarcount", rcnd, self.PGOPT['extlog'])
         if not pgrqst: return self.pglog("Error get file count/size info from 'dsrqst' for " + rcnd, self.PGOPT['errlog'])
      if not pgrqst['fcount']:
         fcnt = self.pgget("wfrqst", "", rcnd, self.PGOPT['extlog'])
         if fcnt: pgrqst['fcount'] = record['fcount'] = fcnt
      if pgrqst['tarflag'] and pgrqst['tarflag'] == 'Y' and not pgrqst['tarcount']:
         tarcnt = self.pgget("tfrqst", "", rcnd, self.PGOPT['extlog'])
         if tarcnt: pgrqst['tarcount'] = record['tarcount'] = tarcnt
      if not pgrqst['size_request']:
         pgrec = self.pgget("wfrqst", "sum(size) size_request", rcnd + " AND type = 'D'", self.PGOPT['extlog'])
         if pgrec and pgrec['size_request']: record['size_request'] = pgrec['size_request']
      if record and self.pgupdt("dsrqst", record, rcnd, self.PGOPT['extlog']) and show:
         if 'size_request' in record:
            show = "size_request({})".format(record['size_request'])
         else:
            show = ''
         if 'fcount' in record:
            if show: show += "/"
            show += "fcount({})".format(record['fcount'])
         if 'tarcount' in record:
            if show: show += "/"
            show += "tarcount({})".format(record['tarcount'])
         self.pglog("{} set for Request {}".format(show, rcnd), self.PGOPT['wrnlog']|self.FRCLOG)
      return pgrqst['fcount']   # always return the number of files

   # check file processed or not wait if under processing by another request
   # return 1 if processed already -1 under processing and 0 not exists
   def check_processed(self, pfile, pgfile, dsid, cridx, rstr):
      wfile = pgfile['wfile']
      origin = (0 if wfile == pfile else 1)
      wcnd = "wfile = '{}' AND rindex <> {} AND ".format(wfile, cridx)
      pinfo = self.check_local_file(pfile, 1, self.PGOPT['wrnlog'])
      winfo = (self.check_local_file(wfile, 1, self.PGOPT['wrnlog']) if origin else pinfo)
      # check if web file exists already
      if winfo and winfo['data_size'] > 0:
         if (pgfile['size'] == winfo['data_size'] or 
             pgfile['time'] and pgfile['date'] == winfo['date_modified'] and
             pgfile['time'] == winfo['time_modified']):
            return 1
         pgrecs = self.pgmget("wfrqst", "rindex", "{}date = '{}' AND time = '{}'".format(wcnd, winfo['date_modified'], winfo['time_modified']), self.PGOPT['extlog'])
         cnt = len(pgrecs['rindex']) if pgrecs else 0
         for i in range(cnt):
            ridx = pgrecs['rindex'][i]
            if self.pgget("dsrqst", "", "rindex = {} AND dsid = '{}'".format(ridx, dsid), self.PGOPT['extlog']):
               return 1
      # check if under process
      pgrecs = self.pgmget("wfrqst", "rindex, pindex, pid", wcnd + "pid > 0", self.PGOPT['extlog'])
      if pgrecs:
         cnt = len(pgrecs['rindex'])
         for i in range(cnt):
            pid = pgrecs['pid'][i]
            if pgrecs['pindex'][i]:
               pidx = pgrecs['pindex'][i]
               pgrec = self.pgget("ptrqst", "lockhost", "pindex = {} AND pid = {}".format(pidx, pid), self.PGOPT['extlog'])
               if pgrec:
                  self.pglog(("{}-{}: Locked by RPT{}".format(rstr, wfile, pidx)) +
                               self.lock_process_info(pid, pgrec['lockhost']),
                              self.LOGWRN|self.FRCLOG)
                  return -1
            else:
               ridx = pgrecs['rindex'][i]
               pgrec = self.pgget("dsrqst", "lockhost", "rindex = {} AND pid = {}".format(ridx, pid), self.PGOPT['extlog'])
               if pgrec:
                  self.pglog(("{}-{}: Locked by Rqst{}".format(rstr, wfile, ridx)) +
                               self.lock_process_info(pid, pgrec['lockhost']),
                              self.LOGWRN|self.FRCLOG)
                  return -1
      if pinfo:
         if origin and pinfo['data_size'] > 0: return 1   # assume this is a good file
         self.delete_local_file(pfile)  # clean the dead file
      return 0

   # Fill request metrics into table dssdb.ousage for given request index and purge date
   def fill_request_metrics(self, ridx, pgpurge):
      order = "r-{}".format(ridx)
      record = {}
      pgorder = self.pgget("ousage", "date_closed, size_input", "order_number = '{}'".format(order), self.PGOPT['extlog'])
      if pgorder:
         if pgorder['date_closed'] != pgpurge['date_purge'] or pgorder['size_input'] != pgpurge['size_input']:
            record['date_closed'] = pgpurge['date_purge']
            record['size_request'] = pgpurge['size_request']
            record['size_input'] = pgpurge['size_input']
            record['count'] = pgpurge['fcount']
            if self.pgupdt("ousage", record, "order_number = '{}'".format(order), self.PGOPT['extlog']):
               self.pglog("Request '{}' is updated for order metrics".format(ridx), self.PGOPT['wrnlog']|self.FRCLOG)
         return    # no further action
      # add new order record
      record['order_number'] = order
      record['wuid_request'] = pgpurge['wuid_request']
      record['dss_uname'] = pgpurge['specialist']
      record['dsid'] = pgpurge['dsid']
      record['date_request'] = pgpurge['date_rqst']
      record['date_closed'] = pgpurge['date_purge']
      record['size_request'] = pgpurge['size_request']
      record['size_input'] = pgpurge['size_input']
      record['count'] = pgpurge['fcount']
      record['pay_method'] = "nochg"
      record['amount'] = 0
      record['method'] = 'R-' + pgpurge['rqsttype']
      record['project'] = 'DSRQST'
      record['ip'] = pgpurge['ip']
      record['quarter'] = pgpurge['quarter']
      if self.add_to_allusage(record, pgpurge['time_rqst']) and self.pgadd("ousage", record, self.PGOPT['extlog']):
         self.pglog("Request '{}' is recorded into order metrics".format(ridx), self.PGOPT['wrnlog']|self.FRCLOG)

   # add request info into allusage
   def add_to_allusage(self, record, time):
      pgrec = self.pgget("wuser",  "email, org_type, country, region",
                          "wuid = {}".format(record['wuid_request']), self.PGOPT['extlog'])
      if not pgrec: return 0
      pgrec['dsid'] = record['dsid']
      pgrec['org_type'] = self.get_org_type(pgrec['org_type'], pgrec['email'])
      pgrec['country'] = self.set_country_code(pgrec['email'], pgrec['country'])
      pgrec['date'] = record['date_request']
      pgrec['time'] = time
      pgrec['size'] = record['size_request']
      pgrec['method'] = record['method']
      pgrec['quarter'] = record['quarter']
      pgrec['ip'] = record['ip']
      pgrec['source'] = 'O'
      return self.add_yearly_allusage(None, pgrec)

   # get request status
   def request_status(self, rstat):
      RSTATUS = {
         'E' : "Error",
         'F' : "Offline",
         'H' : "Hold",
         'I' : "Interrupted",
         'N' : "Not Online",
         'O' : "Online",
         'P' : "Purge",
         'Q' : "Queue",
         'R' : "Request",
         'U' : "Unknown",
         'W' : "Wait"
      }
      if rstat not in RSTATUS: rstat = 'U'
      return RSTATUS[rstat]

   # cache request control information
   def cache_request_control(self, ridx, pgrqst, action, pidx = 0):
      if not pgrqst['rqstid']: pgrqst['rqstid'] = self.add_request_id(ridx, pgrqst['email'], 1)
      if self.check_host_down(self.PGLOG['RQSTHOME'], self.PGLOG['HOSTNAME'], self.PGOPT['errlog']):
         return None   # check if system down
      pgrec = self.PGOPT['RCNTL'] = None
      rtype = pgrqst['rqsttype']
      if rtype != 'C':
         gcnd = "dsid = '{}' AND gindex = ".format(pgrqst['dsid'])
         if rtype in "ST":
            tcnd = " AND (rqsttype = 'T' OR rqsttype = 'S')"
         else:
            tcnd = " AND rqsttype = '{}'".format(rtype)
         gindex = pgrqst['gindex']
         while True:
            pgrec = self.pgget("rcrqst", "*", "{}{}{}".format(gcnd, gindex, tcnd), self.PGOPT['errlog'])
            if not pgrec and gindex > 0:
               pgrec = self.pgget("dsgroup", "pindex", "{}{}".format(gcnd, gindex), self.PGOPT['errlog'])
               if pgrec:
                  gindex = pgrec['pindex']
                  continue
            break
      if not pgrec:
         pgrec = self.pgtable('rcrqst', self.PGOPT['extlog'])
         pgrec['rqsttype'] = rtype
         pgrec['gindex'] = pgrqst['gindex']
      if 'HN' in self.params: pgrec['hostname'] = self.params['HN'][0]
      if 'EO' in self.params: pgrec['empty_out'] = self.params['EO'][0]
      if 'PC' in self.params and self.params['PC'][0]:
         pgrec['command'] = self.params['PC'][0]
      elif 'command' in pgrqst and pgrqst['command'] and pgrqst['command'] != pgrec['command']:
         pgrec['command'] = pgrqst['command']
      if 'ptlimit' in pgrqst and pgrqst['ptlimit'] and pgrqst['ptlimit'] != pgrec['ptlimit']:
         pgrec['ptlimit'] = pgrqst['ptlimit']
      elif 'ptsize' in pgrqst and pgrqst['ptsize'] and pgrqst['ptsize'] != pgrec['ptsize']:
         pgrec['ptsize'] = pgrqst['ptsize']
      self.PGOPT['VP'] = self.params['VP'][0] if 'VP' in self.params else (pgrec['validperiod'] if pgrec['validperiod'] else self.PGOPT['DVP'])
      if pgrec['command']:
         pgrec['command'] += " {} ".format(ridx)
         if action != 'SP':
            if self.request_type(rtype, 1):
               pgrec['command'] += self.join_paths(self.params['WH'], "data/" + pgrqst['dsid'])
            else:
               location = pgrqst['location'] if 'location' in pgrqst else ''
               pgrec['command'] += self.get_file_path(None, pgrqst['rqstid'], location, 1)
            if pidx and action == 'PP':
               pgrec['command'] += " {}".format(pidx)
      self.PGOPT['RCNTL'] = pgrec
      return self.SUCCESS

   # get golally defined table row color
   def table_color(self, idx):
      tcolors = ("#CDBCDC", "#DFD8F8", "#EAEAFC", "#F8F6FE", "#E0C8B1")
      return tcolors[idx]

   # add a unique request id
   def add_request_id(self, ridx, email, updtdb = 0):
      unames = self.get_ruser_names(email)
      lname = self.convert_chars(unames['lstname'], 'RQST')
      rqstid = "{}{}".format(lname.upper(), ridx)
      if updtdb: self.pgexec("UPDATE dsrqst SET rqstid = '{}'".format(rqstid), self.PGOPT['extlog'])
      return rqstid

   # expand request status info
   def get_request_status(self, pgrecs, cnt = 0):
      if not cnt: cnt = (len(pgrecs['rindex']) if pgrecs else 0)
      rstats = pgrecs['status']
      for i in range(cnt):
         pgrec = self.onerecord(pgrecs, i)
         if rstats[i] == 'Q':
            ckrec = self.pgget("dscheck", "runhost, pid, lockhost, stttime",
                                "oindex = {} AND command = 'dsrqst' AND otype <> 'P'".format(pgrec['rindex']), self.PGOPT['extlog'])
            if pgrec['pid']:
               runhost = ""
               pcnt = 0
               percent = self.complete_request_percentage(pgrec, ckrec)
               if percent < 0:
                  rstats[i] += " - pending"
               else:
                  if percent > 0:
                     rstats[i] += " - {}% built".format(percent)
                  else:
                     rstats[i] += " - building"
                  if ckrec and ckrec['runhost']: runhost = ckrec['runhost']
                  if pgrec['ptcount'] > 1 and pgrec['lockhost'] == "partition": pcnt = pgrec['ptcount']
               rstats[i] += self.lock_process_info(pgrec['pid'], pgrec['lockhost'], runhost, pcnt)
            elif ckrec and ckrec['pid']:
               if ckrec['lockhost'] == self.PGLOG['PGBATCH']:
                  rstats[i] += " - pending on {}({})".format(ckrec['lockhost'], ckrec['pid'])
               else:
                  rstats[i] += " - building on {}({})".format(ckrec['lockhost'], ckrec['pid'])
            else:
               rstats[i] += " -  queued"
               if pgrec['hostname']: rstats[i] += " on " + pgrec['hostname']
         elif rstats[i] == 'O' and pgrec['location']:
            rstats[i] += " - " + self.request_status('F')
         else:
            rstats[i] += " - " + self.request_status(rstats[i])
      return rstats   

   # get percentage of completion of request process
   def complete_request_percentage(self, rqst, ckrec):
      if ckrec and not ckrec['stttime']: return -1
      percent = 0
      cnd = "rindex = {} AND status = 'O'".format(rqst['rindex'])
      if rqst['fcount'] and rqst['fcount'] > 0:
         cnt = rqst['pcount'] if rqst['pcount'] else self.pgget("wfrqst", "", cnd)
         if(cnt == 0 and rqst['rqstid'] and 
            (rqst['location'] or self.request_type(rqst['rqsttype'], 1) == 0)):
            files = glob.glob(get_file_path("*", rqst['rqstid'], rqst['location'], 1))
            cnt = len(files)
         if cnt > 0:
            if cnt < rqst['fcount']:
               percent = int(100*cnt/rqst['fcount'])
            else:
               percent = 99
      elif rqst['size_request'] > 0:
         pgrec = self.pgget("wfrqst", "sum(size) ts", cnd)
         if pgrec and pgrec['ts']:
            percent = int(100*pgrec['ts']/rqst['size_request'])
            if percent > 99: percent = 99
      return percent

   # expand request partition status info
   def get_partition_status(self, pgrecs, cnt = 0):
      if not cnt: cnt = (len(pgrecs['rindex']) if pgrecs else 0)
      rstats = [None]*cnt
      for i in range(cnt):
         pgrec = self.onerecord(pgrecs, i)
         rstats[i] = pgrec['status']
         if rstats[i] == 'Q':
            ckrec = self.pgget("dscheck", "runhost, pid, lockhost, stttime",
                                "oindex = {} AND command = 'dsrqst' AND otype = 'P'".format(pgrec['pindex']), self.PGOPT['extlog'])
            if pgrec['pid']:
               runhost = ""
               percent = self.complete_partition_percentage(pgrec, ckrec)
               if percent < 0:
                  rstats[i] += " - pending"
               else:
                  if percent > 0:
                     rstats[i] += " - {}% built".format(percent)
                  else:
                     rstats[i] += " - building"
                  if ckrec and ckrec['runhost']: runhost = ckrec['runhost']
               rstats[i] += self.lock_process_info(pgrec['pid'], pgrec['lockhost'], runhost)
            elif ckrec and ckrec['pid']:
               if ckrec['lockhost'] == self.PGLOG['PGBATCH']:
                  rstats[i] += " - pending on {}({})".format(ckrec['lockhost'], ckrec['pid'])
               else:
                  rstats[i] += " - building on {}({})".format(ckrec['lockhost'], ckrec['pid'])
            else:
               rstats[i] += " -  queued"
               if pgrec['hostname']: rstats[i] += " on " + pgrec['hostname']
         else:
            rstats[i] += " - " + self.request_status(rstats[i])
      return rstats   

   # get percentage of completion of a partition process
   def complete_partition_percentage(self, part, ckrec):
      if ckrec and not ckrec['stttime']: return -1
      percent = 0
      if part['fcount'] > 0:
         cnt = self.pgget("wfrqst", "", "pindex = {} AND status = 'O'".format(part['pindex']))
         if cnt > 0:
            if cnt < part['fcount']:
               percent = int(100*cnt/part['fcount'])
            else:
               percent = 99
      return percent

   # get md5 chechsum for requested file from source file record
   def get_requested_checksum(self, dsid, pgfile):
      if pgfile['srcid'] and pgfile['ofile'] and pgfile['wfile'] == pgfile['ofile']:
         pgsrc = self.pgget_wfile(dsid, "data_size, checksum", "wid = {}".format(pgfile['srcid']), self.LGEREX)
         if pgsrc and pgsrc['checksum'] and pgsrc['data_size'] == pgfile['size']:
            return pgsrc['checksum']
      return None
