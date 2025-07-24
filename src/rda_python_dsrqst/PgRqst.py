#
###############################################################################
#
#     Title : PgRqst.py
#
#    Author : Zaihua Ji,  zjiucar.edu
#      Date : 09/19/2020
#             2025-02-10 transferred to package rda_python_dsrqst from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for holding some global variables and
#             functions for dsrqst utility
#
#    Github : https://github.com/NCAR/rda-python-dsrqst.git
# 
###############################################################################
#
import os
import re
import time
import glob
from os import path as op 
from rda_python_common import PgLOG
from rda_python_common import PgUtil
from rda_python_common import PgCMD
from rda_python_common import PgFile
from rda_python_common import PgSIG
from rda_python_common import PgLock
from rda_python_common import PgOPT
from rda_python_common import PgDBI
from rda_python_common import PgSplit

CORDERS = {}

PgOPT.OPTS = {                         # (!= 0) - setting actions
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
}

PgOPT.ALIAS = {
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
}

#
# single letter short names for option 'FN' (Field Names) to retrieve info
# from RDADB only the fields can be manipulated by this application are listed
#
#  SHORTNM KEYS(PgOPT.OPTS) DBFIELD
PgOPT.TBLHASH['dsrqst'] = {           # condition flag, 0-int, 1-string, -1-exclude
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

PgOPT.TBLHASH['wfrqst'] = {
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

PgOPT.TBLHASH['tfrqst'] = {
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

PgOPT.TBLHASH['rcrqst'] = {
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

PgOPT.TBLHASH['ptrqst'] = {
   'P' : ['PI', "pindex",         0],
   'R' : ['RI', "rindex",         0],
   'B' : ['DS', "dsid",           1],
   'A' : ['PS', "status",         1],
   'O' : ['DO', "ptorder",       -1],
   'C' : ['FC', "fcount",         0],
   'S' : ['SN', "specialist",     1],
}

#default fields for getting info
PgOPT.PGOPT['dsrqst'] = "REBITOCJUXAGS"
PgOPT.PGOPT['wfrqst'] = "FRPTYSMNA"
PgOPT.PGOPT['tfrqst'] = "TFRPSMN"
PgOPT.PGOPT['rcrqst'] = "CTIRWGSPOUH"
PgOPT.PGOPT['ptrqst'] = "PRBAOCS"
#all fields for getting info
PgOPT.PGOPT['dsall'] = "RQEBITOWCJKUVXYAGNMPSLZHDF"
PgOPT.PGOPT['wfall'] = "FRPTILZYSCMNOAJKD"
PgOPT.PGOPT['tfall'] = "TFRPISMNOJKD"
PgOPT.PGOPT['rcall'] = "CTIRWVJKLZFDAXGSPNOUHMBQEY"
PgOPT.PGOPT['ptall'] = "PRBAOCS"

PgOPT.PGOPT['derr'] = ''
PgOPT.PGOPT['ready'] = "request_ready.txt"

# set default options
PgOPT.PGOPT['DVP'] = PgOPT.PGOPT['VP'] = 5     # in days
PgOPT.PGOPT['FLMT'] = 1000 
PgOPT.PGOPT['PTMAX'] = 24    # max number of partitions for a signle request
PgOPT.PGOPT['TARPATH'] = "TarFiles/"

# set default parameters
PgOPT.PGOPT['DTS'] = PgOPT.PGOPT['TS'] = 90000  # total size of all downloads, in GB
PgOPT.params['WH'] = PgLOG.PGLOG['RQSTHOME']

#
# check if enough information entered on command line and/or input file
# for given action(s)
#
def check_enough_options(cact):

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

   if ('RR' not in PgOPT.params and ('RI' in PgOPT.params or
       ('RN' in PgOPT.params and not ('DL' in PgOPT.params['DL'] and 'UR' in PgOPT.params['UR'])))):
      validate_requests()
   if 'DS' in PgOPT.params: validate_datasets()

   if cact == 'SC':
      validate_controls()
      if 'NC' in PgOPT.params and not ('DS' in PgOPT.params and 'RT' in PgOPT.params):
         erridx = 5
   elif PgOPT.OPTS[cact][2] == 4:
      if 'PI' in PgOPT.params: validate_partitions()
      if 'NP' in PgOPT.params:
         if 'RI' not in PgOPT.params: erridx = 9
      elif 'PI' not in PgOPT.params:
         erridx = 4
   elif PgOPT.OPTS[cact][2] > 0:
      if 'RI' not in PgOPT.params:
         if cact == 'UL':
            if 'PI' not in PgOPT.params: erridx = 8
         elif cact != 'DL' or not ('CI' in PgOPT.params or 'UD' in PgOPT.params or 'UR' in PgOPT.params or 'UF' in PgOPT.params):
            erridx = 0
      elif cact == 'SF':
         if not ('WF' in PgOPT.params or 'ON' in PgOPT.params):
            erridx = (7 if 'RO' in PgOPT.params else 1)
         elif 'SL' in PgOPT.params and 'OT' not in PgOPT.params:
            erridx = 6
      elif cact == 'SR':
         if 'NR' in PgOPT.params and 'DS' not in PgOPT.params:
            erridx = 2
      elif cact == 'ST':
         if not ('TI' in PgOPT.params or 'WF' in PgOPT.params or 'ON' in PgOPT.params):
            erridx = (11 if 'RO' in PgOPT.params else 1)
      elif 'WF' in PgOPT.params and (PgOPT.PGOPT['ACTS']&PgOPT.OPTS['HR'][0]):
         erridx = 3
   elif 'CI' in PgOPT.params and cact == 'GC':
      validate_controls()
   elif cact == 'GF' or cact == 'GT':
      if not ('PI' in PgOPT.params or 'RI' in PgOPT.params):
         erridx = 8
   if erridx >= 0:
      PgOPT.action_error(errmsg[erridx], cact)

   PgOPT.set_uid("dsrqst")

   if 'BP' in PgOPT.params:
      if 'DM' in PgOPT.params: PgOPT.params['DM'] = None
      oidx = 0
      otype = ''
      if PgOPT.OPTS[cact][2] == 4 and 'PI' in PgOPT.params:
         oidx = PgOPT.params['PI'][0]
         otype = 'P'
      elif 'RI' in PgOPT.params:
         oidx = PgOPT.params['RI'][0]
         otype = 'R'

      # set command line Batch options
      PgCMD.set_batch_options(PgOPT.params, 2, 1)
      PgCMD.init_dscheck(oidx, otype, "dsrqst", get_dsrqst_dataset(), cact,
                   ("" if 'AW' in PgOPT.params else PgLOG.PGLOG['CURDIR']), PgOPT.params['LN'], PgOPT.params['BP'], PgOPT.PGOPT['extlog'])

   if 'VP' in PgOPT.params: PgOPT.PGOPT['VP'] = PgOPT.params['VP'][0]
   PgSIG.start_none_daemon('dsrqst', cact, PgOPT.params['LN'], 1, 10, 1, 1)

#
# get the associated dataset id
#
def get_dsrqst_dataset():
   
   if 'DS' in PgOPT.params: return PgOPT.params['DS'][0]

   if 'RI' in PgOPT.params and PgOPT.params['RI'][0]:
      pgrec = PgDBI.pgget("dsrqst", "dsid", "rindex = {}".format(PgOPT.params['RI'][0]), PgOPT.PGOPT['extlog'])
      if pgrec: return pgrec['dsid']

   return None

#
# get continue display order of an archived data file of given dataset (and group)
# 
def get_next_disp_order(idx = 0, table = None):

   global CORDERS

   if not idx:
      CORDERS = {}  # reinitial lize cached display orders
      return
   elif not table:
      CORDERS[idx] = 0
      return

   fld = ('cindex' if table == 'sfrqst' else 'rindex')
   if idx not in CORDERS:
      pgrec = PgDBI.pgget(table, "max(disp_order) max_order", "{} = {}".format(fld, idx), PgOPT.PGOPT['extlog'])
      CORDERS[idx] = pgrec['max_order'] if pgrec and pgrec['max_order'] else 0

   CORDERS[idx] += 1
   return CORDERS[idx]

#
# reorder the files for request
#
def reorder_request_files(onames):

   tname = "wfrqst"
   rcnt = len(PgOPT.params['RI'])
   hash = PgOPT.TBLHASH[tname]

   PgLOG.pglog("Reorder request files ...", PgOPT.PGOPT['wrnlog'])

   flds = PgOPT.append_order_fields(onames, "RO", tname)
   fields = "disp_order, "
   if onames.find('F') < 0: fields += "wfile, "
   fields +=  PgOPT.get_string_fields(flds, tname)
   if 'OB' in PgOPT.params or re.search(r'L', onames, re.I):
      ocnd = ''
   else:
      ocnd = PgOPT.get_order_string(onames, tname, "R")
   changed = 0
   for i in range(rcnt):
      rindex = PgOPT.params['RI'][i]
      if i > 0 and rindex == PgOPT.params['RI'][i-1]: continue  # sorted already
      rcnd = "rindex = {}".format(rindex)
      pgrecs = PgDBI.pgmget(tname, fields, rcnd + ocnd, PgOPT.PGOPT['extlog'])
      cnt = len(pgrecs['wfile']) if pgrecs else 0
      if not ocnd and cnt > 1: pgrecs = PgUtil.sorthash(pgrecs, flds, hash)

      record = {}
      for j in range(cnt):
         if (j+1) != pgrecs['disp_order'][j]:
            record['disp_order'] = j + 1
            changed += PgDBI.pgupdt(tname, record, "{} AND wfile = '{}'".format(rcnd, pgrecs['wfile'][j]), PgOPT.PGOPT['extlog'])

   s = 's' if changed > 1 else ''
   PgLOG.pglog("{} request file record{} reordered!".format(changed, s), PgOPT.PGOPT['wrnlog'])

   return changed

#
# reorder the tar files for request
#
def reorder_tar_files(onames):

   tname  = "tfrqst"
   rcnt = len(PgOPT.params['RI'])
   hash = PgOPT.TBLHASH[tname]

   PgLOG.pglog("Reorder tar files ...", PgOPT.PGOPT['wrnlog'])

   flds = PgOPT.append_order_fields(onames, "RO", tname)
   fields = "disp_order, "
   if onames.find('F') < 0: fields += "wfile, "
   fields +=  PgOPT.get_string_fields(flds, tname)
   if 'OB' in PgOPT.params:
      ocnd = ''
   else:
      ocnd = PgOPT.get_order_string(onames, tname, "R")
   changed = 0
   for i in range(rcnt):
      rindex = PgOPT.params['RI'][i]
      if i > 0 and rindex == PgOPT.params['RI'][i-1]: continue  # sorted already
      rcnd = "rindex = {}".format(rindex)
      pgrecs = PgDBI.pgmget(tname, fields, rcnd + ocnd, PgOPT.PGOPT['extlog'])
      cnt = len(pgrecs['wfile']) if pgrecs else 0
      if not ocnd and cnt > 1: pgrecs = PgUtil.sorthash(pgrecs, flds, hash)

      record = {}
      for j in range(cnt):
         if (j+1) != pgrecs['disp_order'][j]:
            record['disp_order'] = j + 1
            changed += PgDBI.pgupdt(tname, record, "{} AND wfile = '{}'".format(rcnd, pgrecs['wfile'][j]), PgOPT.PGOPT['extlog'])

   s = 's' if changed > 1 else ''
   PgLOG.pglog("{} tar file record{} reordered!".format(changed, s), PgOPT.PGOPT['wrnlog'])

   return changed

#
# reorder the source files
#
def reorder_source_files(onames):

   tname = "sfrqst"
   ccnt = len(PgOPT.params['CI'])
   hash = PgOPT.TBLHASH[tname]

   PgLOG.pglog("Reorder source files ...", PgOPT.PGOPT['wrnlog'])

   flds = PgOPT.append_order_fields(onames, "CO", tname)
   fields = "disp_order, "
   if onames.find('F') < 0: fields += "wfile, "
   fields +=  PgOPT.get_string_fields(flds, tname)
   if 'OB' in PgOPT.params or re.search(r'L', onames, re.I):
      ocnd = ''
   else:
      ocnd = PgOPT.get_order_string(onames, tname, "C")
   changed = 0
   for i in range(ccnt):
      cindex = PgOPT.params['CI'][i]
      if i > 0 and cindex == PgOPT.params['CI'][i-1]: continue  # sorted already
      ccnd = "cindex = {}".format(cindex)
      pgrecs = PgDBI.pgmget(tname, fields, ccnd + ocnd, PgOPT.PGOPT['extlog'])
      cnt = len(pgrecs['wfile']) if pgrecs else 0
      if not ocnd and cnt > 1: pgrecs = PgUtil.sorthash(pgrecs, flds, hash)

      record = {}
      for j in range(cnt):
         if (j+1) != pgrecs['disp_order'][j]:
            record['disp_order'] = j + 1
            changed += PgDBI.pgupdt(tname, record, "{} AND wfile = '{}'".format(ccnd, pgrecs['wfile'][j]), PgOPT.PGOPT['extlog'])

   s = 's' if changed > 1 else ''
   PgLOG.pglog("{} source file record{} reordered!".format(changed, s), PgOPT.PGOPT['wrnlog'])

   return changed

#
# validate given dataset IDs
#
def validate_datasets():

   if PgOPT.OPTS['DS'][2]&8: return  # already validated

   dcnt = len(PgOPT.params['DS'])
   for i in range(dcnt):
      dsid = PgOPT.params['DS'][i]
      if not dsid: PgOPT.action_error("Empty Dataset ID is not allowed")
      if i > 0 and dsid == PgOPT.params['DS'][i-1]: continue
      if not PgDBI.pgget("dataset", "", "dsid = '{}'".format(dsid), PgOPT.PGOPT['extlog']):
         PgOPT.action_error("Dataset {} is not in RDADB".format(dsid))

   PgOPT.OPTS['DS'][2] |= 8  # set validated flag

#
# validate given request indices or request IDs
#
def validate_requests():

   if (PgOPT.OPTS['RI'][2]&8) == 8: return   # already validated
   
   if 'RI' in PgOPT.params:
      rcnt = len(PgOPT.params['RI'])
      i = 0
      while i < rcnt:
         val = PgOPT.params['RI'][i]
         if val:
            if not isinstance(val, int):
               if re.match(r'^(!|<|>|<>)$', val):
                  if PgOPT.OPTS[PgOPT.PGOPT['CACT']][2] > 0:
                     PgOPT.action_error("Invalid condition '{}' of Request index".format(val))
                  break
               PgOPT.params['RI'][i] = int(val)
         else:
            PgOPT.params['RI'][i] = 0
         i += 1
      if i >= rcnt:  # normal request index given
         for i in range(rcnt):
            val = PgOPT.params['RI'][i]
            if not val:
               if PgOPT.PGOPT['CACT'] != "SR":
                  PgOPT.action_error("Request Index 0 is not allowed\n" +
                               "Use Action SR with Mode option -NR to add new record", PgOPT.PGOPT['CACT'])
               elif 'NR' not in PgOPT.params:
                  PgOPT.action_error("Mode option -NR must be present to add new Request record", PgOPT.PGOPT['CACT'])
               continue

            if i > 0 and val == PgOPT.params['RI'][i-1]: continue
            pgrec = PgDBI.pgget("dsrqst", "dsid, specialist", "rindex = {}".format(val), PgOPT.PGOPT['extlog'])
            if not pgrec:
               PgOPT.action_error("Request Index {} is not in RDADB".format(val))
            elif PgOPT.OPTS[PgOPT.PGOPT['CACT']][2] > 0:
               if pgrec['specialist'] == PgLOG.PGLOG['CURUID']:
                  PgOPT.params['MD'] = 1
               else:
                  PgOPT.validate_dsowner("dsrqst", pgrec['dsid'])
      else: # found none-equal condition sign
         pgrec = PgDBI.pgmget("dsrqst", "rindex", PgDBI.get_field_condition("rindex", PgOPT.params['RI'], 0, 1), PgOPT.PGOPT['extlog'])
         if not pgrec: PgOPT.action_error("No Request matches given Index condition")
         PgOPT.params['RI'] = pgrec['rindex']
   elif 'RN' in PgOPT.params:
      PgOPT.params['RI'] = rid2rindex(PgOPT.params['RN'])

   PgOPT.OPTS['RI'][2] |= 8  # set validated flag

#
# validate given request partition indices
#
def validate_partitions():

   if (PgOPT.OPTS['PI'][2]&8) == 8: return   # already validated

   pcnt = len(PgOPT.params['PI']) if 'PI' in PgOPT.params else 0
   if not pcnt:
      if PgOPT.PGOPT['CACT'] == 'SP' and not PgOPT.params['NP']:
         PgOPT.action_error("Mode option -NP must be present to add new Request Partitions")
      return
   i = 0
   while i < pcnt:
      val = PgOPT.params['PI'][i]
      if val:
         if not isinstance(val, int):
            if re.match(r'^(!|<|>|<>)$', val):
               if PgOPT.OPTS[PgOPT.PGOPT['CACT']][2] > 0:
                  PgOPT.action_error("Invalid condition '{}' of Request Partition index".format(val))
               break
            PgOPT.params['PI'][i] = int(val)
      else:
         PgOPT.params['PI'][i] = 0
      i += 1

   if i >= pcnt: # normal request request partition given
      for i in range(pcnt):
         val = PgOPT.params['PI'][i]
         if not val: PgOPT.action_error("Request Partition Index 0 is not allowed", PgOPT.PGOPT['CACT'])
         if i and val == PgOPT.params['PI'][i-1]: continue
         pgrec = PgDBI.pgget("ptrqst", "dsid, specialist", "pindex = {}".format(val), PgOPT.PGOPT['extlog'])
         if not pgrec:
            PgOPT.action_error("Request Partition Index {} is not in RDADB".format(val))
         elif PgOPT.OPTS[PgOPT.PGOPT['CACT']][2] > 0:
            if pgrec['specialist'] == PgLOG.PGLOG['CURUID']:
               PgOPT.params['MD'] = 1
            else:
               PgOPT.validate_dsowner("dsrqst", pgrec['dsid'])
   else:   # found none-equal condition sign
      pgrec = PgDBI.pgmget("ptrqst", "pindex", PgDBI.get_field_condition("pindex", PgOPT.params['PI'], 0, 1), PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("No Request Parition matches given Index condition")
      PgOPT.params['PI'] = pgrec['pindex']

   PgOPT.OPTS['PI'][2] |= 8   # set validated flag

#
# validate given request control indices
#
def validate_controls():

   if (PgOPT.OPTS['CI'][2]&8) == 8: return   # already validated

   ccnt = len(PgOPT.params['CI']) if 'CI' in PgOPT.params else 0
   if not ccnt:
      if PgOPT.PGOPT['CACT'] == 'SC':
         if 'NC' not in PgOPT.params:
            PgOPT.action_error("Mode option -NC must be present to add new Request Control")
         ccnt = PgOPT.get_max_count("DS", "RT", "GI")
         for i in range(ccnt):
            PgOPT.params['CI'][i] = 0
      return
   i = 0
   while i < ccnt:
      val = PgOPT.params['CI'][i]
      if val:
         if not isinstance(val, int):
            if re.match(r'^(!|<|>|<>)$', val):
               if PgOPT.OPTS[PgOPT.PGOPT['CACT']][2] > 0:
                  PgOPT.action_error("Invalid condition '{}' of Request Control index".format(val))
               break
            PgOPT.params['CI'][i] = int(val)
      else:
         PgOPT.params['CI'][i] = 0
      i += 1
   if i >= ccnt:   # normal request control index given
      for i in range(ccnt):
         val = PgOPT.params['CI'][i]
         if not val:
            if PgOPT.PGOPT['CACT'] != 'SC':
               PgOPT.action_error("Request Control Index 0 is not allowed\n" +
                            "Use action SC with Mode option -NC to add new record")
            elif 'NC' not in PgOPT.params:
               PgOPT.action_error("Mode option -NC must be present to add new Request Control")
            continue
         if i and val == PgOPT.params['CI'][i-1]: continue
         if not PgDBI.pgget("rcrqst", "", "cindex = {}".format(val), PgOPT.PGOPT['extlog']):
            PgOPT.action_error("Request Control Index {} is not in RDADB".format(val))
   else: # found none-equal condition sign
      pgrec = PgDBI.pgmget("rcrqst", "cindex", PgDBI.get_field_condition("cindex", PgOPT.params['CI'], 0, 1), PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("No Request Control matches given Index condition")
      PgOPT.params['CI'] = pgrec['cindex']

   PgOPT.OPTS['CI'][2] |= 8  # set validated flag

#
# get request index array from given request IDs
#
def rid2rindex(rqstids):

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
            pgrec = PgDBI.pgget("dsrqst", "rindex", "rqstid = '{}'".format(val), PgOPT.PGOPT['extlog'])
            if pgrec:
               indices[i] = pgrec['rindex']
            elif 'NR' in PgOPT.params and PgOPT.PGOPT['CACT'] == 'SR':
               indices[i] = 0
            elif PgOPT.PGOPT['CACT'] == 'SR':
               PgOPT.action_error("Request ID {} is not in RDADB,\nUse Mode Option ".format(val) +
                             "-NR (-NewRequest) to add new Request", 'SR')
            else:
               PgOPT.action_error("Request ID {} is not in RDADB".format(val))
      return indices
   else:   # found wildcard and/or none-equal condition sign
      pgrec = PgDBI.pgmget("dsrqst", "rindex", PgDBI.get_field_condition("rqstid", rqstids, 1, 1), PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("No Request matches given Request ID condition")
      return pgrec['rindex']

#
# get request ID array from given request indices
#
def rindex2rid(indices):

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
            pgrec = PgDBI.pgget("dsrqst", "rqstid", "rindex = {}".format(val), PgOPT.PGOPT['extlog'])
            if not pgrec: PgOPT.action_error("Request Index {} not in RDADB".format(val))
            rqstids[i] = pgrec['rqstid']
      return rqstids
   else:   # found none-equal condition sign
      pgrec = PgDBI.pgmget("dsrqst", "rqstid", PgDBI.get_field_condition("gindex", indices, 0, 1), PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("No Request matches given Index condition")
      return pgrec['rqstid']

#
# get dataset ids for given request indices
#
def get_request_dsids(ridxs):

   count = len(ridxs) if ridxs else 0
   dsids = [None]*count
   for i in range(count):
      ridx = ridxs[i]
      if i == 0 or (ridx != ridxs[i-1]):
         pgrec = PgDBI.pgget("dsrqst", "dsid", "rindex = {}".format(ridx), PgOPT.PGOPT['extlog'])
         if not pgrec: PgOPT.action_error("Request Index {} not in RDADB".format(ridx))
      dsids[i] = pgrec['dsid']

   return dsids

#
# get dataset ids for given request control indices
#
def get_control_dsids(cidxs):

   count = len(cidxs) if cidxs else 0
   dsids = [None]*count
   for i in range(count):
      cidx = cidxs[i]
      if i == 0 or (cidx != cidxs[i - 1]):
         pgrec = PgDBI.pgget("rcrqst", "dsid", "cindex = {}".format(cidx), PgOPT.PGOPT['extlog'])
         if not pgrec: PgOPT.action_error("Request Control Index {} not in RDADB".format(cidx)) 
      dsids[i] = pgrec['dsid']

   return dsids

#
# get file ids for given file names
#
def fname2fid(files, dsids, stypes):

   count = len(files) if files else 0
   fids = [0]*count   
   for i in range(count):
      file = files[i]
      if not file: continue   # missing file name
      dsid = dsids[i]
      type = stypes[i]
      if not type or type == 'W': type = 'D'
      pgrec = PgSplit.pgget_wfile(dsid, 'wid', "wfile = '{}' AND type = '{}'".format(file, type), PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("wfile_{}-{}: Error find Source File".format(dsid, file))
      fids[i] = pgrec['wid']

   return fids

#
# get file names from given file ids
#
def fid2fname(fids, dsids, stypes):

   count = len(fids) if fids else 0
   files = ['']*count
   for i in range(count):
      fid = fids[i]
      if not fid: continue   # missing file id
      stype = stypes[i] if stypes else ''
      dsid = dsids[i]
      condition = "wid = '{}'".format(fid)
      pgrec = PgSplit.pgget_wfile(dsid, 'wfile', condition, PgOPT.PGOPT['extlog'])
      if not pgrec: PgOPT.action_error("wfile_{}-{}: Error find Source File".format(dsid, fid))
      files[i] = pgrec['wfile']

   return files

#
# get WEB file path for given dsid and file name
# opt = 0 - relative path to PgOPT.params['WH']
#       1 - absolute path
#       2 - relative path to PgOPT.params['WH']/data/dsid
#
def get_file_path(fname, dpath, rtpath, opt = 0):

   if not rtpath: rtpath = PgOPT.params['WH']

   if fname:
      if re.search(r'^/', fname):
         if opt != 1 and re.search(r'^{}/'.format(rtpath), fname):
            fname = PgLOG.join_paths(rtpath, fname, 1)   # remove rtpath if exists 
            if opt == 2: fname = PgLOG.join_paths(dpath, fname, 1)   # remove webpath if exists         
      elif opt == 2:
         fname = PgLOG.join_paths(dpath, fname, 1)   # remove webpath if exists 
      else:
         fname = PgLOG.join_paths(dpath, fname)
         if opt == 1: fname = PgLOG.join_paths(rtpath, fname)
   elif opt == 0:
      fname = dpath
   elif opt == 1:
      fname = PgLOG.join_paths(rtpath, dpath)

   return fname

#
# check and see if enough disk space is allowed for the request
#
def request_limit():

#   pgrec = PgDBI.pgget("wfrqst", "round(sum(size)/1000000000, 0) s", "status = 'O'")

#   if pgrec and pgrec['s'] and pgrec['s'] > PgOPT.PGOPT['TS']:
#      PgLOG.pglog("Exceed Total Download Limit PgOPT.PGOPT['TS']GB", PgOPT.PGOPT['extlog'])
#      return 1 # reach total request limit
#   else:
      return 0 # OK to process request
#

#
# return: converted file name and error message
#
def convert_archive_format(pgfile, pgrqst, cmd, rstr):

   wfile = pgfile['wfile']
   ofile = pgfile['ofile']
   errmsg = None
   if pgrqst['file_format']: wfile = re.sub(r'\.{}'.format(pgrqst['file_format']), '', wfile, 1, re.I)
   fmsg = "{}-{}".format(rstr, wfile)
   pstat = check_processed(wfile, pgfile, pgrqst['dsid'], pgrqst['rindex'], rstr)
   if pstat > 0:
      PgLOG.pglog(fmsg + ": converted already", PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
      return (wfile, errmsg)
   elif pstat < 0:
      return (None, errmsg)

   pgsrc = PgSplit.pgget_wfile(pgrqst['dsid'], "wfile ofile, data_format, file_format", "wid = {}".format(pgfile['srcid']), PgLOG.LGEREX)
   if not pgsrc:
      errmsg = PgLOG.pglog("{}: Error get source record ({}-{})".format(fmsg, pgfile['srctype'], pgfile['srcid']), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
      return (wfile, errmsg)

   whome = PgLOG.join_paths(PgLOG.PGLOG['DSDHOME'], pgrqst['dsid'])
   ofile = PgLOG.join_paths(whome, pgsrc['ofile'])
   if not pgrqst['data_format']:
      if PgOPT.PGOPT['RCNTL'] and PgOPT.PGOPT['RCNTL']['data_format']: pgrqst['data_format'] = PgOPT.PGOPT['RCNTL']['data_format']
   if not pgrqst['file_format']:
      if PgOPT.PGOPT['RCNTL'] and PgOPT.PGOPT['RCNTL']['file_format']: pgrqst['file_format'] = PgOPT.PGOPT['RCNTL']['file_format']
   if not pgsrc['file_format']:
      errmsg = PgLOG.pglog("{}: miss original archive format to convert to {}".format(fmsg, pgrqst['data_format']), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
      return (wfile, errmsg)

   afmt = valid_archive_format(pgrqst['file_format'], pgsrc['file_format'], 1)
   if not cmd:
      errmsg = PgLOG.pglog("{}: miss archive format conversion command ({} to {})".format(fmsg, pgsrc['data_format'], pgrqst['data_format']), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
      return (wfile, errmsg)

   errmsg = ''
   syserr = "\n"
   acmd = "{} {}".format(cmd, ofile)
   wfile = PgLOG.pgsystem(acmd, PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG, 273)   # 273 = 1 +16 + 256
   if PgLOG.PGLOG['SYSERR']: syserr += PgLOG.PGLOG['SYSERR']

   if not wfile:
      errmsg = "{}: Error convert archive format{}".format(acmd, syserr)
      PgLOG.pglog(errmsg, PgOPT.PGOPT['errlog'])
   else:
      ms = re.match(r'^(.*)\n', wfile)
      if ms: wfile = ms.group(1)
      finfo = PgFile.check_local_file(wfile, 0, PgOPT.PGOPT['wrnlog'])
      if not finfo:
         errmsg = "{}: no file converted{}".format(acmd, syserr)
         PgLOG.pglog(errmsg, PgOPT.PGOPT['errlog'])
      elif finfo['data_size'] == 0:
         errmsg = "Empty file " + wfile

   return (wfile, errmsg)

#
# return: converted file name and error message
#
def convert_data_format(pgfile, pgrqst, cmd, rstr):

   wfile = pgfile['wfile']
   ofile = pgfile['ofile']
   errmsg = None
   if pgrqst['file_format']: wfile = re.sub(r'\.{}'.format(pgrqst['file_format']), '', wfile, 1, re.I)
   fmsg = "{}-{}".format(rstr, wfile)
   pstat = check_processed(wfile, pgfile, pgrqst['dsid'], pgrqst['rindex'], rstr)
   if pstat > 0:
      PgLOG.pglog(fmsg + ": converted already", PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
      return (wfile, errmsg)
   elif pstat < 0:
      return (None, errmsg)

   pgsrc = PgSplit.pgget_wfile(pgrqst['dsid'], "wfile ofile, data_format, file_format", "wid = {}".format(pgfile['srcid']), PgLOG.LGEREX)
   if not pgsrc:
      errmsg = PgLOG.pglog("{}: Error get source record ({}-{})".format(fmsg, pgfile['srctype'], pgfile['srcid']), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
      return (wfile, errmsg)

   if pgfile['srctype'] == "W":
      whome = PgLOG.join_paths(PgLOG.PGLOG['DSDHOME'], pgrqst['dsid'])
      ofile = PgLOG.join_paths(whome, pgsrc['ofile'])
   if not pgrqst['data_format']:
      if PgOPT.PGOPT['RCNTL'] and PgOPT.PGOPT['RCNTL']['data_format']: pgrqst['data_format'] = PgOPT.PGOPT['RCNTL']['data_format']
   if not pgrqst['file_format']:
      if PgOPT.PGOPT['RCNTL'] and PgOPT.PGOPT['RCNTL']['file_format']: pgrqst['file_format'] = PgOPT.PGOPT['RCNTL']['file_format']
   if not pgsrc['data_format']:
      errmsg = PgLOG.pglog("{}: miss original data format to convert to {}".format(fmsg, pgrqst['data_format']), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
      return (wfile, errmsg)

   afmt = valid_archive_format(pgrqst['file_format'], pgsrc['file_format'], 1)
   if afmt: wfile = re.sub(r'\.{}'.format(afmt), '', wfile, 1, re.I)
   if not cmd:
      if re.search(r'netcdf$', pgrqst['data_format'], re.I):
         cmd = "format_to_netcdf"
      else:
         errmsg = PgLOG.pglog("{}: miss format conversion command ({} to {})".format(fmsg, pgsrc['data_format'], pgrqst['data_format']), PgOPT.PGOPT['errlog']|PgLOG.RETMSG)
         return (wfile, errmsg)

   if pgsrc['file_format']:
      ext = get_format_extension(pgrqst['data_format'])
      errmsg = multiple_conversion(cmd, ofile, pgsrc['data_format'].lower(), pgsrc['file_format'], ext, wfile)
   else:
      errmsg = do_conversion("{} {} {}".format(cmd, ofile, pgsrc['data_format'].lower()), wfile)

   if afmt and not errmsg:
      wfile = PgFile.compress_local_file(wfile, afmt, 1)[0]
      finfo = PgFile.check_local_file(wfile, 7, PgOPT.PGOPT['wrnlog'])
      if not finfo:
         errmsg = "Error check " + wfile
      elif finfo['data_size'] == 0:
         errmsg = "Empty file " + wfile

   return (wfile, errmsg)

#
# get file extension for given data format
#
def get_format_extension(dfmt):
   
   DEXTS = {'netcdf' : ".nc", 'nc' : ".nc", 'grib' : ".grb", 'grb' : ".grb", 'hdf' : ".hdf"}

   for dkey in DEXTS:
      if re.search(r'{}'.format(dkey), dfmt, re.I):
         return DEXTS[dkey]

   return ''

#
# convert data format for a given file
#
# return '' if sucessful error mesage otherwise
#
def do_conversion(cmd, file):

   msg = ''
   err = "\n"
   PgLOG.PGLOG['STD2ERR'] = ["fatal:"]
   ret = PgLOG.pgsystem(cmd, PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG, 257)   # 257 = 1 + 256
   if PgLOG.PGLOG['SYSERR']: err += PgLOG.PGLOG['SYSERR']
   PgLOG.PGLOG['STD2ERR'] = []

   if not ret:
      msg = "{}: Error convert format{}".format(cmd, err)
      PgLOG.pglog(msg, PgOPT.PGOPT['errlog'])
   elif not PgFile.check_local_file(file, 0, PgOPT.PGOPT['wrnlog']):
      msg = "{}: no file converted{}".format(cmd, err)
      PgLOG.pglog(msg, PgOPT.PGOPT['errlog'])

   return msg

#
# convert data format for given file, keeping the archive format
#
# return 0 if sucessful error mesage otherwise
#
def multiple_conversion(cmd, ifile, dfmt, afmt, oext, ofile):

   iname = op.basename(ifile)
   wdir = iname + "_tmpdir"
   if op.exists(wdir): PgLOG.pgsystem("rm -rf " + wdir, PgOPT.PGOPT['extlog'], 5)
   PgFile.local_copy_local("{}/{}".format(wdir, iname), ifile, PgOPT.PGOPT['extlog'])
   PgFile.change_local_directory(wdir, PgOPT.PGOPT['extlog'])
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
         PgLOG.pgsystem("tar -xvf " + tfile, PgOPT.PGOPT['extlog'], 5)
         PgFile.delete_local_file(tfile, PgOPT.PGOPT['extlog'])
         acts[j] = 'tar'
         j += 1
         files[j] = PgFile.get_directory_files()
         cnts[j] = len(files[j])
      else:
         ms = re.search(r'^({})'.format(PgFile.CMPSTR), fmt, re.I)
         if ms:
            ext = ms.group(1)
            acts[j] = ext
            j += 1
            cnts[j] = cnts[j-1]
            files[j] = [None]*cnts[j]
            for i in range(cnts[j]):
               files[j][i] = PgFile.compress_local_file(tfiles[i], ext, 0, PgOPT.PGOPT['extlog'])[0]

   # convert data format now
   tfiles = files[j]
   for i in range(cnts[j]):
      tfile = tfiles[i]
      file = tfile + oext
      msg = do_conversion("{} {} {}".format(cmd, tfile, dfmt), tfile)
      if msg:
         PgFile.change_local_directory("../", PgOPT.PGOPT['extlog'])
         return msg
      dir = op.dirname(tfile)
      if dir and dir != ".":
         PgFile.move_local_file(file, op.basename(file), PgOPT.PGOPT['extlog'])
      PgFile.delete_local_file(tfile, PgOPT.PGOPT['extlog'])
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
         PgLOG.pgsystem("tar -cvf {} *".format(file), PgOPT.PGOPT['extlog'], 5)
         files[j][0] = file
      else:
         tfiles = files[j+1]
         for i in range(cnts[j]):
            files[j][i] = PgFile.compress_local_file(tfiles[i], acts[j], 1, PgOPT.PGOPT['extlog'])[0]

   PgFile.change_local_directory("../", PgOPT.PGOPT['extlog'])
   if op.exists(ofile): PgFile.delete_local_file(ofile, PgOPT.PGOPT['extlog'])
   PgFile.move_local_file(ofile, "{}/{}".format(wdir, files[0][0]), PgOPT.PGOPT['extlog'])
   PgFile.delete_local_file(wdir, PgOPT.PGOPT['extlog'])

   return ''  

#
# validate the given archive format (afmt) is needed or not
# against existing format (format)
# return the needed format if diff; otherwise, with the needed format appended 
#
def valid_archive_format(afmt, format, diff = 0):

   if afmt and format and re.search(r'(^|\.){}(\.|$)'.format(afmt), format, re.I): afmt = None
   if diff: return afmt

   if afmt:
      if format:
         format += '.' + afmt
      else:
         format = afmt

   return format

#
# format floating point values
#
def format_floats(recs, info, idx1, idx2):

   vals = recs['size']
   total = 0
   for i in range(idx1, idx2):
      val = vals[i]
      info['SIZ'][i] = val
      total += val
      vals[i] = format_one_float(val)

   return total

#
# format a float point value into string
#
def format_one_float(val):

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

#
# format dates
#
def format_dates(vals, idx1, idx2, fmt = None):
   
   if not fmt: fmt = "MM/DD/YYYY"

   for i in range(idx1, idx2):
      if not vals[i]: continue
      dates = re.split(r'-', str(vals[i]))
      vals[i] = PgUtil.fmtdate(int(dates[0]), int(dates[1]), int(dates[2]), fmt)

#
# set request file counts and total sizes
#
def set_request_count(rcnd, pgrqst = None, show = 0):

   record = {}
   if not pgrqst:
      # get the request count and size information if not given
      pgrqst = PgDBI.pgget("dsrqst", "fcount, size_request, tarflag, tarcount", rcnd, PgOPT.PGOPT['extlog'])
      if not pgrqst: return PgLOG.pglog("Error get file count/size info from 'dsrqst' for " + rcnd, PgOPT.PGOPT['errlog'])

   if not pgrqst['fcount']:
      fcnt = PgDBI.pgget("wfrqst", "", rcnd, PgOPT.PGOPT['extlog'])
      if fcnt: pgrqst['fcount'] = record['fcount'] = fcnt

   if pgrqst['tarflag'] and pgrqst['tarflag'] == 'Y' and not pgrqst['tarcount']:
      tarcnt = PgDBI.pgget("tfrqst", "", rcnd, PgOPT.PGOPT['extlog'])
      if tarcnt: pgrqst['tarcount'] = record['tarcount'] = tarcnt

   if not pgrqst['size_request']:
      pgrec = PgDBI.pgget("wfrqst", "sum(size) size_request", rcnd + " AND type = 'D'", PgOPT.PGOPT['extlog'])
      if pgrec and pgrec['size_request']: record['size_request'] = pgrec['size_request']

   if record and PgDBI.pgupdt("dsrqst", record, rcnd, PgOPT.PGOPT['extlog']) and show:
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
      PgLOG.pglog("{} set for Request {}".format(show, rcnd), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

   return pgrqst['fcount']   # always return the number of files

#
# check file processed or not wait if under processing by another request
# return 1 if processed already -1 under processing and 0 not exists
#
def check_processed(pfile, pgfile, dsid, cridx, rstr):

   wfile = pgfile['wfile']
   origin = (0 if wfile == pfile else 1)

   wcnd = "wfile = '{}' AND rindex <> {} AND ".format(wfile, cridx)
   pinfo = PgFile.check_local_file(pfile, 1, PgOPT.PGOPT['wrnlog'])
   winfo = (PgFile.check_local_file(wfile, 1, PgOPT.PGOPT['wrnlog']) if origin else pinfo)

   # check if web file exists already
   if winfo and winfo['data_size'] > 0:
      if (pgfile['size'] == winfo['data_size'] or 
          pgfile['time'] and pgfile['date'] == winfo['date_modified'] and
          pgfile['time'] == winfo['time_modified']):
         return 1

      pgrecs = PgDBI.pgmget("wfrqst", "rindex", "{}date = '{}' AND time = '{}'".format(wcnd, winfo['date_modified'], winfo['time_modified']), PgOPT.PGOPT['extlog'])
      cnt = len(pgrecs['rindex']) if pgrecs else 0
      for i in range(cnt):
         ridx = pgrecs['rindex'][i]
         if PgDBI.pgget("dsrqst", "", "rindex = {} AND dsid = '{}'".format(ridx, dsid), PgOPT.PGOPT['extlog']):
            return 1

   # check if under process
   pgrecs = PgDBI.pgmget("wfrqst", "rindex, pindex, pid", wcnd + "pid > 0", PgOPT.PGOPT['extlog'])
   if pgrecs:
      cnt = len(pgrecs['rindex'])
      for i in range(cnt):
         pid = pgrecs['pid'][i]
         if pgrecs['pindex'][i]:
            pidx = pgrecs['pindex'][i]
            pgrec = PgDBI.pgget("ptrqst", "lockhost", "pindex = {} AND pid = {}".format(pidx, pid), PgOPT.PGOPT['extlog'])
            if pgrec:
               PgLOG.pglog(("{}-{}: Locked by RPT{}".format(rstr, wfile, pidx)) +
                            PgLock.lock_process_info(pid, pgrec['lockhost']),
                           PgLOG.LOGWRN|PgLOG.FRCLOG)
               return -1
         else:
            ridx = pgrecs['rindex'][i]
            pgrec = PgDBI.pgget("dsrqst", "lockhost", "rindex = {} AND pid = {}".format(ridx, pid), PgOPT.PGOPT['extlog'])
            if pgrec:
               PgLOG.pglog(("{}-{}: Locked by Rqst{}".format(rstr, wfile, ridx)) +
                            PgLock.lock_process_info(pid, pgrec['lockhost']),
                           PgLOG.LOGWRN|PgLOG.FRCLOG)
               return -1
   
   if pinfo:
      if origin and pinfo['data_size'] > 0: return 1   # assume this is a good file
      PgFile.delete_local_file(pfile)  # clean the dead file

   return 0

#
# Fill request metrics into table dssdb.ousage for given request index and purge date
#
def fill_request_metrics(ridx, pgpurge):

   order = "r-{}".format(ridx)
   record = {}
   pgorder = PgDBI.pgget("ousage", "date_closed, size_input", "order_number = '{}'".format(order), PgOPT.PGOPT['extlog'])
   if pgorder:
      if pgorder['date_closed'] != pgpurge['date_purge'] or pgorder['size_input'] != pgpurge['size_input']:
         record['date_closed'] = pgpurge['date_purge']
         record['size_request'] = pgpurge['size_request']
         record['size_input'] = pgpurge['size_input']
         record['count'] = pgpurge['fcount']
         if PgDBI.pgupdt("ousage", record, "order_number = '{}'".format(order), PgOPT.PGOPT['extlog']):
            PgLOG.pglog("Request '{}' is updated for order metrics".format(ridx), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)
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
   if add_to_allusage(record, pgpurge['time_rqst']) and PgDBI.pgadd("ousage", record, PgOPT.PGOPT['extlog']):
      PgLOG.pglog("Request '{}' is recorded into order metrics".format(ridx), PgOPT.PGOPT['wrnlog']|PgLOG.FRCLOG)

#
# add request info into allusage
def add_to_allusage(record, time):

   pgrec = PgDBI.pgget("wuser",  "email, org_type, country, region",
                       "wuid = {}".format(record['wuid_request']), PgOPT.PGOPT['extlog'])
   if not pgrec: return 0
   pgrec['dsid'] = record['dsid']
   pgrec['org_type'] = PgDBI.get_org_type(pgrec['org_type'], pgrec['email'])
   pgrec['country'] = PgDBI.set_country_code(pgrec['email'], pgrec['country'])
   pgrec['date'] = record['date_request']
   pgrec['time'] = time
   pgrec['size'] = record['size_request']
   pgrec['method'] = record['method']
   pgrec['quarter'] = record['quarter']
   pgrec['ip'] = record['ip']
   pgrec['source'] = 'O'

   return PgDBI.add_yearly_allusage(None, pgrec)

#
# get request status
#
def request_status(rstat):

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

#
# cache request control information
#
def cache_request_control(ridx, pgrqst, action, pidx = 0):

   if not pgrqst['rqstid']: pgrqst['rqstid'] = add_request_id(ridx, pgrqst['email'], 1)
   if PgFile.check_host_down(PgLOG.PGLOG['RQSTHOME'], PgLOG.PGLOG['HOSTNAME'], PgOPT.PGOPT['errlog']):
      return None   # check if system down

   pgrec = PgOPT.PGOPT['RCNTL'] = None
   rtype = pgrqst['rqsttype']
   if rtype != 'C':
      gcnd = "dsid = '{}' AND gindex = ".format(pgrqst['dsid'])
      if rtype in "ST":
         tcnd = " AND (rqsttype = 'T' OR rqsttype = 'S')"
      else:
         tcnd = " AND rqsttype = '{}'".format(rtype)
   
      gindex = pgrqst['gindex']
      while True:
         pgrec = PgDBI.pgget("rcrqst", "*", "{}{}{}".format(gcnd, gindex, tcnd), PgOPT.PGOPT['errlog'])
         if not pgrec and gindex > 0:
            pgrec = PgDBI.pgget("dsgroup", "pindex", "{}{}".format(gcnd, gindex), PgOPT.PGOPT['errlog'])
            if pgrec:
               gindex = pgrec['pindex']
               continue
         break

   if not pgrec:
      pgrec = PgDBI.pgtable('rcrqst', PgOPT.PGOPT['extlog'])
      pgrec['rqsttype'] = rtype
      pgrec['gindex'] = pgrqst['gindex']

   if 'HN' in PgOPT.params: pgrec['hostname'] = PgOPT.params['HN'][0]
   if 'EO' in PgOPT.params: pgrec['empty_out'] = PgOPT.params['EO'][0]
   if 'PC' in PgOPT.params and PgOPT.params['PC'][0]:
      pgrec['command'] = PgOPT.params['PC'][0]
   elif pgrqst['command'] and pgrqst['command'] != pgrec['command']:
      pgrec['command'] = pgrqst['command']

   if pgrqst['ptlimit'] and pgrqst['ptlimit'] != pgrec['ptlimit']:
      pgrec['ptlimit'] = pgrqst['ptlimit']
   elif pgrqst['ptsize'] and pgrqst['ptsize'] != pgrec['ptsize']:
      pgrec['ptsize'] = pgrqst['ptsize']

   PgOPT.PGOPT['VP'] = PgOPT.params['VP'][0] if 'VP' in PgOPT.params else (pgrec['validperiod'] if pgrec['validperiod'] else PgOPT.PGOPT['DVP'])

   if pgrec['command']:
      pgrec['command'] += " {} ".format(ridx)
      if action != 'SP':
         if PgOPT.request_type(rtype, 1):
            pgrec['command'] += PgLOG.join_paths(PgOPT.params['WH'], "data/" + pgrqst['dsid'])
         else:
            pgrec['command'] += get_file_path(None, pgrqst['rqstid'], pgrqst['location'], 1)
         if pidx and action == 'PP':
            pgrec['command'] += " {}".format(pidx)

   PgOPT.PGOPT['RCNTL'] = pgrec
   return PgLOG.SUCCESS

#
# get golally defined table row color
#
def table_color(idx):
   
   tcolors = ("#CDBCDC", "#DFD8F8", "#EAEAFC", "#F8F6FE", "#E0C8B1")

   return tcolors[idx]

#
# add a unique request id
#
def add_request_id(ridx, email, updtdb = 0):

   unames = PgDBI.get_ruser_names(email)
   lname = PgLOG.convert_chars(unames['lstname'], 'RQST')
   rqstid = "{}{}".format(lname.upper(), ridx)
   if updtdb: PgDBI.pgexec("UPDATE dsrqst SET rqstid = '{}'".format(rqstid), PgOPT.PGOPT['extlog'])

   return rqstid

#
# expand request status info
#
def get_request_status(pgrecs, cnt = 0):

   if not cnt: cnt = (len(pgrecs['rindex']) if pgrecs else 0)
   rstats = pgrecs['status']
   for i in range(cnt):
      pgrec = PgUtil.onerecord(pgrecs, i)
      if rstats[i] == 'Q':
         ckrec = PgDBI.pgget("dscheck", "runhost, pid, lockhost, stttime",
                             "oindex = {} AND command = 'dsrqst' AND otype <> 'P'".format(pgrec['rindex']), PgOPT.PGOPT['extlog'])
         if pgrec['pid']:
            runhost = ""
            pcnt = 0
            percent = complete_request_percentage(pgrec, ckrec)
            if percent < 0:
               rstats[i] += " - pending"
            else:
               if percent > 0:
                  rstats[i] += " - {}% built".format(percent)
               else:
                  rstats[i] += " - building"
               if ckrec and ckrec['runhost']: runhost = ckrec['runhost']
               if pgrec['ptcount'] > 1 and pgrec['lockhost'] == "partition": pcnt = pgrec['ptcount']
            rstats[i] += PgLock.lock_process_info(pgrec['pid'], pgrec['lockhost'], runhost, pcnt)
         elif ckrec and ckrec['pid']:
            if ckrec['lockhost'] == PgLOG.PGLOG['PGBATCH']:
               rstats[i] += " - pending on {}({})".format(ckrec['lockhost'], ckrec['pid'])
            else:
               rstats[i] += " - building on {}({})".format(ckrec['lockhost'], ckrec['pid'])
         else:
            rstats[i] += " -  queued"
            if pgrec['hostname']: rstats[i] += " on " + pgrec['hostname']
      elif rstats[i] == 'O' and pgrec['location']:
         rstats[i] += " - " + request_status('F')
      else:
         rstats[i] += " - " + request_status(rstats[i])

   return rstats   

#
# get percentage of completion of request process
#
def complete_request_percentage(rqst, ckrec):

   if ckrec and not ckrec['stttime']: return -1

   percent = 0
   cnd = "rindex = {} AND status = 'O'".format(rqst['rindex'])
   if rqst['fcount'] and rqst['fcount'] > 0:
      cnt = rqst['pcount'] if rqst['pcount'] else PgDBI.pgget("wfrqst", "", cnd)
      if(cnt == 0 and rqst['rqstid'] and 
         (rqst['location'] or PgOPT.request_type(rqst['rqsttype'], 1) == 0)):
         files = glob.glob(get_file_path("*", rqst['rqstid'], rqst['location'], 1))
         cnt = len(files)
      if cnt > 0:
         if cnt < rqst['fcount']:
            percent = int(100*cnt/rqst['fcount'])
         else:
            percent = 99
   elif rqst['size_request'] > 0:
      pgrec = PgDBI.pgget("wfrqst", "sum(size) ts", cnd)
      if pgrec and pgrec['ts']:
         percent = int(100*pgrec['ts']/rqst['size_request'])
         if percent > 99: percent = 99

   return percent

#
# expand request partition status info
#
def get_partition_status(pgrecs, cnt = 0):

   if not cnt: cnt = (len(pgrecs['rindex']) if pgrecs else 0)
   rstats = [None]*cnt
   for i in range(cnt):
      pgrec = PgUtil.onerecord(pgrecs, i)
      rstats[i] = pgrec['status']
      if rstats[i] == 'Q':
         ckrec = PgDBI.pgget("dscheck", "runhost, pid, lockhost, stttime",
                             "oindex = {} AND command = 'dsrqst' AND otype = 'P'".format(pgrec['pindex']), PgOPT.PGOPT['extlog'])
         if pgrec['pid']:
            runhost = ""
            percent = complete_partition_percentage(pgrec, ckrec)
            if percent < 0:
               rstats[i] += " - pending"
            else:
               if percent > 0:
                  rstats[i] += " - {}% built".format(percent)
               else:
                  rstats[i] += " - building"
               if ckrec and ckrec['runhost']: runhost = ckrec['runhost']
            rstats[i] += PgLock.lock_process_info(pgrec['pid'], pgrec['lockhost'], runhost)
         elif ckrec and ckrec['pid']:
            if ckrec['lockhost'] == PgLOG.PGLOG['PGBATCH']:
               rstats[i] += " - pending on {}({})".format(ckrec['lockhost'], ckrec['pid'])
            else:
               rstats[i] += " - building on {}({})".format(ckrec['lockhost'], ckrec['pid'])
         else:
            rstats[i] += " -  queued"
            if pgrec['hostname']: rstats[i] += " on " + pgrec['hostname']
      else:
         rstats[i] += " - " + request_status(rstats[i])

   return rstats   

#
# get percentage of completion of a partition process
#
def complete_partition_percentage(part, ckrec):

   if ckrec and not ckrec['stttime']: return -1

   percent = 0
   if part['fcount'] > 0:
      cnt = PgDBI.pgget("wfrqst", "", "pindex = {} AND status = 'O'".format(part['pindex']))
      if cnt > 0:
         if cnt < part['fcount']:
            percent = int(100*cnt/part['fcount'])
         else:
            percent = 99

   return percent

#
# get md5 chechsum for requested file from source file record
#
def get_requested_checksum(dsid, pgfile):

   if pgfile['srcid'] and pgfile['ofile'] and pgfile['wfile'] == pgfile['ofile']:
      pgsrc = PgSplit.pgget_wfile(dsid, "data_size, checksum", "wid = {}".format(pgfile['srcid']), PgLOG.LGEREX)

      if pgsrc and pgsrc['checksum'] and pgsrc['data_size'] == pgfile['size']:
         return pgsrc['checksum']

   return None
