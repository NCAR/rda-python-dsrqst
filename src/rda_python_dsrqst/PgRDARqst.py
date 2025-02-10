#
###############################################################################
#
#     Title : PgRDARqst.py
#
#    Author : Zaihua Ji,  zji@ucar.edu
#      Date : 09/25/2020
#             2025-02-10 transferred to package rda_python_dsrqst from
#             https://github.com/NCAR/rda-shared-libraries.git
#   Purpose : python library module for holding some functions for submitting request
#             on command line
#
#    Github : https://github.com/NCAR/rda-python-dsrqst.git
#
###############################################################################
#
import os
import re
from rda_python_common import PgLOG
from rda_python_common import PgUtil
from rda_python_common import PgFile
from rda_python_common import PgDBI
from rda_python_common import PgOPT
from . import PgRqst

UNAMES = CCEMAIL = VLDCMD = None
PTLIMIT = PTSIZE = 0

USG = (
"\n\nUsage for function rda_request(rqst, logact)\n\n" +
"  rqst - a dictionary array for request information:\n" +
"   'rtype'   : RequestType (mandatory)\n" +
"   'dsid'    : DatasetId (mandatory)\n" +
"   'gindex'  : GroupIndex (optional, default to 0)\n" +
"   'email'   : UserEmail (optional, default to UserLoginID\@ucar.edu)\n" +
"   'rstat'   : RequestStatus (optional, default to value in request control record)\n" +
"   'sflag'   : SubsetFlags (optional, 1 Variable, 2 - Temporal, 3 Spatial)\n" +
"   'tflag'   : TarFlag (Optional, default to N - No tar, Y-tar small files)\n" +
"   'dfmt'    : DataFormat (optional, specify for data format conversion)\n" +
"   'afmt'    : ArchiveFormat (optional, set to compress result)\n" +
" 'size_request' : DataSizerequested (optional)\n" +
" 'size_input'   : DataSizeInvolved (optional)\n" +
"   'fcount'  : NumberofFilesRequested (optional)\n" +
"   'ptlimit' : ParitionFileCountLimit (optional)\n" +
"   'ptsize'  : ParitionDataSizeLimit (optional)\n" +
"   'command' : RequestCommand (optional, command different from the one in request control)\n" +
" 'validsubset' : ValidationCommand (optional, command to validate rinfo at subset submission time)\n" +
"  'location' : RequestLocation (optional, default to current working directory)\n" +
"   'rinfo'   : RequesInfo (detail request information, mandatory for subset request)\n" +
"   'rnote'   : RequestNote (optional, readable version of 'rinfo')\n\n" +
"     logact - optional logging action flag, PgLOG.LOGWRN as default.\n\n" +
"      Return: message for the request being added.\n" +
" For example: msg = rda_request(rqst)\n")

SUSG = (
"\n\nUsage for function rda_request_status(ridx, email, logact)\n\n" +
"    ridx - Request index\n" +
"   email - User email address\n" +
"  logact - optional logging action flag, PgLOG.LOGWRN as default\n\n" +
"   Return: number of requests matched and a dictionary array for request status:\n" +
"  'rindex'       : Request Index\n" +
"  'rqstid'       : Request ID\n" +
"  'email'        : User Email\n" +
"  'dsid'         : Dataset Id\n" +
"  'gindex'       : Group Index (0 if no group)\n" +
"  'rqsttype'     : Request Type\n" +
"  'status'       : Request Status (include progress if under building)\n" +
"  'size_request' : Data Size requested\n" +
"  'size_input'   : Data Size Involved\n" +
"  'fcount'       : Number of Files requested\n" +
"  'ptlimit'      : file count limit for each partition\n" +
"  'ptsize'       : data size limit for each partition\n" +
"  'date_rqst'    : Date request submiited\n" +
"  'time_rqst'    : Time request submitted\n" +
"  'date_ready'   : Date request built\n" +
"  'time_ready'   : Time request built\n" +
"  'date_purge'   : Date request to be purged\n" +
"  'time_purge'   : Time request to be purged\n" +
"  'data_format'  : Data Format (example, NETCDF)\n" +
"  'file_format'  : Archive Foramt (example, GZ)\n" +
"  'specialist'   : Specialist who handles the request\n" +
"  'location'     : Path to the requested data\n" +
"  'rnote'        : Readable subsetting request detail\n" +
"  'rinfo'        : Subsetting request string\n" +
"For example:\n" +
"(cnt, pgrecs) = rda_request_status(ridx, email)\n" +
"if cnt == 2:\n" +
"   status0 = pgrecs['status'][0]\n" +
"   status1 = pgrecs['status'][1]\n" +
"\n")

#
# Function rda_request(rqst - a dictionary array for request information)
#
# add a RDA subset request; Run PgRDARqst.rda_request() to get help message
#
def rda_request(rqst = None, logact = PgLOG.LOGWRN):

   if not rqst:
      PgLOG.pglog(USG, PgLOG.WARNLG)
      return PgLOG.pglog("Miss request information in form of dictionary array", logact|PgLOG.RETMSG)

   pgrqst = {}    # intialize empty dictionary
   msg = common_request_info(pgrqst, rqst, logact)
   if msg: return msg
   msg = process_request_detail(pgrqst, rqst, logact)
   if msg: return msg
   msg = add_request_record(pgrqst, logact)
   if msg: return msg

   # retrieve new request record
   pgrqst = PgDBI.pgget("dsrqst", "*", "rindex = {}".format(pgrqst['rindex']), logact|PgLOG.EXITLG)

   msg = build_request_message(pgrqst, logact)
   send_request_email(pgrqst, msg, logact)

   return return_request_message(pgrqst, 1, logact) + msg

#
# fill up the common request info
#
def common_request_info(pgrqst, rqst, logact):

   global CCEMAIL, PTLIMIT, PTSIZE, UNAMES, VLDCMD
   wdir = rtype = dsid = email = None
   PTLIMIT = PTSIZE = 0
   gindex = 0
   if 'rtype' in rqst and rqst['rtype']: rtype = rqst['rtype']
   if not rtype:
      PgLOG.pglog(USG, PgLOG.WARNLG)
      return PgLOG.pglog("Miss request type to subset a request", logact|PgLOG.RETMSG)

   if 'dsid' in rqst and rqst['dsid']: dsid = PgUtil.format_dataset_id(rqst['dsid'])
   if not dsid:
      PgLOG.pglog(USG, PgLOG.WARNLG)
      return PgLOG.pglog("Miss dataset ID to submit a request", logact|PgLOG.RETMSG)

   if 'gindex' in rqst and rqst['gindex']: gindex = rqst['gindex']
   if not gindex and 'rinfo' in rqst and rqst['rinfo']:
      ms = re.search(r'(g|t)index=(\d+)',  rqst['rinfo'])
      if ms: gindex = int(ms.group(2))

   if 'email' in rqst and rqst['email']: email = rqst['email']
   if not email: email = PgLOG.PGLOG['CURUID'] + "@ucar.edu"
   UNAMES = PgDBI.get_ruser_names(email, 1)
   if not UNAMES: return "Register {} on https://rda.ucar.edu to subset data request".format(email)

   if 'location' in rqst and rqst['location']: wdir = rqst['location']
   if wdir:
      if wdir == "web":
         wdir = None
      elif not PgFile.check_local_file(wdir, 0, logact):
         return PgLOG.pglog(wdir + ": working directory not exists for submit a request", logact|PgLOG.RETMSG)
   else:
      wdir = os.getcwd()

   gcnd = "dsid = '{}' AND gindex = {}".format(dsid, gindex)
   if rtype == "T" or rtype == "S":
      tcnd = " AND (rqsttype = 'T' OR rqsttype = 'S')"
      ocnd = " ORDER BY rqsttype DESC"
   else:
      tcnd = " AND rqsttype = '{}'".format(rtype)
      ocnd = ""

   msg = dsid
   if gindex: msg += "_{}".format(gindex)

   i = 0
   while True:
      cnd = gcnd + tcnd + ocnd
      pgctl = PgDBI.pgget("rcrqst", "*", cnd, logact)
      if not pgctl:
         if gindex:
            pgrec = PgDBI.pgget("dsgroup", "pindex", gcnd, logact)
            if pgrec:
               gindex = pgrec['pindex']
               gcnd = "dsid = '{}' AND gindex = {}".format(dsid, gindex)
               i += 1
               continue
         elif i == 0:
            if ocnd:
               ocnd += ", gindex"
            else:
               ocnd = " ORDER BY gindex"
            gcnd = "dsid = '{}'".format(dsid)
            i += 1
            continue

         return PgLOG.pglog(msg + ": NO Request Control record found", logact|PgLOG.RETMSG)
      break

   if pgctl['maxrqst'] > 0:
      msg = allow_request(rtype, UNAMES, pgctl)
      if msg: return msg

   if pgctl['maxperiod'] and 'rinfo' in rqst and rqst['rinfo']:
      msg = valid_request_period(rtype, UNAMES, pgctl, rqst['rinfo'])
      if msg: return msg

   pgrqst['dsid'] = dsid
   pgrqst['email'] = email
   if wdir: pgrqst['location'] = wdir
   pgrqst['fromflag'] = rqst['fromflag'] if 'fromflag' in rqst else 'C'
   if 'ip' in rqst and rqst['ip']: pgrqst['ip'] = rqst['ip']

   # use values in control record
   pgrqst['rqsttype'] = pgctl['rqsttype']
   pgrqst['specialist'] = pgctl['specialist']
   pgrqst['cindex'] = pgctl['cindex']
   pgrqst['gindex'] = pgctl['gindex']
   CCEMAIL = pgctl['ccemail']
   VLDCMD = rqst['validsubset'] if 'validsubset' in rqst and rqst['validsubset'] else pgctl['validsubset']
   if 'command' in rqst and rqst['command']: pgrqst['command'] = rqst['command']

   if 'ptlimit' in rqst and rqst['ptlimit']:
      PTLIMIT = pgrqst['ptlimit'] = rqst['ptlimit']
   elif 'ptlimit' in pgctl and pgctl['ptlimit']:
      PTLIMIT = pgctl['ptlimit']
   elif 'ptsize' in rqst and rqst['ptsize']:
      PTSIZE = pgrqst['ptsize'] = rqst['ptsize']
   elif 'ptsize' in pgctl and pgctl['ptsize']:
      PTSIZE = pgctl['ptsize']

   # set request status
   if 'rstat' in rqst and rqst['rstat']:
      pgrqst['status'] = ('Q' if rqst['rstat'] == 'Q'  else 'W')
   elif pgctl['control'] == "A":
      pgrqst['status'] = "Q"
   else:
      pgrqst['status'] = "W"

   # set other request info
   hash = {'sflag' : "subflag", 'tflag' : "tarflag", 'dfmt' : "data_format", 'afmt' : "file_format", 'enote' : "enotice"}
   for key in hash:
      fld = hash[key]
      if key in rqst and rqst[fld]:
         pgrqst[fld] = rqst[key]
      elif fld in pgctl and pgctl[fld]:
         pgrqst[fld] = pgctl[fld]

   # make sure archive format in upper case
   if 'file_format' in pgrqst: pgrqst['file_format'] = pgrqst['file_format'].upper()

   return None

#
# check if allow request for the user
#
def allow_request(rtype, unames, pgctl):

   cnt = PgDBI.pgget("dsrqst", "", "cindex = {} AND email = '{}' AND status <> 'P'".format(pgctl['cindex'], unames['email']))

   if cnt < pgctl['maxrqst']:
      return None
   else:
      rstr = PgOPT.request_type(rtype)
      gstr = (" Product" if pgctl['gindex'] else '')
      return ("{}: Your {} request is denied since you have {} outstanding ".format(unames['name'], rstr, cnt) +
              "requests already for this Dataset{}. Try later.".format(gstr))

#
# check if a request temporal period is not exceeding the limit
#
def valid_request_period(rtype, unames, pgctl, rinfo):

   dates = None
   ms = re.search(r'dates=(\d+-\d+-\d+)( | \d+:\d+ )(\d+-\d+-\d+)', rinfo)
   if ms:
      dates = [ms.group(1), ms.group(3)]
   else:
      ms = re.search(r'startdate=(\d+-\d+-\d+)', rinfo)
      if ms:
         date1 = ms.group(1)
         ms = re.search(r'enddate=(\d+-\d+-\d+)', rinfo)
         if ms: dates = [date1, ms.group(1)]
   if not dates: return None      

   ms = re.match(r'^(\d+)([YMWD])$', pgctl['maxperiod'], re.I)
   if not ms: return None
   val = int(ms.group(1))
   unit = ms.group(2).upper()

   yr = mn = dy = 0
   if unit == 'Y':
      yr = val
      unit = 'Year'
   elif unit == 'M':
      mn = val
      unit = 'Month'
   elif unit == 'W':
      dy = 7*val
      unit = 'Week'
   else:
      dy = val
      unit = 'Day'
   if PgUtil.adddate(dates[0], yr, mn, dy) > dates[1]: return None

   pstr = "{} {}{}".format(val, unit, ('s' if val > 1 else ''))
   rstr = PgOPT.request_type(rtype)
   gstr = (" Product" if pgctl['gindex'] else '')
   return ("{}: Your {} request period, ".format(unames['name'], rstr) +
           "{} - {}, is longer than {} for ".format(dates[0], dates[1], pstr) +
           "this Dataset{}. Try a shorter data period.".format(gstr))

#
# process and fill up the detail request info based on the request type
#
def process_request_detail(pgrqst, rqst, logact):

   rtype = pgrqst['rqsttype']
   if rtype == "S" or rtype == "T":
      if 'rinfo' in rqst and rqst['rinfo']:
         pgrqst['rinfo'] = rqst['rinfo']
      else:
         PgLOG.pglog(USG, PgLOG.WARNLG)
         return PgLOG.pglog("Miss subset request detail for {}".format(rqst['dsid']), logact|PgLOG.RETMSG)

      if 'rnote' in rqst and rqst['rnote']:
         pgrqst['note'] = rqst['rnote']
      else:
         pgrqst['note'] = rqst['rinfo']

      msg = subset_request_submitted(pgrqst, logact)
      if msg: return msg
      if VLDCMD:
         msg = valid_request_info(pgrqst['dsid'], rqst['rinfo'], logact)
         if msg: return msg

   else:
      return PgLOG.pglog("Request type 'rtype' is not supported yet", logact|PgLOG.RETMSG)

   pgrqst['size_request'] = rqst['size_request'] if 'size_request' in rqst and rqst['size_request'] else 0
   pgrqst['size_input'] = rqst['size_input'] if 'size_input' in rqst and rqst['size_input'] else 0
   pgrqst['fcount'] = rqst['fcount'] if 'fcount' in rqst and rqst['fcount'] else 0
   pgrqst['ptcount'] = initialize_ptcount(pgrqst)

   return None

#
# validate the request info if validating command if provided
#
def valid_request_info(dsid, rinfo, logact):
   
   PgLOG.pgsystem("echo {} | {} {}".format(rinfo, VLDCMD, dsid), logact, 262+1024)

   return PgLOG.PGLOG['SYSERR']

#
# add one request record
#
def add_request_record(pgrqst, logact):
   
   nidx = new_request_id(logact)
   lname = PgLOG.convert_chars(UNAMES['lstname'], 'RQST').upper()
   pgrqst['rqstid'] = "{}{}".format(lname, nidx)   # set request ID
   (pgrqst['date_rqst'], pgrqst['time_rqst']) = PgUtil.get_date_time()
   ridx = PgDBI.pgadd("dsrqst", pgrqst, logact|PgLOG.EXITLG|PgLOG.AUTOID|PgLOG.DODFLT)
   if ridx > 0:
      if ridx != nidx:  # reset request ID
         record = {'rqstid' : "{}{}".format(lname, ridx)}
         PgDBI.pgupdt("dsrqst", record, "rindex = {}".format(ridx), logact|PgLOG.EXITLG)

      PgLOG.pglog("{}: Request Index {} added for <{}> {}".format(pgrqst['dsid'], ridx, UNAMES['name'], pgrqst['email']), PgLOG.LOGWRN)
      pgrqst['rindex'] = ridx
      return None
   else:
      return PgLOG.pglog("Fail to add request record for '{}'".format(pgrqst['dsid']), logact|PgLOG.RETMSG)

#
# find a unique request name/ID from given user last name
# by appending (existing maximum rindex + 1) 
#
def new_request_id(logact):

   pgrec = PgDBI.pgget("dsrqst", "MAX(rindex) maxid", '', logact)
   if pgrec:
      return (pgrec['maxid'] + 1)
   else:
      return 0

#
# check if the same request was submitted already
#
def subset_request_submitted(rqst, logact):

   pgrqst = PgDBI.pgget("dsrqst", "*", "dsid = '{}' AND gindex = {} ".format(rqst['dsid'], rqst['gindex']) +
                        "AND rqsttype = '{}' AND email = '{}' ".format(rqst['rqsttype'], rqst['email']) +
                        "AND rinfo = '{}'".format(rqst['rinfo']), logact|PgLOG.EXITLG)
   if not pgrqst: return None

   msg = build_request_message(pgrqst, logact)
   return return_request_message(pgrqst, 0, logact) + msg

#
# build a string message for a submitted request
#
def build_request_message(rqst, logact):

   ridx = rqst['rindex']
   dsid = rqst['dsid']
   rstr = PgOPT.request_type(rqst['rqsttype'])
   drec = PgDBI.pgget("dataset", "title", "dsid = '{}'".format(dsid), logact|PgLOG.EXITLG)

   buf = ("Request Summary:\n" +
          "Index    : {}\n".format(ridx) +
          "ID       : {}\n".format(rqst['rqstid']) +
          "Category : {}\n".format(rstr) +
          "Status   : {}\n".format(PgRqst.request_status(rqst['status'])) +
          "Dataset  : {}\n".format(dsid) +
          "Title    : {}\n".format(drec['title']) +
          "User     : {}\n".format(UNAMES['name']) +
          "Email    : {}\n".format(rqst['email']) +
          "Date     : {}\n".format(rqst['date_rqst']) +
          "Time     : {}\n".format(rqst['time_rqst']))
   if 'data_format' in rqst and rqst['data_format']: buf += "Format   : {}\n".format(rqst['data_format'])
   if 'file_format' in rqst and rqst['file_format']: buf += "Compress : {}\n".format(rqst['file_format'])

   if 'note' in rqst and rqst['note']:
      desc = rqst['note']
   elif 'rinfo' in rqst and rqst['rinfo']:
      desc = rqst['rinfo']
   else:
      desc = None

   if desc: buf += "Request Detail:\n{}\n".format(PgLOG.break_long_string(desc))

   if 'fcount' in rqst and rqst['fcount'] and 'size_input' in rqst and rqst['size_input']:
      s = 's' if rqst['fcount'] > 1 else ''
      buf += "\nTotal {} file{} ({}) requested.\n".format(rqst['fcount'], s, PgUtil.format_float_value(rqst['size_input']))

   return buf

#
# email request info to specialist
#
def send_request_email(rqst, msg, logact):
   
   if not CCEMAIL or CCEMAIL == 'N': return

   ridx = rqst['rindex']
   dsid = rqst['dsid']
   rstr = PgOPT.request_type(rqst['rqsttype'])
   PgLOG.add_carbon_copy(CCEMAIL, 1, "", rqst['specialist'])
   if PgLOG.PGLOG['CCDADDR']:
      receiver = PgLOG.PGLOG['CCDADDR']
      PgLOG.PGLOG['CCDADDR'] = ''
   else:
      receiver = rqst['specialist'] + "@ucar.edu"

   subject =  "{} Request '{}' of {}!".format(rstr, ridx, dsid)
   uname = "{} ({})".format(UNAMES['name'], rqst['email'])

   header = ("A {} Request '{}' is submmited for dataset '{}' ".format(rstr, ridx, dsid) +
             "from {} via command line. A summary of the request ".format(uname) +
             "information is given below.\n\n The Request is currently ")
   if rqst['status'] == "Q":
      header += ("granted and queued for processing. An email notice will be sent " +
                 "to you and {} when the requested data are ready.\n\n".format(uname))
   else:
      header += ("waiting your approval:\n\n" +
                 "  * To grant this Request you may click this link " +
                 "{}/#wqrqst?ridx={} or issue ".format(PgLOG.PGLOG['DSSURL'], ridx) +
                 "command 'dsrqst sr -ri {} -rs Q' to put this request in queue.\n\n".format(ridx) +
                 "  * To decline this request you execute 'dsrqst dl -ri {}' to ".format(ridx) +
                 "remove the request from RDADB and please, for courtsey, reply " +
                 "this email to explain why this Request is refused.\n\n")

   PgLOG.send_email(subject, receiver, header + msg, rqst['email'], PgLOG.LOGWRN)

#
# create and return the request message back to caller
#
def return_request_message(rqst, success, logact):

   ridx = rqst['rindex']
   dsid = rqst['dsid']
   rstr = PgOPT.request_type(rqst['rqsttype'])
   title  = "{} Request {}".format(rstr, ridx)
   name = rqst['specialist']
   email = name + "@ucar.edu"
   rec = PgDBI.pgget("dssgrp", "lstname, fstname", "logname = '{}'".format(name), logact)
   if rec: name = "{} {}".format(rec['fstname'], rec['lstname'])

   msg = "{}:\n\nYour {} request has been ".format(title, rstr)
   if success:
      msg += ("submitted successfully.\nA summary of your request is given below.\n\n" +
              "Your request will be processed soon. You will be informed via email\n" +
              "when the data is ready to be picked up.\n" +
              "\nYou may check request status of data requests you have submitted via " +
              "the web link\n{}/#ckrqst\n".format(PgLOG.PGLOG['DSSURL']))
   else:
      msg += ("DECLINED since you have summitted\na same request already as " +
              "in the summary shown below.\n")
      if rqst['status'] == "O":
         msg += ("\nYour previous Request ridx is available under\n" +
                 "{} until {}.\n".format(rqst['location'], rqst['date_purge']))

   msg += ("\nIf the information is CORRECT no further action is need.\n" +
           "If the information is NOT CORRECT, or if you have additional comments\n" +
           "you may email to {} ({}) with corrections or comments.\n\n".format(email, name))

   return msg

#
# Function rda_request_status(ridx  - Request Index)
# expand request status info; Run PgRDARqst.rda_request() to get help message.
#
def rda_request_status(ridx = 0, email = None, logact = PgLOG.ERRLOG):

   if ridx and not isinstance(ridx, int):
      ms = re.match(r'^\D+(\d+)$', ridx)
      if ms:
         ridx = int(ms.group(1))
      elif re.match(r'^.+@.+\.\w+$', ridx):
         email = ridx
         ridx = 0
      else:
         ridx = int(ridx)

   if ridx:
      cnd = "rindex = {}".format(ridx)
   elif email:
      cnd = "email = '{}' ORDER BY rindex".format(email)
   else:
      PgLOG.pglog(SUSG, PgLOG.WARNLG)
      return (0, None)

   pgrecs = PgLOG.pgmget("dsrqst", "*", cnd, logact)
   if pgrecs:
      cnt = len(pgrecs['rindex'])
      if cnt > 0: pgrecs['status'] = PgRqst.get_request_status(pgrecs)
   else:
      cnt = 0

   return (cnt, pgrecs)

#
# initilize partition count
# return 0 for setting partition dynamically later; 1 not further partitioning
#
def initialize_ptcount(pgrqst):

   if PTLIMIT:
      fcount = 0
      if 'fcount' in pgrqst and pgrqst['fcount']: fcount = pgrqst['fcount']
      if not fcount or fcount > PTLIMIT: return 0
   elif PTSIZE:
      fsize = 0
      if 'size_input' in pgrqst and  pgrqst['size_input']:
         fsize = pgrqst['size_input']
      elif 'size_request' in pgrqst and  pgrqst['size_request']:
         fsize = pgrqst['size_request']
      if not fsize or fsize > PTSIZE: return 0

   return 1
