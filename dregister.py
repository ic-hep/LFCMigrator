#!/usr/bin/env python

import sys
import uuid
# DIRAC does not work otherwise
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
# end of DIRAC setup

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

def addreplica(lfn, pfn, checksum, fsize, site, fc):
  infoDict = {}
  infoDict['PFN'] = pfn
  infoDict['Size'] = int(fsize)
  infoDict['SE'] = site
  infoDict['GUID'] = str(uuid.uuid4())
  infoDict['Checksum'] = checksum   
  fileDict = {}
  fileDict[lfn] = infoDict
  result = fc.addReplica(fileDict)
  if not result["OK"]:
    print "Failed to add replica: %s" % str(result)
    return False
  res = result["Value"]
  if res["Failed"]:
    print "Failed to update replica list: %s" % str(result)
    return False
  return True

def updatefile(lfn, pfn, checksum, fsize, site, fc):
  result = fc.getFileMetadata(lfn)
  if not result["OK"]:
    print "Failed to get file metadata: %s" % str(result)
    return False
  res = result["Value"]["Successful"][lfn]
  old_checksum = res["Checksum"]
  old_fsize = res["Size"]
  if long(fsize) != long(old_fsize):
    print "File size mismatch (%s != %s)" % (fsize, old_fsize)
    return False
  if checksum.upper() != old_checksum.upper():
    print "File checksum mismatch (%s != %s)" % (checksum, old_checksum)
    return False
  # Check where it's already registered
  result = fc.getReplicas(lfn)
  if not result["OK"]:
    print "Failed to get file replicas: %s" % str(result)
    return False
  res = result["Value"]["Successful"][lfn]
  if site in res:
    # Already registered at this site
    return True
  # Everything matches & not registered, update replica...
  return addreplica(lfn, pfn, checksum, fsize, site, fc)

def registerfile(lfn, pfn, checksum, fsize, site, fc):
  infoDict = {}
  infoDict['PFN'] = pfn
  infoDict['Size'] = int(fsize)
  infoDict['SE'] = site
  infoDict['GUID'] = str(uuid.uuid4())
  infoDict['Checksum'] = checksum   
  fileDict = {}
  fileDict[lfn] = infoDict
  # to do: check if file is there
  result = fc.addFile(fileDict) 
  if not result["OK"]:
    print result
    return False
  res = result["Value"]
  if res["Failed"]:
    msg = res["Failed"][lfn]["FileCatalog"]
    if "File already registered" in msg:
      return updatefile(lfn, pfn, checksum, fsize, site, fc)
    else:
      print "Unknown error: %s" % str(res)
      return False
  return True

def main():
  if len(sys.argv) != 3:
    print "Usage: dregister.py <DIRAC_Site> <File_List_File>"
    print ""
    sys.exit(1)
  site = sys.argv[1]
  fname = sys.argv[2]

  fc = FileCatalog()
  fd = open(fname, "r")
  i = 0

  for line in fd:
    line = line.strip()
    lfn, pfn, fsize, checksum = line.split(' ')
    if i % 100 == 0:
      print i, lfn, pfn, checksum, fsize, site
    res = registerfile(lfn, pfn, checksum, fsize, site, fc)
    if not res:
      print "Error with '%s', stopping." % lfn
      break
    i += 1
  fd.close()
    

if __name__ == "__main__":
  main()
