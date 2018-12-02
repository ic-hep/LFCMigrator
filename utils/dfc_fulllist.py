#!/usr/bin/env python

import posix
import MySQLdb as mysql
import MySQLdb.cursors as cursors
import os, sys

USERNAME = "root"
PASSWORD = ""
HOSTNAME = ""
DBNAME = "FileCatalogDB"
BATCH = 5

class DFCDB(object):

  def __init__(self):
    """ Connects to the DB and sets CWD to /. """
    self.__db = mysql.connect(HOSTNAME, user=USERNAME,
                              passwd=PASSWORD, db=DBNAME, cursorclass=cursors.SSCursor, autocommit=True)
    self.__db2 = mysql.connect(HOSTNAME, user=USERNAME,
                               passwd=PASSWORD, db=DBNAME, autocommit=True)

  def get_all_inodes(self):
    # Returns FileID, Size, Checksum, PFN
    cur = self.__db.cursor()
    cur.execute("SELECT FC_Files.FileID,Size,Checksum,PFN FROM FC_Replicas, FC_ReplicaInfo, FC_Files, FC_FileInfo WHERE FC_Replicas.RepID=FC_ReplicaInfo.RepID AND FC_Files.FileID=FC_Replicas.FileID AND FC_Files.FileID=FC_FileInfo.FileID")
    while True:
      rows = cur.fetchmany(BATCH)
      if not rows:
        break
      for res in rows:
        yield res
    cur.close()

def main():

  dfc = DFCDB()

  for inode, fsize, cksum, pfn in dfc.get_all_inodes():
    if not '/t2k.org/' in pfn:
      # Not a T2K file
      continue
    print inode, fsize, cksum, pfn

if __name__ == '__main__':
  main()
