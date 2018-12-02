#!/usr/bin/env python

import posix
import MySQLdb as mysql
import MySQLdb.cursors as cursors
import os, sys

USERNAME = "root"
PASSWORD = ""
HOSTNAME = ""
DBNAME = "cns_db_18"
BATCH = 5

class LFCDB(object):

  def __findRoot(self):
    """ Get the root inode. """
    cur = self.__db.cursor()
    cur.execute('SELECT fileid FROM cns_file_metadata WHERE parent_fileid = 0')
    root = cur.fetchone()[0]
    cur.close()
    return root

  def __init__(self):
    """ Connects to the DB and sets CWD to /. """
    self.__db = mysql.connect(HOSTNAME, user=USERNAME,
                              passwd=PASSWORD, db=DBNAME, cursorclass=cursors.SSCursor)
    self.__db2 = mysql.connect(HOSTNAME, user=USERNAME,
                               passwd=PASSWORD, db=DBNAME)
    self.__root = self.__findRoot()
    self.__dircache = {}


  def get_all_inodes(self):
    cur = self.__db.cursor()
    cur.execute("SELECT fileid,sfn FROM cns_file_replica")
    while True:
      rows = cur.fetchmany(BATCH)
      if not rows:
        break
      for res in rows:
        yield res
    cur.close()

  def get_lfn_by_inode(self, inode, leaf=True):
    if inode == self.__root:
      return '/'
    if inode in self.__dircache:
      return self.__dircache[inode]

    cur = self.__db2.cursor()
    cur.execute("SELECT parent_fileid,name FROM cns_file_metadata WHERE fileid = %s", (inode, ))
    res = cur.fetchone()
    cur.close()
    if res is None:
      #print "DANGER: Orphan: %d!" % inode
      return None
    parent_name = self.get_lfn_by_inode(res[0], leaf=False)
    if not parent_name:
      return None
    full_path = os.path.join(parent_name, res[1])
    if not leaf:
      self.__dircache[inode] = full_path
    return full_path

def main():

  lfc = LFCDB()

  for inode, sfn in lfc.get_all_inodes():
    lfn = lfc.get_lfn_by_inode(inode)
    if not lfn:
      # Orphaned file, ignore
      continue
    if not lfn.startswith('/grid/t2k.org'):
      continue
    print lfn, sfn


if __name__ == '__main__':
  main()
