#!/usr/bin/env python

import re
import sqlite3

SE_ID_MAP = {
   "UKI-LT2-IC-HEP-disk" : 1,
}

SE_NAME_MAP = {
    "UKI-LT2-IC-HEP-disk": "gfe02.grid.hep.ph.ic.ac.uk",
}

SE_BASE_MAP = {
    "UKI-LT2-IC-HEP-disk": "srm://gfe02.grid.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/t2k",
}

class Utils():

    PFN_REM_BASE = re.compile(r'(t2k|t2k.org|nd280)(/.*)')

    @staticmethod
    def se_id_to_name(se_id):
        for se_name, se_id in SE_ID_MAP.iteritems():
            if se_id == se_id:
                return se_name
        raise RuntimeError("Unknwon se_id '%d'" % se_id)

    @staticmethod
    def norm_pfn(pfn):
        pfn = pfn.replace('//', '/')
        pfn_match = Utils.PFN_REM_BASE.search(pfn)
        if not pfn_match:
            raise RuntimeError("Unable to process PFN '%s'?" % pfn)
        return pfn_match.group(2)
        return pfn

    @staticmethod
    def norm_size(fsize):
        return int(fsize)

    @staticmethod
    def norm_cksum(cksum):
        cksum = cksum.lower()
        return cksum.zfill(8)

class DB():
    """ All DB Entries. """

    def __init__(self, dbfile="data.db"):
        """ Open DB and create tables if needed. """
        self.__conn = sqlite3.connect(dbfile)
        cur = self.__conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS lfns (se_id INTEGER, lfn TEXT, pfn TEXT, PRIMARY KEY (se_id, lfn))")
        cur.execute("CREATE TABLE IF NOT EXISTS pfns (se_id INTEGER, pfn TEXT, fsize INTEGER, cksum TEXT, PRIMARY KEY (se_id, pfn))")
        cur.execute("CREATE TABLE IF NOT EXISTS dfns (se_id INTEGER, dfn TEXT, fsize INTEGER, cksum TEXT, PRIMARY KEY (se_id, dfn))")
        self.__conn.commit()
        cur.close()

    def __del__(self):
        self.__conn.close()

    def commit(self):
        self.__conn.commit()

    def add_pfn(self, se_id, pfn, fsize, cksum):
        pfn = Utils.norm_pfn(pfn)
        fsize = Utils.norm_size(fsize)
        cksum = Utils.norm_cksum(cksum)
        cur = self.__conn.cursor()
        try:
            cur.execute("INSERT INTO pfns VALUES (?, ?, ?, ?)", (se_id, pfn, fsize, cksum))
        except sqlite3.IntegrityError:
            # Oh no, key already exists?
            # Check if entry is the same...
            res = cur.execute("SELECT fsize, cksum FROM pfns WHERE se_id = ? AND pfn = ?", (se_id, pfn))
            for old_fsize, old_cksum in res:
              if old_fsize != fsize or old_cksum != cksum:
                raise RuntimeError("Failed to add PFN '%s', already exists? (se_id=%d)" % (pfn, se_id))
        cur.close()

    def rem_pfns_by_se(self, se_id):
        cur = self.__conn.cursor()
        cur.execute("DELETE FROM pfns WHERE se_id = ?", (se_id,))
        cur.close()
        self.__conn.commit()

    def iterpfns(self, se_id=None):
        cur = self.__conn.cursor()
        if se_id:
            res = cur.execute("SELECT se_id, pfn, fsize, cksum FROM pfns WHERE se_id = ?", (se_id,))
        else:
            res = cur.execute("SELECT se_id, pfn, fsize, cksum FROM pfns")
        for row in res:
            yield row
