#!/usr/bin/env python

import re
import sqlite3

SE_ID_MAP = {
   "UKI-LT2-IC-HEP-disk" : 1,
   "UKI-LT2-QMUL2-disk" : 2,
   "RAL-LCG2-T2K-tape": 3,
   "UKI-NORTHGRID-LIV-HEP-disk": 4,
   "CA-TRIUMF-T2K1-disk": 5,
   "IFIC-LCG2-disk": 6,
   "UKI-NORTHGRID-SHEF-HEP-disk": 7,
   "pic-disk": 8,
   "UKI-NORTHGRID-LANCS-HEP-disk": 9,
   "UKI-SOUTHGRID-OX-HEP-disk": 10,
   "JP-KEK-CRC-02-disk": 11,
   "UKI-SOUTHGRID-RALPP-disk": 12,
   "UNIBE-LHEP-disk": 13,
   "UKI-NORTHGRID-MAN-HEP-disk": 14,
}

SE_NAME_MAP = {
    "gfe02.grid.hep.ph.ic.ac.uk": "UKI-LT2-IC-HEP-disk",
    "gfe02.hep.ph.ic.ac.uk": "UKI-LT2-IC-HEP-disk",
    "se03.esc.qmul.ac.uk": "UKI-LT2-QMUL2-disk",
    "ccsrm02.in2p3.fr": None,
    "srm-gen.gridpp.rl.ac.uk": None,
    "srm-t2k.gridpp.rl.ac.uk": "RAL-LCG2-T2K-tape",
    "hepgrid11.ph.liv.ac.uk": "UKI-NORTHGRID-LIV-HEP-disk",
    "t2ksrm.nd280.org": "CA-TRIUMF-T2K1-disk",
    "srmv2.ific.uv.es": "IFIC-LCG2-disk",
    "lcgse0.shef.ac.uk": "UKI-NORTHGRID-SHEF-HEP-disk",
    "srm.pic.es": "pic-disk",
    "fal-pygrid-30.lancs.ac.uk": "UKI-NORTHGRID-LANCS-HEP-disk",
    "t2se01.physics.ox.ac.uk": "UKI-SOUTHGRID-OX-HEP-disk",
    "kek2-se01.cc.kek.jp": "JP-KEK-CRC-02-disk",
    "kek2-tmpse.cc.kek.jp": None,
    "kek2-se.cc.kek.jp": None,
    "se04.esc.qmul.ac.uk": None,
    "heplnx204.pp.rl.ac.uk": "UKI-SOUTHGRID-RALPP-disk",
    "dpm.lhep.unibe.ch": "UNIBE-LHEP-disk",
    "srmcms.pic.es": None,
    "bohr3226.tier2.hep.manchester.ac.uk": "UKI-NORTHGRID-MAN-HEP-disk",
}

SE_BASE_MAP = {
    "UKI-LT2-IC-HEP-disk": "srm://gfe02.grid.hep.ph.ic.ac.uk:8443/srm/managerv2?SFN=/pnfs/hep.ph.ic.ac.uk/data/t2k",
    "UKI-LT2-QMUL2-disk": "srm://se03.esc.qmul.ac.uk:8444/srm/managerv2?SFN=/t2k.org",
    "RAL-LCG2-T2K-tape": "srm://srm-t2k.gridpp.rl.ac.uk:8443/srm/managerv2?SFN=/castor/ads.rl.ac.uk/prod",
    "UKI-NORTHGRID-LIV-HEP-disk": "srm://hepgrid11.ph.liv.ac.uk:8446/srm/managerv2?SFN=/dpm/ph.liv.ac.uk/home/t2k.org",
    "CA-TRIUMF-T2K1-disk": "srm://t2ksrm.nd280.org:8443/srm/managerv2?SFN=/nd280data", 
    "IFIC-LCG2-disk": "srm://srmv2.ific.uv.es:8443/srm/managerv2?SFN=/lustre/ific.uv.es/grid/t2k.org",
    "UKI-NORTHGRID-SHEF-HEP-disk": "srm://lcgse0.shef.ac.uk:8446/srm/managerv2?SFN=/dpm/shef.ac.uk/home/t2k.org",
    "pic-disk": "srm://srm.pic.es:8443/srm/managerv2?SFN=/pnfs/pic.es/data/t2k.org",
    "UKI-NORTHGRID-LANCS-HEP-disk": "srm://fal-pygrid-30.lancs.ac.uk:8446/srm/managerv2?SFN=/dpm/lancs.ac.uk/home/t2k.org",
    "UKI-SOUTHGRID-OX-HEP-disk": "srm://t2se01.physics.ox.ac.uk:8446/srm/managerv2?SFN=/dpm/physics.ox.ac.uk/home/t2k.org",
    "JP-KEK-CRC-02-disk": "srm://kek2-se01.cc.kek.jp:8444/srm/managerv2?SFN=/t2k.org",
    "UKI-SOUTHGRID-RALPP-disk": "srm://heplnx204.pp.rl.ac.uk:8443/srm/managerv2?SFN=/pnfs/pp.rl.ac.uk/data/t2k",
    "UNIBE-LHEP-disk": "srm://dpm.lhep.unibe.ch:8446/srm/managerv2?SFN=/dpm/lhep.unibe.ch/home/t2k.org",
    "UKI-NORTHGRID-MAN-HEP-disk": "srm://bohr3226.tier2.hep.manchester.ac.uk:8446/srm/managerv2?SFN=/dpm/tier2.hep.manchester.ac.uk/home/t2k.org",
}

class Utils():

    PFN_REM_BASE = re.compile(r'(t2k|t2k.org|nd280data)(/.*)')

    @staticmethod
    def se_id_to_name(se_id):
        for se_name, map_se_id in SE_ID_MAP.iteritems():
            if se_id == map_se_id:
                return se_name
        raise RuntimeError("Unknwon se_id '%d'" % se_id)

    @staticmethod
    def split_full_pfn(full_pfn):
        proto, rest = full_pfn.split("://")
        hostname, pfn = rest.split("/", 1)
        # hostname may contain a port number
        if ":" in hostname:
            hostname = hostname.split(":", 1)[0]
        # pfn may contain a WS endpoint
        if "SFN=" in pfn:
            pfn = pfn.split("SFN=", 1)[1]
        if not hostname in SE_NAME_MAP:
            raise RuntimeError("No mapping for SE '%s' (PFN: '%s')" % (hostname, full_pfn))
        se_name = SE_NAME_MAP[hostname]
        se_id = None
        if se_name:
            se_id = SE_ID_MAP[se_name]
        return se_id, "/%s" % pfn

    @staticmethod
    def lfn_to_pfn(lfn, se_name):
        """ Convert an LFN to a DIRAC compatible PFN at site se_id. """
        return SE_BASE_MAP[se_name] + lfn

    @staticmethod
    def norm_lfn(lfn):
        lfn = lfn.replace('//', '/')
        if lfn.startswith('/grid'):
           lfn = lfn[5:]
        return lfn

    @staticmethod
    def norm_pfn(pfn, se_id=None):
        pfn = pfn.replace('//', '/')
        pfn_match = Utils.PFN_REM_BASE.search(pfn)
        if not pfn_match:
            # Some PFNs don't contain any base path (due to a bug in some tool?)
            #raise RuntimeError("Unable to process PFN '%s'?" % pfn)
            # Return just the pfn
            res = pfn
            if '/' not in pfn:
                res = "/%s" % pfn
        else:
            res = pfn_match.group(2)
        if se_id == SE_ID_MAP['RAL-LCG2-T2K-tape']:
            # RAL is special, the base path doesn't include t2k.org
            # We have to add that bit back to our PFN path
            res = "/t2k.org%s" % res
        return res

    @staticmethod
    def norm_size(fsize):
        return int(fsize)

    @staticmethod
    def norm_cksum(cksum):
        if not cksum:
            return None
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
        pfn = Utils.norm_pfn(pfn, se_id)
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
        finally:
            cur.close()

    def rem_pfns_by_se(self, se_id):
        cur = self.__conn.cursor()
        cur.execute("DELETE FROM pfns WHERE se_id = ?", (se_id,))
        cur.close()
        self.__conn.commit()

    def add_lfn(self, se_id, lfn, pfn):
        lfn = Utils.norm_lfn(lfn)
        pfn = Utils.norm_pfn(pfn, se_id)
        cur = self.__conn.cursor()
        try:
            cur.execute("INSERT INTO lfns VALUES (?, ?, ?)", (se_id, lfn, pfn))
        except sqlite3.IntegrityError:
            res = cur.execute("SELECT pfn FROM lfns WHERE se_id = ? AND lfn = ?", (se_id, lfn))
            for old_pfn in res:
              old_pfn = old_pfn[0]
              if old_pfn != pfn:
                raise RuntimeError("Failed to add LFN '%s', already exists? (se_id=%d, pfn='%s' != '%s')" % (lfn, se_id, pfn, old_pfn))
        finally:
            cur.close()

    def add_dfn(self, se_id, pfn, fsize, cksum):
        pfn = Utils.norm_pfn(pfn, se_id)
        # Normalised PFNs _are_ LFNs in DIRAC
        fsize = Utils.norm_size(fsize)
        cksum = Utils.norm_cksum(cksum)
        cur = self.__conn.cursor()
        try:
            cur.execute("INSERT INTO dfns VALUES (?, ?, ?, ?)", (se_id, pfn, fsize, cksum))
        except sqlite3.IntegrityError:
            res = cur.execute("SELECT fsize, cksum FROM dfns WHERE se_id = ? AND dfn = ?", (se_id, pfn))
            for old_fsize, old_cksum in res:
              if old_fsize != fsize or old_cksum != cksum:
                raise RuntimeError("Failed to add DFN '%s', already exists? (se_id=%d)" % (pfn, se_id))
        finally:
            cur.close()

    def iterpfns(self, se_id=None):
        cur = self.__conn.cursor()
        if se_id:
            res = cur.execute("SELECT se_id, pfn, fsize, cksum FROM pfns WHERE se_id = ?", (se_id,))
        else:
            res = cur.execute("SELECT se_id, pfn, fsize, cksum FROM pfns")
        for row in res:
            yield row
        cur.close()

    def iterlfns(self, se_id=None):
        cur = self.__conn.cursor()
        if se_id:
            res = cur.execute("SELECT se_id, lfn, pfn FROM lfns WHERE se_id = ?", (se_id,))
        else:
            res = cur.execute("SELECT se_id, lfn, pfn FROM lfns")
        for row in res:
            yield row
        cur.close()

    def iterdfns(self, se_id=None):
        cur = self.__conn.cursor()
        if se_id:
            res = cur.execute("SELECT se_id, dfn, fsize, cksum FROM dfns WHERE se_id = ?", (se_id,))
        else:
            res = cur.execute("SELECT se_id, dfn, fsize, cksum FROM dfns")
        for row in res:
            yield row
        cur.close()

    def iterconflicts(self, direct=False):
        cur = self.__conn.cursor()
        if direct:
          res = cur.execute("""SELECT dfns.se_id,lfns.se_id,lfn FROM dfns, lfns WHERE dfns.dfn == "/t2k.org"||lfns.pfn AND dfns.se_id = lfns.se_id""")
        else:
          res = cur.execute("""SELECT dfns.se_id,lfns.se_id,lfn FROM dfns, lfns WHERE dfns.dfn == "/t2k.org"||lfns.pfn""")
        for row in res:
            yield row
        cur.close()

    def itermoves(self, se_id, nomove=False):
        cur = self.__conn.cursor()
        if nomove:
            res = cur.execute("""SELECT lfn,pfns.pfn FROM lfns INNER JOIN pfns ON lfns.pfn = "/t2k.org"||pfns.pfn AND lfns.se_id = pfns.se_id WHERE lfns.se_id = ?""", (se_id, ))
        else:
            res = cur.execute("""SELECT lfn,pfns.pfn FROM lfns INNER JOIN pfns ON lfns.pfn = pfns.pfn AND lfns.se_id = pfns.se_id WHERE lfns.se_id = ?""", (se_id, ))
        for row in res:
            yield row
        cur.close()

    def iterdarkdata(self, se_id):
        cur = self.__conn.cursor()
        res = cur.execute("""SELECT pfn FROM pfns WHERE pfns.pfn NOT IN (SELECT dfn FROM dfns WHERE se_id=?) AND pfns.pfn NOT IN (SELECT pfn FROM lfns WHERE se_id=?) AND se_id=?""", (se_id, se_id, se_id,))
        for row in res:
            yield row
        cur.close()

    def iterrepl(self):
        cur = self.__conn.cursor()
        res = cur.execute("""SELECT lfns.pfn,COUNT(DISTINCT fsize),COUNT(DISTINCT cksum) FROM lfns OUTER LEFT JOIN pfns ON lfns.pfn = pfns.pfn GROUP BY lfns.pfn""")
        for row in res:
            yield row
        cur.close()

    def iterregister(self, se_id):
        cur = self.__conn.cursor()
        res = cur.execute("""SELECT lfn,fsize,cksum FROM lfns INNER JOIN pfns ON lfns.pfn = pfns.pfn AND lfns.se_id = pfns.se_id WHERE lfns.se_id = ?""", (se_id, ))
        for row in res:
            yield row
        cur.close()
        
