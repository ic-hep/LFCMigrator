#!/usr/bin/env python
# Manager dump file imports

import sys

from dblib import SE_ID_MAP, DB, Utils

def load_se(db, dump_file, se_name):
    
    if not se_name in SE_ID_MAP:
        print >>sys.stderr, "Error: Unknown DIRAC SE name '%s'." % se_name
        return
    se_id = SE_ID_MAP[se_name]
    cksum_first = False # True if checksum is before size in file
    lines = 1
    try:
        fd = open(dump_file, 'r')
        print "Info: Dump file '%s' opened." % dump_file
        print "Info: Detecting column order..."
        # First work out the order of the size/cksum by inspecting 100 lines
        for i in xrange(1, 101):
          lines = i
          line = fd.readline().strip()
          if not line: break # File doesn't contain 100 lines!
          pfn, field1, field2 = line.split(" ")
          if not field1.isdigit():
            cksum_first = True # We found something non-numberic in the column
        if cksum_first:
          print "Info: File uses 'pfn cksum size' format."
        else:
          print "Info: File uses 'pfn size cksum' format."
        # Reset the file and import the data
        fd.seek(0)
        print "Info: Importing records..."
        lines = 1
        for line in fd.readlines():
            line = line.strip()
            pfn, fsize, cksum = line.split(" ")
            if cksum_first:
                # Swap around the size & cksum columns
                tmp = fsize
                fsize = cksum
                cksum = tmp
            db.add_pfn(se_id, pfn, fsize, cksum)
            if not lines % 10000:
                 print "Info: %d records imported..." % lines
            lines += 1
        fd.close()
        print "Info: Import complete (%d lines processed)." % lines
        print "Info: Committing Database."
        db.commit()
    except IOError as err:
        print >>sys.stderr, "Error reading dump file: %s" % str(err)
        print >>sys.stderr, "(%d lines read when error occured.)" % lines
        raise
    except ValueError as err:
        print >>sys.stderr, "Error on line %d: %s" % (lines, str(err))
        raise
    except RuntimeError as err:
        print >>sys.stderr, "Error: %s (line %s)" % (str(err), lines)
        raise
    print "Info: All done."

def clear_se(db, se_name):
    if not se_name in SE_ID_MAP:
        print >>sys.stderr, "Error: Unknown DIRAC SE name '%s'." % se_name
        return
    se_id = SE_ID_MAP[se_name]
    print "Info: Removing all PFNs for '%s' (%d)" % (se_name, se_id)
    db.rem_pfns_by_se(se_id)
    print "Info: All done."

def se_stats(db):
    se_counts = {}
    for se_id, pfn, fsize, cksum in db.iterpfns():
        if se_id in se_counts:
            se_counts[se_id] += 1
        else:
            se_counts[se_id] = 1
    print "Num files on SE"
    for se_id, count in se_counts.iteritems():
        print "%s%s" % (Utils.se_id_to_name(se_id).ljust(20), str(count).rjust(10))
    print ""

def load_lfc(db, fname):
    lines = 1
    try:
        fd = open(fname, "r")
        print "Info: Dump file '%s' open, processing..." % fname
        for line in fd.readlines():
            line = line.strip()
            lfn, full_pfn = line.split(" ")
            se_id, pfn = Utils.split_full_pfn(full_pfn)
            if se_id is not None: 
                db.add_lfn(se_id, lfn, pfn)
            if not lines % 100000:
                 print "Info: %d LFC records imported..." % lines
            lines += 1
        fd.close()
        print "Info: Committing Database."
        db.commit()
    except IOError as err:
        print >>sys.stderr, "Error reading LFC dump: %s" % str(err)
        print >>sys.stderr, "(%d lines read when error occured.)" % lines
        raise
    except ValueError as err:
        print >>sys.stderr, "Error on line %d: %s" % (lines, str(err))
        raise
    except RuntimeError as err:
        print >>sys.stderr, "Error: %s (line %s)" % (str(err), lines)
        raise
    print "Info: All done."

def lfc_stats(db):
    pass

def load_dfc(db, fname):
    pass

def dfc_stats(db):
    pass

def usage(errtxt=None):
    """ Print usage information and exit. """
    if errtxt:
        print >>sys.stderr, "Error: %s" % errtxt
        print >>sys.stderr, ""
    print >>sys.stderr, "se_dump: Handle SE dump files & data"
    print >>sys.stderr, "  Usage: se_dump.py <action> <action_opts>"
    print >>sys.stderr, "  Actions:"
    print >>sys.stderr, "    load_se <dump_file> <dirac_se_name> - Loads an SE dump file"
    print >>sys.stderr, "    clear_se <dirac_se_name> - Deletes all entries for an SE"
    print >>sys.stderr, "    se_stats - Print statistics about SE data"
    print >>sys.stderr, "    load_lfc <lfc_file> - Loads an LFC dump file"
    print >>sys.stderr, "    lfc_stats - Print stats about LFC data"
    print >>sys.stderr, "    load_dfc <dfc_file> - Loads a DFC dump file"
    print >>sys.stderr, "    dfc_stats - Print stats about DFC data"
    print >>sys.stderr, ""
    sys.exit(0)

def main():
    """ Process options and run process. """
    opt_list = [
      ('load_se',   load_se,   2),
      ('clear_se',  clear_se,  1),
      ('se_stats',  se_stats,  0),
      ('load_lfc',  load_lfc,  1),
      ('lfc_stats', lfc_stats, 0),
      ('load_dfc',  load_dfc,  1),
      ('dfc_stats', dfc_stats, 0),
    ]
    db = DB()
    if len(sys.argv) < 2:
      usage()
    for fcn_name, fcn_ptr, num_args in opt_list:
      if fcn_name == sys.argv[1]:
        if len(sys.argv) != num_args + 2:
           usage("Wrong number of arguments for function.")
        fcn_ptr(db, *sys.argv[2:])
        return
    usage("Unknown function '%s'.")

if __name__ == '__main__':
    main()
