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

def load_dfc(db, fname):
    lines = 1
    bad_lines = 0
    try:
        fd = open(fname, "r")
        print "Info: DFC file '%s' open, processing..." % fname
        for line in fd.readlines():
            line = line.strip()
            fsize, cksum, full_pfn = line.split(" ", 3)
            try:
                se_id, pfn = Utils.split_full_pfn(full_pfn)
            except ValueError:
                # User has stored a corrupt PFN in the catalog
                # Ignore it
                lines += 1
                bad_lines += 1
                continue
            if se_id is not None:
                db.add_dfn(se_id, pfn, fsize, cksum)
            if not lines % 10000:
                 print "Info: %d DFC records imported..." % lines
            lines += 1
        fd.close()
        print "Info: Imported %d lines (%d contianed bad PFN)" % (lines, bad_lines)
        print "Info: Committing Database."
        db.commit()
    except IOError as err:
        print >>sys.stderr, "Error reading DFC dump: %s" % str(err)
        print >>sys.stderr, "(%d lines read when error occured.)" % lines
        raise
    except ValueError as err:
        print >>sys.stderr, "Error on line %d: %s" % (lines, str(err))
        raise
    except RuntimeError as err:
        print >>sys.stderr, "Error: %s (line %s)" % (str(err), lines)
        raise
    print "Info: All done."

def stats(db):
    print ""
    # PFN
    pfn_counts = {}
    for se_id, pfn, fsize, cksum in db.iterpfns():
        if se_id in pfn_counts:
            pfn_counts[se_id] += 1
        else:
            pfn_counts[se_id] = 1
    print "Num files on SE"
    print "==============="
    for se_id, count in pfn_counts.iteritems():
        print "%s%s" % (Utils.se_id_to_name(se_id).ljust(32), str(count).rjust(10))
    print ""
    # LFN 
    lfn_counts = {}
    for se_id, lfn, pfn in db.iterlfns():
        if se_id in lfn_counts:
            lfn_counts[se_id] += 1
        else:
            lfn_counts[se_id] = 1
    print "Num files on LFC"
    print "================"
    for se_id, count in lfn_counts.iteritems():
        print "%s%s" % (Utils.se_id_to_name(se_id).ljust(32), str(count).rjust(10))
    print ""
    # DFC 
    dfn_counts = {}
    for se_id, dfn, fsize, cksum in db.iterdfns():
        if se_id in dfn_counts:
            dfn_counts[se_id] += 1
        else:
            dfn_counts[se_id] = 1
    print "Num files on DFC"
    print "================"
    for se_id, count in dfn_counts.iteritems():
        print "%s%s" % (Utils.se_id_to_name(se_id).ljust(32), str(count).rjust(10))
    print ""

def conflicts(db, fname="conflicts_%s.txt"):
    print "Generating list of LFC/DFC conflicts..."
    direct = True
    dir_counts = {}
    indir_counts = {}
    for i in ('direct', 'indirect',):
        print "Writing %s conflicts LFNs to '%s'..." % (i, fname % i)
        fd = open(fname % i, "w")
        if direct:
            se_counts = dir_counts
        else:
            se_counts = indir_counts
        for dfc_se_id,lfc_se_id, lfn in db.iterconflicts(direct=direct):
            fd.write("%s %s %s\n" % (lfn, Utils.se_id_to_name(dfc_se_id), Utils.se_id_to_name(lfc_se_id)))
            if dfc_se_id in se_counts:
                se_counts[dfc_se_id] += 1
            else:
                se_counts[dfc_se_id] = 1
        fd.close()
        print "Completed %s." % i
        direct = False
    print ""
    print "Direct Conflicts by SE"
    print "======================"
    for se_id, count in dir_counts.iteritems():
        print "%s%s" % (Utils.se_id_to_name(se_id).ljust(32), str(count).rjust(10))
    print ""
    print "Indirect Conflicts by SE"
    print "========================"
    for se_id, count in indir_counts.iteritems():
        print "%s%s" % (Utils.se_id_to_name(se_id).ljust(32), str(count).rjust(10))

def movelists(db, se_name):
    if not se_name in SE_ID_MAP:
        print >>sys.stderr, "Error: Unknown DIRAC SE name '%s'." % se_name
        return
    se_id = SE_ID_MAP[se_name]
    # Counters
    normal_move = 0
    special_move = 0
    no_move = 0
    print "Generating move lists for %s (%d)..." % (se_name, se_id)
    # TODO: Write move list files
    for lfn, pfn in db.itermoves(se_id):
        if lfn == "/t2k.org%s" % pfn:
            normal_move += 1
        else:
            special_move += 1
    print "Summarising unmoved files..."
    for lfn, pfn in db.itermoves(se_id, nomove=True):
        no_move += 1
    print "Complete."
    print ""
    print "Summary"
    print "======="
    print "Files to move: %d" % normal_move
    print "Files to rename (special): %d" % special_move
    print "Files without move: %d" % no_move
    print "Total: %d" % (normal_move + special_move + no_move)
    print ""

def darkdata(db, se_name):
    if not se_name in SE_ID_MAP:
        print >>sys.stderr, "Error: Unknown DIRAC SE name '%s'." % se_name
        return
    se_id = SE_ID_MAP[se_name]
    print "Generating dark data list for %s..." % se_name
    fname = "%s_dark.txt" % se_name
    print "Info: Output file is '%s'." % fname
    fd = open(fname, "w")
    pfn_count = 0
    for pfn in db.iterdarkdata(se_id):
        fd.write("%s\n" % pfn)
        pfn_count += 1
    fd.close()
    print "Info: %d dark files found." % pfn_count
    print "All done."

def badrepl(db):
    print "Finding bad LFC replicas..."
    fname_bad = "bad_repls.txt"
    fname_missing = "missing_repls.txt"
    num_bad = 0
    num_missing = 0
    print "INFO: Writing bad LFNs to '%s'..." % fname_bad
    print "INFO: Writing missing LFNs to '%s'..." % fname_missing
    fd_bad = open(fname_bad, "w")
    fd_missing = open(fname_missing, "w")
    for pfn, num_sizes, num_cksums in db.iterrepl():
        if num_sizes > 1 or num_cksums > 1:
            num_bad += 1
            fd_bad.write("%s\n" % pfn)
        if num_sizes < 1 or num_cksums < 1:
            num_missing += 1
            fd_missing.write("%s\n" % pfn)
    fd_bad.close()
    fd_missing.close()
    print "INFO: Found %d bad replicas." % num_bad
    print "INFO: Found %d missing replicas." % num_missing
    print "All done."

def usage(errtxt=None):
    """ Print usage information and exit. """
    if errtxt:
        print >>sys.stderr, "Error: %s" % errtxt
        print >>sys.stderr, ""
    print >>sys.stderr, "dump_util: Do stuff with LFC, DFC & SE data"
    print >>sys.stderr, "  Usage: dump_util.py <action> <action_opts>"
    print >>sys.stderr, "  Import Actions:"
    print >>sys.stderr, "    load_se <dump_file> <dirac_se_name> - Loads an SE dump file"
    print >>sys.stderr, "    clear_se <dirac_se_name> - Deletes all entries for an SE"
    print >>sys.stderr, "    load_lfc <lfc_file> - Loads an LFC dump file"
    print >>sys.stderr, "    load_dfc <dfc_file> - Loads a DFC dump file"
    print >>sys.stderr, "    stats - Print stats about imported data"
    print >>sys.stderr, ""
    print >>sys.stderr, " Processing Actions:"
    print >>sys.stderr, "    conflicts - Generate a list of DFC/LFC conflicts"
    print >>sys.stderr, "    movelists <dirac_se_name> - Generate move files for an SE"
    print >>sys.stderr, "    darkdata <dirac_se_name> - Generate a dark file list for an SE"
    print >>sys.stderr, "    badrepl - Finds bad and missing LFC replicas"
    #print >>sys.stderr, "    register <dirac_se_name> - Generate registration lists for an SE"
    sys.exit(0)

def main():
    """ Process options and run process. """
    opt_list = [
      ('load_se',   load_se,   2),
      ('clear_se',  clear_se,  1),
      ('load_lfc',  load_lfc,  1),
      ('load_dfc',  load_dfc,  1),
      ('stats',     stats,     0),
      ('conflicts', conflicts, 0),
      ('movelists', movelists, 1),
      ('darkdata',  darkdata,  1),
      ('badrepl',   badrepl,   0),
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
