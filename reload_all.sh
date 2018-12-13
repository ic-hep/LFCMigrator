#!/bin/bash

set -e
set -x

rm -f data.db
./dump_util.py load_dfc data/dfc.data
./dump_util.py load_lfc data/lfc.data
./dump_util.py load_se data/ic.dump UKI-LT2-IC-HEP-disk
./dump_util.py load_se data/lancs.dump UKI-NORTHGRID-LANCS-HEP-disk
./dump_util.py load_se data/pic.dump pic-disk
./dump_util.py load_se data/qmul.dump UKI-LT2-QMUL2-disk
./dump_util.py load_se data/ralpp.dump UKI-SOUTHGRID-RALPP-disk
./dump_util.py load_se data/ralt1.dump RAL-LCG2-T2K-tape

