WORKLOAD=read_scan
# WORKLOAD=read_write
WORKLOAD=scan_write
WORKLOAD=scan
WORKLOAD=dynamic
# WORKLOAD=long_range
# WORKLOAD=test
# WORKLOAD=balanced
# WORKLOAD=read
# WORKLOAD=dynamic1

cd workload
python3 workload_generator.py --file-num 16 --config workload_cfg/${WORKLOAD}.cfg --prepare-num 100000000 --output workload_query/${WORKLOAD}_query.dat