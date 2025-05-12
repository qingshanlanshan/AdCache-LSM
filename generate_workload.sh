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
python3 workload_generator.py --config ${WORKLOAD}.cfg --prepare workload_datset_20g.dat --output ${WORKLOAD}_query.dat