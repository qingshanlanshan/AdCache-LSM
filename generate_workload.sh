# WORKLOAD=read
# WORKLOAD=short_scan
# WORKLOAD=long_scan
# WORKLOAD=balanced
# WORKLOAD=dynamic
WORKLOAD=test

cd workload
# 100G
python3 workload_generator.py --file-num 16 --config workload_cfg/${WORKLOAD}.cfg --key-range 4000000 --output workload_query/${WORKLOAD}_query.dat
