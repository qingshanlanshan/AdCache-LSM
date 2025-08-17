# WORKLOAD=read
# WORKLOAD=short_scan
# WORKLOAD=long_scan
# WORKLOAD=balanced
WORKLOAD=dynamic
# WORKLOAD=test

cd workload

python3 workload_generator.py --file-num 1 \
    --config workload_cfg/${WORKLOAD}.cfg \
    --key-range 100000000 \
    --output workload_query/${WORKLOAD}_query.dat
