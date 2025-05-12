cd ./MooseLSM/build
cmake -DCMAKE_PREFIX_PATH=/home/jiarui/CacheLSM/libtorch -DCMAKE_BUILD_TYPE=RELWITHDEBINFO ..
# cmake -DCMAKE_BUILD_TYPE=DEBUG ..
# cmake -DCMAKE_BUILD_TYPE=RELWITHDEBINFO ..
# sudo make install
# make all -j
make cache_test -j
make ldb -j
make simple_test -j
make torch_pretrain -j
rc=$? 
if [ $rc -ne 0 ]; then
    echo "make failed"
    exit $rc
fi
K=1024
M=$((1024 * $K))
G=$((1024 * $M))




log_file=/home/jiarui/CacheLSM/log
result_file=/home/jiarui/CacheLSM/results
dir=/home/jiarui/CacheLSM/workload
db_path=/home/jiarui/CacheLSM/db_20g
# db_path=/home/jiarui/CacheLSM/db_4g

prepare_file=/home/jiarui/CacheLSM/workload/workload_datset_20g.dat
# prepare_file=/home/jiarui/CacheLSM/workload/workload_datset_4g.dat

# workload=read_scan
# workload=read_write
# workload=scan_write
# workload=scan
workload=dynamic
# workload=long_range
# workload=test
# workload=balanced
# workload=read
# workload=dynamic1
# workload=skewness_9

cache_style=block
# cache_style=range
# cache_style=LRU
# cache_style=lecar
# cache_style=RLCache
# cache_style=heap

test_cachesize(){
    echo $workload >> $log_file
    echo $workload >> $result_file
    workload_file=${dir}/${workload}_query.dat
    n_lines=$(wc -l < ${prepare_file})
    n_queries=$(wc -l < ${workload_file})
    bpk=$1
    cache_size=$2
    path=$db_path
    echo n_entry: $n_lines >> $result_file
    echo n_query: $n_queries >> $result_file
    if [ ! -d "$path" ]; then
        ./tools/cache_test -workload="prepare" -bpk=${bpk} -workload_file=${prepare_file} -path=$path >> ${log_file}
    fi
    # sudo perf record --strict-freq -F 10000 -g -o /home/jiarui/CacheLSM/perf.data \
    # valgrind --leak-check=full -v --log-file=/home/jiarui/CacheLSM/valgrind-out.txt \
    ./tools/cache_test -workload="test" -bpk=${bpk} -cache_size=${cache_size} -cache_style=${cache_style} -workload_file=${workload_file} -path=$path >> ${log_file}
    # sudo perf script -i ../../perf.data > ../../perf.data.script
}
rm $result_file
rm $log_file
if [[ $cache_style == "RLCache" ]]; then
    ./tools/torch_pretrain 
fi

# test_cachesize 10 100000
# test_cachesize 10 200000
# test_cachesize 10 400000
# test_cachesize 10 800000
# test_cachesize 10 1600000

# test_cachesize 10 500000
# test_cachesize 10 1000000
# test_cachesize 10 2000000
# test_cachesize 10 4000000
# test_cachesize 10 8000000


# cd ../..
# python test.py

# ./tools/simple_test

# workload=dynamic1
# rm $result_file
# cache_style=block
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_1

# rm $result_file
# cache_style=range
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_1

# rm $result_file
# cache_style=LRU
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_1

# rm $result_file
# cache_style=RLCache
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_1

# rm $result_file
# cache_style=lecar
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_1


# workload=dynamic2
# rm $result_file
# cache_style=block
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_2

# rm $result_file
# cache_style=range
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_2

# rm $result_file
# cache_style=LRU
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_2

# rm $result_file
# cache_style=RLCache
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_2

# rm $result_file
# cache_style=lecar
# test_cachesize 10 1600000
# cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_2

# test_skewness(){
#     cache_size=$1
#     # skewness
#     skewness_workloads=("skewness_5" "skewness_7" "skewness_9" "skewness_95" "skewness_99" "skewness_12")
#     for skewness_workload in "${skewness_workloads[@]}"; do
#         echo $skewness_workload
#         workload=$skewness_workload
#         rm $result_file
#         test_cachesize 10 $cache_size
#         cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_${workload}
#     done
# }

# cache_style=RLCache
# test_skewness 1600000


workload=dynamic
rm $result_file
cache_style=block
test_cachesize 10 8000000
cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_20g

rm $result_file
cache_style=range
test_cachesize 10 8000000
cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_20g

rm $result_file
cache_style=LRU
test_cachesize 10 8000000
cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_20g

rm $result_file
cache_style=RLCache
test_cachesize 10 8000000
cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_20g

rm $result_file
cache_style=lecar
test_cachesize 10 8000000
cp /home/jiarui/CacheLSM/results /home/jiarui/CacheLSM/results_${cache_style}_20g
