cd ./MooseLSM/build
cmake -DCMAKE_PREFIX_PATH=/home/jiarui/AdCache/libtorch -DCMAKE_BUILD_TYPE=RELWITHDEBINFO ..
# cmake -DCMAKE_BUILD_TYPE=DEBUG ..
# cmake -DCMAKE_BUILD_TYPE=RELWITHDEBINFO ..
# sudo make install
# make all -j
set -e 
make sst_dump -j
make ldb -j
make simple_test -j
make torch_pretrain -j
make cache_test -j

echo "make done"
K=1024
M=$((1024 * $K))
G=$((1024 * $M))




log_file=/home/jiarui/AdCache/log
result_file=/home/jiarui/AdCache/results
dir=/home/jiarui/AdCache/workload/workload_query
db_path=/home/jiarui/AdCache/db
result_path=/home/jiarui/AdCache/results_logs

prepare_file=/home/jiarui/AdCache/workload/dataset.dat
num_threads=16

test_cachesize(){
    rm $log_file
    rm $result_file
    echo $workload >> $log_file
    echo $workload >> $result_file
    workload_file=${dir}/${workload}_query.dat
    n_lines=$(wc -l < ${prepare_file})
    n_queries=$(wc -l < ${workload_file}_0)
    cache_style=$1
    cache_size=$2
    path=$db_path
    echo n_entry: $n_lines >> $result_file
    echo n_query: $n_queries >> $result_file
    if [ ! -d "$path" ]; then
        ./tools/cache_test -workload="prepare" -workload_file=${prepare_file} -path=$path >> ${log_file}
    fi
    ./tools/cache_test -workload="test" -worker_threads_num=${num_threads} -cache_size=${cache_size} -cache_style=${cache_style} -workload_file=${workload_file} -path=$path >> ${log_file}
    
    echo "saving results to ${result_path}/results_${cache_style}_${workload}_${cache_size}_${num_threads}.log"
    echo "saving logs to ${result_path}/log_${cache_style}_${workload}_${cache_size}_${num_threads}.log"
    cp $result_file $result_path/results_${cache_style}_${workload}_${cache_size}_${num_threads}.log
    cp $log_file $result_path/log_${cache_style}_${workload}_${cache_size}_${num_threads}.log
}

workload=read
# test_cachesize adcache 100000
# test_cachesize adcache 200000
# test_cachesize adcache 400000
# test_cachesize adcache 800000
# test_cachesize adcache 1600000

workload=long_scan
# test_cachesize adcache 100000
# test_cachesize adcache 200000
# test_cachesize adcache 400000
# test_cachesize adcache 800000
# test_cachesize adcache 1600000

workload=short_scan
# test_cachesize adcache 100000
# test_cachesize cacheus 200000
# test_cachesize cacheus 400000
# test_cachesize cacheus 800000
# test_cachesize cacheus 1600000

workload=balanced
# test_cachesize adcache 100000
# test_cachesize cacheus 200000
# test_cachesize cacheus 400000
# test_cachesize adcache 800000
# test_cachesize cacheus 1600000

workload=dynamic
# test_cachesize block 25000000
# test_cachesize range 25000000
# test_cachesize kv 25000000
# test_cachesize lecar 25000000
# test_cachesize cacheus 25000000
# test_cachesize adcache 25000000
