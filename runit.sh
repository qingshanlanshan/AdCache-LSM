cd ./MooseLSM/build
cmake -DCMAKE_PREFIX_PATH=/home/jiarui/AdCache/libtorch -DCMAKE_BUILD_TYPE=RELWITHDEBINFO ..
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




log_file=/home/jiarui/AdCache/log
result_file=/home/jiarui/AdCache/results
dir=/home/jiarui/AdCache/workload/workload_query
db_path=/home/jiarui/AdCache/db

prepare_file=/home/jiarui/AdCache/workload/dataset_100000000_entries.dat
num_threads=16

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
    ./tools/cache_test -workload="test" -worker_threads_num=${num_threads} -bpk=${bpk} -cache_size=${cache_size} -cache_style=${cache_style} -workload_file=${workload_file} -path=$path >> ${log_file}
}
rm $log_file


workload=dynamic
rm $result_file
cache_style=block
test_cachesize 10 25000000
mv /home/jiarui/AdCache/results /home/jiarui/AdCache/results_${cache_style}_100g

rm $result_file
cache_style=range
test_cachesize 10 25000000
mv /home/jiarui/AdCache/results /home/jiarui/AdCache/results_${cache_style}_100g

rm $result_file
cache_style=LRU
test_cachesize 10 25000000
mv /home/jiarui/AdCache/results /home/jiarui/AdCache/results_${cache_style}_100g

rm $result_file
cache_style=RLCache
test_cachesize 10 25000000
mv /home/jiarui/AdCache/results /home/jiarui/AdCache/results_${cache_style}_100g

rm $result_file
cache_style=lecar
test_cachesize 10 25000000
mv /home/jiarui/AdCache/results /home/jiarui/AdCache/results_${cache_style}_100g

rm $result_file
cache_style=cacheus
test_cachesize 10 25000000
mv /home/jiarui/AdCache/results /home/jiarui/AdCache/results_${cache_style}_100g