from dataclasses import dataclass
import random
import zipf
import argparse

@dataclass
class Workload:
    num_queries: int
    read_ratio: float
    short_scan_ratio: float
    long_scan_ratio: float

    def __post_init__(self):
        self.num_queries = int(self.num_queries)
        self.read_ratio = float(self.read_ratio)
        self.short_scan_ratio = float(self.short_scan_ratio)
        self.long_scan_ratio = float(self.long_scan_ratio)
    
    
def get_config(config_file: str) -> list[Workload]:
    with open(config_file, 'r') as f:
        lines = f.readlines()
    res:list[Workload] = []
    for line in lines:
        if line.startswith('#'):
            continue
        line = line.strip()
        res.append(Workload(*map(str, line.split())))
    for workload in res:
        print(f"Workload: {workload.num_queries} queries, {workload.read_ratio} read ratio, {workload.short_scan_ratio} short scan ratio, {workload.long_scan_ratio} long scan ratio")
    return res

def get_db(db_file: str) -> list[str]:
    with open(db_file, 'r') as f:
        lines = f.readlines()
    return [line.strip().split()[-1] for line in lines]

def generate_workload(workload: Workload, db: list[str], output_file: str) -> None:
    queries = []
    
    # read
    num_read = int(workload.num_queries * workload.read_ratio)
    dist = zipf.Zipf(db, 0.9)
    t = dist.sample(num_read)
    unique_keys = set()
    for i in t:
        queries.append(f"READ {i}")
        unique_keys.add(i)
    print(f"Unique keys in read: {len(unique_keys)}")
    
    # short scan
    num_scan = int(workload.num_queries * (workload.short_scan_ratio + workload.long_scan_ratio))
    t = dist.sample(num_scan)
    unique_keys = set()
    scan_length_list = []
    for i in t:
        scan_length = 16
        if random.random() < workload.long_scan_ratio / (workload.short_scan_ratio + workload.long_scan_ratio):
            scan_length = 64
        queries.append(f"SCAN {i} {scan_length}")
        scan_length_list.append(scan_length)
        unique_keys.add(i)
        
        
    if len(scan_length_list) > 0:
        print(f"Unique keys in scan: {len(unique_keys)}")
        print(f"Average scan length: {sum(scan_length_list) / len(scan_length_list)}")
        
    # write
    num_write = workload.num_queries - num_read - num_scan
    t = random.choices(db, k=num_write)
    for i in t:
        queries.append(f"UPDATE {i}")
        
        
    random.shuffle(queries)
    with open(output_file, 'a') as f:
        for query in queries:
            f.write(query + '\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    parser.add_argument('--prepare', type=str, required=True)
    parser.add_argument('--output', type=str, required=True)
    args = parser.parse_args()
    
    CONFIG_FILE = args.config
    DB_FILE = args.prepare
    OUTPUT_FILE = args.output
    

    workloads = get_config(CONFIG_FILE)
    db = get_db(DB_FILE)
    # if OUTPUT_FILE exists, clear it
    with open(OUTPUT_FILE, 'w') as f:
        pass
    for workload in workloads:
        generate_workload(workload, db, OUTPUT_FILE)