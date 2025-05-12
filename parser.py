import numpy as np
from dataclasses import dataclass
import csv
import math

@dataclass
class Result:
    def __init__(self):
        self.data = {
        "DB Size" : {
            "value" : 0,
            "key" : "n_entry",
            "type" : int,
        },
        "Cache Size" : {
            "value" : 0,
            "key" : "cache_size",
            "type" : int,
        },
        "Cache Style" : {
            "value" : "",
            "key" : "cache_style",
            "type" : str,
        },
        "bpk" : {
            "value" : 10,
            "key" : "bpk",
            "type" : int,
        },
        "OP Time" : {
            "value" : 0,
            "key" : "OP time",
            "type" : float,
        },
        "Get Time" : {
            "value" : 0,
            "key" : "get time",
            "type" : float,
        },
        "Scan Time" : {
            "value" : 0,
            "key" : "scan time",
            "type" : float,
        },
        "Put Time" : {
            "value" : 0,
            "key" : "put time",
            "type" : float,
        },
        "SST Read Time" : {
            "value" : 0,
            "key" : "sst read time",
            "type" : float,
        },
        "SST Read Count" : {
            "value" : 0,
            "key" : "sst read count",
            "type" : int,
        },
    }
    
    
    def update(self, line : str):
        line = line.strip().split(":")
        for k, v in self.data.items():
            if v["key"] == line[0]:
                value = line[-1].strip()
                if v["type"] in [float, int] and not v["type"](value) >= 0:
                    pass
                else:
                    value = v["type"](value)
                self.data[k]["value"] = value
                break
            
    def __repr__(self):
        res = ""
        for k, v in self.data.items():
            res += f"{k}: {v['value']} "
        return res
            

def parse_file(file_path : str):
    with open(file_path, "r") as f:
        lines = f.readlines()
    DB_name = lines[0].strip()
    results = []
    for line in lines[1:]:
        if "n_entry" in line:
            results.append(Result())
        if len(results) > 0:
            results[-1].update(line)
    return DB_name, results

def write_csv(file_path : str, results : list):
    with open(file_path, mode='a+') as f:
        writer = csv.writer(f)
        headers = [k for k in results[0].data.keys()]
        # cur pos
        # cur_pos = f.tell()
        # if cur_pos == 0:
        #     writer.writerow(headers)
        writer.writerow(headers)
        for r in results:
            writer.writerow([r.data[k]["value"] for k in headers])
            
def main():
    DB_name, results = parse_file("results")
    for r in results:
        print(r)
    bpk = results[0].data["bpk"]["value"]
    cache_style = results[0].data["Cache Style"]["value"]
    sst_read_time = results[0].data["SST Read Time"]["value"]
    
    if sst_read_time > 0:
        write_csv(f"results_csv/{DB_name}_bpk{bpk}_{cache_style}.csv", results)
    
if __name__ == "__main__":
    main()