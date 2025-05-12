# AdCache: Adaptive Cache Management with Admission Control for LSM-tree Key-Value Stores

This repository contains the source code and experiment scripts for our paper:

**AdCache: Adaptive Cache Management with Admission Control for LSM-tree Key-Value Stores**  
(*Under Submission)

AdCache is a lightweight, reinforcement learning-based cache manager that dynamically partitions memory between block cache and range cache, while applying selective admission for both point lookups and scans in LSM-tree-based key-value stores.

---

## 🧪 Running the Experiments

### Workload Configuration

Workload definitions are located in the `workload/` directory. Each workload file specifies the operation mix (get, short scan, long scan, put).

### Running Experiments

Use the provided `runit.sh` script to configure and run experiments.  
Results are appended to a file named results every 1000 operations.

## Contact
For questions, issues, or reproduction requests, please contact:
jiarui005@e.ntu.edu.sg
