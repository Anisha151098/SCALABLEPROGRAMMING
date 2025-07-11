import multiprocessing
import time
import re
import os  # Added missing import
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import matplotlib.pyplot as plt
 
# Configuration
INPUT_FILE = 'scalable_data.txt'
OUTPUT_FILE = 'results.txt'
NUM_IO_WORKERS = 4  # For task-parallel operations (ingest/clean/store)
NUM_CPU_WORKERS = multiprocessing.cpu_count()  # For data-parallel MapReduce
 
class HybridProcessor:
    def __init__(self):
        self.performance_data = {
            'sequential': {'time': 0},
            'hybrid': {'time': 0}
        }
 
    # -------- TASK 1: Ingest Data (Task Parallel) --------
    def ingest_data(self):
        """Parallel file reading using threads"""
        with ThreadPoolExecutor(max_workers=NUM_IO_WORKERS) as executor:
            chunks = self._split_file(INPUT_FILE, NUM_IO_WORKERS)
            results = list(executor.map(self._read_chunk, chunks))
        return [line for chunk in results for line in chunk]
 
    def _split_file(self, file_path, num_chunks):
        """Determine byte ranges for parallel reading"""
        file_size = os.path.getsize(file_path)
        chunk_size = file_size // num_chunks
        return [(file_path, i * chunk_size, (i + 1) * chunk_size) 
                for i in range(num_chunks)]
 
    def _read_chunk(self, chunk_info):
        """Read specific file chunk"""
        file_path, start, end = chunk_info
        with open(file_path, 'r', encoding='utf-8') as f:
            f.seek(start)
            return f.read(end - start).splitlines()
 
    # -------- TASK 2: Clean Data (Task Parallel) --------
    def clean_data(self, lines):
        """Parallel text cleaning"""
        with ThreadPoolExecutor(max_workers=NUM_IO_WORKERS) as executor:
            cleaned = list(executor.map(self._clean_line, lines))
        return cleaned
 
    def _clean_line(self, line):
        return re.sub(r'[^a-zA-Z0-9\s]', '', line).lower()
 
    # -------- TASK 3: MapReduce (Data Parallel) --------
    def mapreduce(self, lines):
        """Hybrid parallel MapReduce"""
        chunk_size = len(lines) // NUM_CPU_WORKERS
        chunks = [lines[i:i + chunk_size] 
                 for i in range(0, len(lines), chunk_size)]
 
        with ProcessPoolExecutor(max_workers=NUM_CPU_WORKERS) as executor:
            counters = list(executor.map(self._map_chunk, chunks))
 
        return self._reduce_counters(counters)
 
    def _map_chunk(self, chunk):
        return Counter(word for line in chunk for word in line.split())
 
    def _reduce_counters(self, counters):
        total = Counter()
        for counter in counters:
            total.update(counter)
        return total
 
    # -------- TASK 4: Store Results (Task Parallel) --------
    def store_results(self, result):
        """Parallel result writing"""
        top_words = result.most_common(50)
        with ThreadPoolExecutor(max_workers=NUM_IO_WORKERS) as executor:
            executor.submit(self._write_results, top_words)
 
    def _write_results(self, results):
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            for word, count in results:
                f.write(f"{word}: {count}\n")
 
    # -------- Benchmarking --------
    def benchmark(self):
        """Run and compare both approaches"""
        print("=== Sequential Processing ===")
        seq_time = self._run_pipeline(parallel=False)
        self.performance_data['sequential']['time'] = seq_time
 
        print("\n=== Hybrid Parallel Processing ===")
        par_time = self._run_pipeline(parallel=True)
        self.performance_data['hybrid']['time'] = par_time
 
        self._plot_results()
        print(f"\nSpeedup: {seq_time/par_time:.2f}x")
 
    def _run_pipeline(self, parallel):
        start = time.time()
 
        # 1. Ingest
        data = self.ingest_data()
        print(f"[{'Parallel' if parallel else 'Sequential'}] Ingested {len(data)} lines")
 
        # 2. Clean
        cleaned = self.clean_data(data)
        print(f"[{'Parallel' if parallel else 'Sequential'}] Cleaned {len(cleaned)} lines")
 
        # 3. Process
        if parallel:
            results = self.mapreduce(cleaned)
        else:
            results = self._map_chunk(cleaned)  # Sequential processing
        print(f"[{'Parallel' if parallel else 'Sequential'}] Found {len(results)} unique words")
 
        # 4. Store
        self.store_results(results)
        print(f"[{'Parallel' if parallel else 'Sequential'}] Results stored in {OUTPUT_FILE}")
 
        return time.time() - start
 
    def _plot_results(self):
        """Visualize performance comparison"""
        labels = ['Sequential', 'Hybrid Parallel']
        times = [self.performance_data['sequential']['time'],
                self.performance_data['hybrid']['time']]
 
        plt.figure(figsize=(10, 6))
        bars = plt.bar(labels, times, color=['red', 'green'])
        plt.title('Hybrid Parallelism Performance')
        plt.ylabel('Time (seconds)')
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}s', ha='center', va='bottom')
        plt.savefig('performance_comparison.png')
        print("Performance graph saved to performance_comparison.png")
 
if __name__ == "__main__":
    # Verify input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found!")
        print("Please create the file or update INPUT_FILE path.")
    else:
        processor = HybridProcessor()
        processor.benchmark()
