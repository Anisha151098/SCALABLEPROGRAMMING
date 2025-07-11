import time
from multiprocessing import Pool, cpu_count
import os
import matplotlib.pyplot as plt
import boto3
from io import BytesIO
from collections import Counter
import numpy as np
 
# Configuration
INPUT_FILE = 'your_1gb_text_file.txt'  # Replace with your file path
S3_BUCKET = 'your-performance-metrics-bucket'
AWS_ACCESS_KEY = 'YOUR_ACCESS_KEY'
AWS_SECRET_KEY = 'YOUR_SECRET_KEY'
TOP_WORDS_COUNT = 20  # Number of top words to visualize
 
def read_in_chunks(file_path, chunk_size=1024*1024):  # 1MB chunks
    """Generator to read large file in chunks"""
    file_size = os.path.getsize(file_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield data, file_size
 
def sequential_word_count(file_path):
    """Optimized sequential word count with metrics"""
    word_counts = {}
    start_time = time.time()
    total_bytes = 0
    for chunk, file_size in read_in_chunks(file_path):
        total_bytes += len(chunk.encode('utf-8'))
        for word in chunk.split():
            word = word.lower().strip('.,!?;:"()[]{}')
            if word:
                word_counts[word] = word_counts.get(word, 0) + 1
    processing_time = time.time() - start_time
    throughput = file_size / (processing_time * 1024 * 1024)  # MB/s
    latency = processing_time * 1000  # ms
    return word_counts, processing_time, throughput, latency
 
def map_function(args):
    """Map function with progress tracking"""
    chunk, _ = args
    word_counts = {}
    for word in chunk.split():
        word = word.lower().strip('.,!?;:"()[]{}')
        if word:
            word_counts[word] = word_counts.get(word, 0) + 1
    return word_counts
 
def parallel_word_count(file_path, num_processes=None):
    """Parallel word count with metrics"""
    if num_processes is None:
        num_processes = cpu_count()
    start_time = time.time()
    file_size = os.path.getsize(file_path)
    with Pool(num_processes) as pool:
        # Map phase
        mapped_counts = pool.map(map_function, read_in_chunks(file_path))
        # Reduce phase
        final_counts = {}
        for partial_counts in mapped_counts:
            for word, count in partial_counts.items():
                final_counts[word] = final_counts.get(word, 0) + count
    processing_time = time.time() - start_time
    throughput = file_size / (processing_time * 1024 * 1024)  # MB/s
    latency = processing_time * 1000  # ms
    return final_counts, processing_time, throughput, latency
 
def save_performance_graph(metrics, s3_key):
    """Save performance comparison graph to S3"""
    labels = ['Sequential', 'Parallel']
    time_values = [metrics['seq_time'], metrics['par_time']]
    throughput_values = [metrics['seq_throughput'], metrics['par_throughput']]
    latency_values = [metrics['seq_latency'], metrics['par_latency']]
 
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))
    # Execution Time
    bars1 = ax1.bar(labels, time_values, color=['blue', 'green'])
    ax1.set_ylabel('Time (seconds)')
    ax1.set_title('Execution Time Comparison')
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}s', ha='center', va='bottom')
    # Throughput
    bars2 = ax2.bar(labels, throughput_values, color=['blue', 'green'])
    ax2.set_ylabel('Throughput (MB/s)')
    ax2.set_title('Throughput Comparison')
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f} MB/s', ha='center', va='bottom')
    # Latency
    bars3 = ax3.bar(labels, latency_values, color=['blue', 'green'])
    ax3.set_ylabel('Latency (ms)')
    ax3.set_title('Latency Comparison')
    for bar in bars3:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f} ms', ha='center', va='bottom')
    plt.tight_layout()
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=300)
    buffer.seek(0)
    s3 = boto3.client('s3',
                     aws_access_key_id=AWS_ACCESS_KEY,
                     aws_secret_access_key=AWS_SECRET_KEY)
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
    plt.close()
    return f's3://{S3_BUCKET}/{s3_key}'
 
def save_top_words_graph(word_counts, s3_key, top_n=TOP_WORDS_COUNT):
    """Save top words visualization to S3"""
    top_words = Counter(word_counts).most_common(top_n)
    words, counts = zip(*top_words)
    plt.figure(figsize=(12, 8))
    bars = plt.barh(words, counts, color='skyblue')
    plt.xlabel('Count')
    plt.title(f'Top {top_n} Most Frequent Words')
    plt.gca().invert_yaxis()  # Highest count at top
    for bar in bars:
        width = bar.get_width()
        plt.text(width, bar.get_y() + bar.get_height()/2.,
                f' {int(width)}', va='center')
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
    buffer.seek(0)
    s3 = boto3.client('s3',
                     aws_access_key_id=AWS_ACCESS_KEY,
                     aws_secret_access_key=AWS_SECRET_KEY)
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
    plt.close()
    return f's3://{S3_BUCKET}/{s3_key}'
 
if __name__ == '__main__':
    print(f"Processing file: {INPUT_FILE}")
    print(f"File size: {os.path.getsize(INPUT_FILE)/(1024*1024):.2f} MB")
    print(f"Available CPU cores: {cpu_count()}")
    # Sequential processing
    print("\nRunning sequential word count...")
    seq_counts, seq_time, seq_throughput, seq_latency = sequential_word_count(INPUT_FILE)
    print(f"Sequential completed in {seq_time:.2f} seconds")
    print(f"Throughput: {seq_throughput:.2f} MB/s")
    print(f"Latency: {seq_latency:.2f} ms")
    # Parallel processing
    print("\nRunning parallel word count...")
    par_counts, par_time, par_throughput, par_latency = parallel_word_count(INPUT_FILE)
    print(f"Parallel completed in {par_time:.2f} seconds")
    print(f"Throughput: {par_throughput:.2f} MB/s")
    print(f"Latency: {par_latency:.2f} ms")
    # Verify results
    assert seq_counts == par_counts, "Results don't match between implementations"
    # Prepare metrics
    metrics = {
        'seq_time': seq_time,
        'par_time': par_time,
        'seq_throughput': seq_throughput,
        'par_throughput': par_throughput,
        'seq_latency': seq_latency,
        'par_latency': par_latency,
        'speedup': seq_time / par_time
    }
    # Save graphs to S3
    perf_graph = save_performance_graph(metrics, 'performance_metrics.png')
    top_words_graph = save_top_words_graph(seq_counts, 'top_words.png')
    # Print summary
    print("\n=== Results Summary ===")
    print(f"Speedup factor: {metrics['speedup']:.2f}x")
    print(f"Performance metrics graph: {perf_graph}")
    print(f"Top words graph: {top_words_graph}")
    print(f"Unique words counted: {len(seq_counts)}")
