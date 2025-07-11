import time
from multiprocessing import Pool, cpu_count
import os
import matplotlib.pyplot as plt
import boto3
from io import BytesIO
from collections import Counter
import re
import numpy as np
 
# Configuration
INPUT_FILE = 'your_1gb_text_file.txt'  # Replace with your file path
S3_BUCKET = 'your-hashtag-metrics-bucket'
AWS_ACCESS_KEY = 'YOUR_ACCESS_KEY'
AWS_SECRET_KEY = 'YOUR_SECRET_KEY'
CHUNK_SIZE = 1024 * 1024  # 1MB chunks
TOP_HASHTAGS = 20  # Number of top hashtags to visualize
 
# Regular expression to find hashtags
HASHTAG_PATTERN = re.compile(r'#\w+')
 
def read_in_chunks(file_path, chunk_size=CHUNK_SIZE):
    """Generator to read large file in chunks with progress tracking"""
    file_size = os.path.getsize(file_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield data, file_size
 
def extract_hashtags(text):
    """Extract hashtags from text with counts"""
    return Counter(HASHTAG_PATTERN.findall(text.lower())  # Case insensitive
 
def sequential_hashtag_analysis(file_path):
    """Sequential hashtag analysis with metrics"""
    hashtag_counts = Counter()
    start_time = time.time()
    for chunk, file_size in read_in_chunks(file_path):
        chunk_counts = extract_hashtags(chunk)
        hashtag_counts.update(chunk_counts)
    processing_time = time.time() - start_time
    throughput = file_size / (processing_time * 1024 * 1024)  # MB/s
    latency = processing_time * 1000  # ms
    return hashtag_counts, processing_time, throughput, latency
 
def map_function(args):
    """Map function for parallel processing"""
    chunk, _ = args
    return extract_hashtags(chunk)
 
def reduce_function(counters):
    """Reduce function to combine counters"""
    final_counter = Counter()
    for counter in counters:
        final_counter.update(counter)
    return final_counter
 
def parallel_hashtag_analysis(file_path, num_processes=None):
    """Parallel hashtag analysis with metrics"""
    if num_processes is None:
        num_processes = cpu_count()
    start_time = time.time()
    file_size = os.path.getsize(file_path)
    with Pool(num_processes) as pool:
        mapped_counts = pool.map(map_function, read_in_chunks(file_path))
        final_counts = reduce_function(mapped_counts)
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
 
def save_hashtag_graph(hashtag_counts, s3_key, top_n=TOP_HASHTAGS):
    """Save top hashtags visualization to S3"""
    top_hashtags = hashtag_counts.most_common(top_n)
    hashtags, counts = zip(*top_hashtags)
    plt.figure(figsize=(12, 8))
    bars = plt.barh(hashtags, counts, color='purple')
    plt.xlabel('Count')
    plt.title(f'Top {top_n} Trending Hashtags')
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
 
def save_hashtag_table(hashtag_counts, s3_key, top_n=TOP_HASHTAGS):
    """Save hashtag frequency table as CSV to S3"""
    top_hashtags = hashtag_counts.most_common(top_n)
    csv_content = "Hashtag,Count\n" + "\n".join([f"{tag},{count}" for tag, count in top_hashtags])
    buffer = BytesIO(csv_content.encode('utf-8'))
    s3 = boto3.client('s3',
                     aws_access_key_id=AWS_ACCESS_KEY,
                     aws_secret_access_key=AWS_SECRET_KEY)
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
    return f's3://{S3_BUCKET}/{s3_key}'
 
if __name__ == '__main__':
    print(f"Processing file: {INPUT_FILE}")
    print(f"File size: {os.path.getsize(INPUT_FILE)/(1024*1024):.2f} MB")
    print(f"Available CPU cores: {cpu_count()}")
    # Sequential processing
    print("\nRunning sequential hashtag analysis...")
    seq_counts, seq_time, seq_throughput, seq_latency = sequential_hashtag_analysis(INPUT_FILE)
    print(f"Sequential completed in {seq_time:.2f} seconds")
    print(f"Throughput: {seq_throughput:.2f} MB/s")
    print(f"Latency: {seq_latency:.2f} ms")
    print(f"Total unique hashtags found: {len(seq_counts)}")
    # Parallel processing
    print("\nRunning parallel hashtag analysis...")
    par_counts, par_time, par_throughput, par_latency = parallel_hashtag_analysis(INPUT_FILE)
    print(f"Parallel completed in {par_time:.2f} seconds")
    print(f"Throughput: {par_throughput:.2f} MB/s")
    print(f"Latency: {par_latency:.2f} ms")
    print(f"Total unique hashtags found: {len(par_counts)}")
    # Verify results match exactly
    assert seq_counts == par_counts, "Results don't match between implementations"
    # Prepare metrics
    metrics = {
        'seq_time': seq_time,
        'par_time': par_time,
        'seq_throughput': seq_throughput,
        'par_throughput': par_throughput,
        'seq_latency': seq_latency,
        'par_latency': par_latency,
        'speedup': seq_time / par_time,
        'total_hashtags': len(par_counts),
        'top_hashtags': par_counts.most_common(TOP_HASHTAGS)
    }
    # Save graphs to S3
    perf_graph = save_performance_graph(metrics, 'hashtag_performance.png')
    hashtag_graph = save_hashtag_graph(par_counts, 'top_hashtags.png')
    hashtag_table = save_hashtag_table(par_counts, 'hashtag_frequencies.csv')
    # Print summary
    print("\n=== Results Summary ===")
    print(f"Speedup factor: {metrics['speedup']:.2f}x")
    print(f"Total unique hashtags found: {metrics['total_hashtags']}")
    print("\nTop 5 Hashtags:")
    for i, (tag, count) in enumerate(metrics['top_hashtags'][:5], 1):
        print(f"{i}. {tag}: {count} occurrences")
    print(f"\nPerformance metrics graph: {perf_graph}")
    print(f"Hashtag distribution graph: {hashtag_graph}")
    print(f"Hashtag frequency table: {hashtag_table}")
