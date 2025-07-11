import boto3
import time
from collections import deque, Counter
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
from io import BytesIO
import json
import os
 
# Configuration - MAKE SURE TO SET THESE CORRECTLY
STREAM_NAME = "scalable"
REGION = "eu-west-1"
S3_BUCKET = "your-metrics-bucket"  # Double-check this matches your exact bucket name
WINDOW_SIZE = 300  # 5 minutes in seconds
MAX_WORKERS = 4
 
class StreamAnalyzer:
    def __init__(self):
        self.kinesis = boto3.client("kinesis", region_name=REGION)
        self.s3 = boto3.client("s3")
        self.word_counts = Counter()
        self.window = deque()
        self.metrics = {
            'parallel': {'latency': [], 'throughput': [], 'timestamps': []},
            'sequential': {'latency': [], 'throughput': [], 'timestamps': []}
        }
        self.current_mode = 'parallel'
        self.last_switch = time.time()
        self.processed_records = 0
        self.verify_s3_access()
 
    def verify_s3_access(self):
        """Verify we can access the S3 bucket before starting"""
        try:
            self.s3.head_bucket(Bucket=S3_BUCKET)
            print(f"✅ Successfully accessed S3 bucket: {S3_BUCKET}")
        except Exception as e:
            print(f"❌ Failed to access S3 bucket: {str(e)}")
            print("Please verify:")
            print(f"1. Bucket name '{S3_BUCKET}' exists in region '{REGION}'")
            print("2. Your AWS credentials have proper permissions")
            print("3. The bucket isn't blocked by S3 policies")
            raise
 
    def process_record(self, record, mode):
        start_time = time.time()
        data = record['Data'].decode('utf-8')
        if mode == 'parallel':
            words = self._parallel_process(data)
        else:
            words = self._sequential_process(data)
        # Update metrics
        processing_time = time.time() - start_time
        self.metrics[mode]['latency'].append(processing_time * 1000)  # ms
        self.metrics[mode]['throughput'].append(len(words) / processing_time)  # words/sec
        self.metrics[mode]['timestamps'].append(datetime.utcnow().isoformat())
        # Update sliding window
        self._update_window(words)
        self.processed_records += 1
        return words
 
    def _parallel_process(self, text):
        words = text.split()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = list(executor.map(
                lambda w: w.lower() if w.isalpha() else None,
                words
            ))
        return [w for w in results if w]
 
    def _sequential_process(self, text):
        return [word.lower() for word in text.split() if word.isalpha()]
 
    def _update_window(self, words):
        now = datetime.utcnow()
        self.window.append((now, words))
        self.word_counts.update(words)
        # Remove entries older than 5 minutes
        cutoff = now - timedelta(seconds=WINDOW_SIZE)
        while self.window and self.window[0][0] < cutoff:
            old_time, old_words = self.window.popleft()
            self.word_counts.subtract(old_words)
        # Clean zero counts
        self.word_counts += Counter()
 
    def get_top_words(self, n=5):
        return self.word_counts.most_common(n)
 
    def switch_mode(self):
        if time.time() - self.last_switch >= WINDOW_SIZE:
            # Save metrics before switching
            self._save_performance_data()
            # Switch mode
            self.current_mode = 'sequential' if self.current_mode == 'parallel' else 'parallel'
            self.last_switch = time.time()
            print(f"\n🔄 Switching to {self.current_mode.upper()} mode")
            return True
        return False
 
    def _save_performance_data(self):
        """Save both graph and metrics data to S3"""
        if not self.metrics[self.current_mode]['timestamps']:
            print("⚠️ No metrics to save")
            return
 
        timestamp = int(time.time())
        mode = self.current_mode
        try:
            # 1. Generate and save the graph
            fig = self._generate_performance_graph()
            graph_key = f"performance_graphs/{mode}_{timestamp}.png"
            buffer = BytesIO()
            fig.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
            buffer.seek(0)
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=graph_key,
                Body=buffer,
                ContentType='image/png'
            )
            plt.close(fig)
            # 2. Save metrics JSON
            metrics_data = {
                "mode": mode,
                "timestamp": datetime.utcnow().isoformat(),
                "processed_records": self.processed_records,
                "top_words": dict(self.get_top_words()),
                "avg_latency": sum(self.metrics[mode]['latency'])/len(self.metrics[mode]['latency']),
                "avg_throughput": sum(self.metrics[mode]['throughput'])/len(self.metrics[mode]['throughput']),
                "graph_location": f"s3://{S3_BUCKET}/{graph_key}"
            }
            self.s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"performance_metrics/{mode}_{timestamp}.json",
                Body=json.dumps(metrics_data, indent=2)
            )
            print(f"✅ Saved {mode} performance data to S3")
            print(f"  - Graph: s3://{S3_BUCKET}/{graph_key}")
            print(f"  - Metrics: s3://{S3_BUCKET}/performance_metrics/{mode}_{timestamp}.json")
            # Reset metrics for the new period
            self.metrics[mode] = {'latency': [], 'throughput': [], 'timestamps': []}
        except Exception as e:
            print(f"❌ Failed to save to S3: {str(e)}")
            # Try creating the bucket if it doesn't exist
            try:
                self.s3.create_bucket(
                    Bucket=S3_BUCKET,
                    CreateBucketConfiguration={'LocationConstraint': REGION}
                )
                print(f"Created bucket {S3_BUCKET}, retrying...")
                self._save_performance_data()  # Retry
            except Exception as create_error:
                print(f"Could not create bucket: {str(create_error)}")
 
    def _generate_performance_graph(self):
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        mode = self.current_mode
        timestamps = [datetime.fromisoformat(ts) for ts in self.metrics[mode]['timestamps']]
        # Latency plot
        ax1.plot(timestamps, self.metrics[mode]['latency'], 'r-')
        ax1.set_ylabel('Latency (ms)')
        ax1.set_title(f'{mode.capitalize()} Processing Latency')
        ax1.grid(True)
        # Throughput plot
        ax2.plot(timestamps, self.metrics[mode]['throughput'], 'b-')
        ax2.set_ylabel('Throughput (words/sec)')
        ax2.set_title(f'{mode.capitalize()} Processing Throughput')
        ax2.grid(True)
        plt.tight_layout()
        return fig
 
def main():
    analyzer = StreamAnalyzer()
    # Initialize Kinesis consumer
    shard_id = analyzer.kinesis.describe_stream(
        StreamName=STREAM_NAME
    )['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = analyzer.kinesis.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="LATEST"
    )['ShardIterator']
 
    print("\nStarting stream analysis...")
    print(f"Initial mode: {analyzer.current_mode.upper()}")
    print(f"Will alternate every {WINDOW_SIZE} seconds")
    print(f"S3 Bucket: {S3_BUCKET}\n")
    last_update = time.time()
    while True:
        try:
            # Check if we need to switch modes
            analyzer.switch_mode()
            # Get records from Kinesis
            response = analyzer.kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=100
            )
            shard_iterator = response['NextShardIterator']
 
            # Process records
            if response['Records']:
                for record in response['Records']:
                    analyzer.process_record(record, analyzer.current_mode)
 
            # Print updates every 30 seconds
            if time.time() - last_update > 30:
                top_words = analyzer.get_top_words()
                print(f"\nCurrent mode: {analyzer.current_mode.upper()}")
                print(f"Records processed: {analyzer.processed_records}")
                print("Top 5 trending words:")
                for word, count in top_words:
                    print(f"  {word}: {count}")
                last_update = time.time()
 
            time.sleep(1)
 
        except Exception as e:
            print(f"\n⚠️ Error: {str(e)}")
            time.sleep(5)
 
if __name__ == '__main__':
    main()
