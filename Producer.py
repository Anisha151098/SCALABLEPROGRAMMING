import boto3
import time
 
s3 = boto3.client("s3")
kinesis = boto3.client("kinesis", region_name="eu-west-1")  # Use your region
 
bucket = "awscalable"
key = "Scalable_data.txt"
stream_name = "scalable"
 
# Read from S3 line by line (streaming body for big file)
response = s3.get_object(Bucket=bucket, Key=key)
body = response['Body']
 
print("📤 Starting streaming to Kinesis...")
 
for byte_line in body.iter_lines():
    line = byte_line.decode("utf-8").strip()
    if line:
        kinesis.put_record(StreamName=stream_name, Data=line, PartitionKey="p1")
        time.sleep(0.01)  # simulate streaming rate
print("✅ Finished streaming.")
