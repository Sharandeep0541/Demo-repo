import json
import boto3
import time
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)

sqs = boto3.client("sqs", region_name="us-east-1")

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/539247461779/demoqueue"

all_messages = []

print("Starting to read messages from SQS...")

while True:
    response = sqs.receive_message(
        QueueUrl=QUEUE_URL,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2
    )

    messages = response.get("Messages", [])
    if not messages:
        break

    for msg in messages:
        body = json.loads(msg["Body"])
        receipt_handle = msg["ReceiptHandle"]

        all_messages.append({
            "body": body,
            "receipt_handle": receipt_handle
        })

print(f"Total messages received: {len(all_messages)}")

# ---- Processing step (demo) ----
for item in all_messages:
    print("Processing file:", item["body"]["filename"])

# ---- Delete messages after successful processing ----
for item in all_messages:
    sqs.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=item["receipt_handle"]
    )

print("All messages processed and deleted successfully.")
