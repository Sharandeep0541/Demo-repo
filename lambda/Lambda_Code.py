import json
import os
import boto3
from urllib.parse import unquote_plus

sqs = boto3.client("sqs")
sns = boto3.client("sns")
sf = boto3.client("stepfunctions")

SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

def notify_failure(reason, details):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="ETL Preprocessing Failed",
        Message=json.dumps({
            "reason": reason,
            "details": details
        })
    )

def lambda_handler(event, context):
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    size = record["s3"]["object"]["size"]
    filename = key.split("/")[-1]

    if not filename.startswith("annual"):
        print(f"FAILED: Filename check -> {filename}")
        notify_failure("invalid_filename", {"bucket": bucket, "key": key})
        return {"status": "failed", "reason": "invalid_filename"}

    if size == 0:
        print(f"FAILED: Empty file -> {filename}")
        notify_failure("empty_file", {"bucket": bucket, "key": key})
        return {"status": "failed", "reason": "empty_file"}

    message = {
        "bucket": bucket,
        "key": key,
        "filename": filename,
    }
    sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(message)
    )

    print("SUCCESS: Message sent to SQS")
    print(message)
    return {"status": "success"}
