import boto3
import json

REGION = "us-east-1"

MEMORY_NAME = "memory_self_managed_batch10"
DESCRIPTION = "Self managed memory with batch 10 events"

S3_BUCKET = "selfmanagement-messages"
SNS_TOPIC_ARN = ""
MEMORY_ROLE = ""

control = boto3.client(
    "bedrock-agentcore-control",
    region_name=REGION
)

response = control.create_memory(
    name=MEMORY_NAME,
    description=DESCRIPTION,
    eventExpiryDuration=90,
    memoryExecutionRoleArn=MEMORY_ROLE,
    memoryStrategies=[
        {
            "customMemoryStrategy": {
                "name": "self_managed_batch10",
                "description": "Self managed pipeline batch 10 events",
                "configuration": {
                    "selfManagedConfiguration": {
                        "historicalContextWindowSize": 10,
                        "invocationConfiguration": {
                            "topicArn": SNS_TOPIC_ARN,
                            "payloadDeliveryBucketName": S3_BUCKET
                        },
                        "triggerConditions": [
                            {"messageBasedTrigger": {"messageCount": 10}},
                            {"tokenBasedTrigger": {"tokenCount": 1000}},
                            {"timeBasedTrigger": {"idleSessionTimeout": 10}}
                        ]
                    }
                }
            }
        }
    ]
)

print(json.dumps(response, indent=2, default=str))