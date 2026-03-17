import boto3
import json

REGION    = "us-east-1"
MEMORY_ID = ""
ACTOR_ID  = ""

agentcore_rt = boto3.client("bedrock-agentcore", region_name=REGION)

print("\n=== REFLECTIONS ===")
result = agentcore_rt.retrieve_memory_records(
    memoryId=MEMORY_ID,
    namespace=f"/reflections/{ACTOR_ID}",   # ← igual ao save
    searchCriteria={
        "searchQuery": "viagem planejamento recomendação",
        "topK": 5
    }
)

print(f"Total: {len(result.get('memoryRecords', []))}")
for record in result.get("memoryRecordSummaries", []):
    print(f"\nid        : {record.get('memoryRecordId')}")
    print(f"namespace : {record.get('namespace')}")
    print(f"content   : {record.get('content', {}).get('text', '')}")