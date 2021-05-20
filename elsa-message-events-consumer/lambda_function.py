import json
import boto3

from boto3.dynamodb.conditions import Key

class DynamoClient:

    def __init__(self):
        dynamo = boto3.resource('dynamodb')
        self.table = dynamo.Table("elsa-emoji-events")

    def check_metadata(self, pk, sk):
        response = self.table.query(
            KeyConditionExpression=Key('emoji_id|emoji_name').eq(pk) \
            & Key('author_id|timestamp').eq(sk)
        )

        return response["Items"][0] if response["Items"] else None


    def put(self, item):
        return self.table.put_item(Item=item)

    def update(self, item, latest_timestamp):
        return self.table.update_item(
            Key={
                "emoji_id|emoji_name": item["emoji_id|emoji_name"],
                "author_id|timestamp": item["author_id|timestamp"]
            },
            UpdateExpression='SET #ts = :ts, #c = #c + :ct',
            ExpressionAttributeValues={
                ':ts': latest_timestamp,
                ':ct': 1
            },
            ExpressionAttributeNames={
                '#ts': 'timestamp',
                '#c': 'count'
            }
        )

dynamo_client = DynamoClient()

def lambda_handler(event, context):

    message_body = json.loads(event["Records"][0]["body"])

    author_id = message_body["authorId"]
    author_name = message_body["authorName"]
    emoji_id = message_body["emojiId"]
    emoji_name = message_body["emojiName"]
    timestamp = message_body["timestamp"]
    channel = message_body["channel"]
    voice_channel = message_body["voiceChannel"]

    # Emoji partition key emoji_id|emoji_name

    pk, sk = key(emoji_id, emoji_name), key(author_id, timestamp)

    metadata_sk = key("METADATA", "")

    record = {
        "emoji_id|emoji_name": pk,
        "author_id|timestamp": sk,
        "author_name": author_name,
        "channel": channel,
        "voice_channel": voice_channel,
        "timestamp": timestamp
    }

    # put the record
    dynamo_client.put(record)

    r = dynamo_client.check_metadata(pk, metadata_sk)

    if r:

        dynamo_client.update(r, timestamp)
        print(f"Metadata record for emoji ID {emoji_id} | emoji name {emoji_name} updated successfully")

    else:

        metadata_record = {
            "emoji_id|emoji_name": pk,
            "author_id|timestamp": metadata_sk,
            "timestamp": timestamp,
            "count": 1
        }

        dynamo_client.put(metadata_record)
        print(f"Metadata record created for emoji ID {emoji_id} | emoji name {emoji_name}")


    return {
        'statusCode': 200
    }

def key(id, name):
    return id + "|" + name if name else id
