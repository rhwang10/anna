import json
import boto3

from boto3.dynamodb.conditions import Key

class MessageEvents:

    def __init__(self):
        dynamo = boto3.resource('dynamodb')
        self.table = dynamo.Table("elsa-message-events")

    def get(self, author_id, word):
        response = self.table.query(
            KeyConditionExpression=Key('authorId').eq(author_id) & Key('word').eq(word)
        )

        # Returns the first match, which should be the only match
        # since authorId + word should be a unique composite key
        return response["Items"][0] if response["Items"] else None

    def put(self, item):
        response = self.table.put_item(
            Item=item
        )
        return response

    def update(self, item, latest_timestamp):
        response = self.table.update_item(
            Key={
                "authorId": item["authorId"],
                "word": item["word"]
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

message_events = MessageEvents()

def lambda_handler(event, context):

    message_body = json.loads(event["Records"][0]["body"])

    words = message_body["message"].split(" ")
    author_id = message_body["authorId"]
    timestamp = message_body["timestamp"]
    message_id = message_body["messageId"]

    # Update count for each word
    for word in words:

        r = message_events.get(author_id, word)

        # Update if record exists
        if r:
            message_events.update(r, timestamp)
            print(f"Record for authorId {author_id} and word {word} updated successfully")

        # Otherwise, insert
        else:
            item = {
                "authorId": author_id,
                "word": word,
                "timestamp": timestamp,
                "message_id": message_id,
                "count": 1
            }

            message_events.put(item)
            print(f"Record for authorId {author_id} and word {word} created successfully")

    return {
        'statusCode': 200
    }
