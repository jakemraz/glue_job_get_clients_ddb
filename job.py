import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import json

ddb_table = "cdk-pinpoint-ddb-pinpointcategoryDA990D86-ZL8XENRX3IRY"

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(ddb_table)


category = 'IT'
beginTime = 1576565000 # Tue, Dec 17, 2019 4:16:40 PM GMT+09:00
endTime = 1576569000 # Tue, Dec 17, 2019 4:50:00PM GTM+09:00

response = table.query(
        KeyConditionExpression=Key('category').eq(category) & Key('event_time').between(beginTime, endTime)
    )

print(response)



