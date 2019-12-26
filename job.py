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

campaigns = []
for item in response['Items']:
    campaigns.append("'" + item['campaign_id'] + "'")
campaigns = ','.join(campaigns)
#print(campaigns)

spark_sql = 'select client.client_id as client_id, count(*) \
    as cnt from "pinpoint_campaign"."pinpoint_1127" where attributes.campaign_id in (%s) \
    and event_type = \'_campaign.opened_notification\' group by client.client_id'%campaigns
print(spark_sql)