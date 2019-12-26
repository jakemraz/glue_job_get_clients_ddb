import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import json
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job # Glue Job 인 만큼 가장 중요하다
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import regexp_extract, col

class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, decimal.Decimal):
                if o % 1 > 0:
                    return float(o)
                else:
                    return int(o)
            return super(DecimalEncoder, self).default(o)

def get_campaigns():
    ddb_table = "cdk-pinpoint-ddb-pinpointcategoryDA990D86-ZL8XENRX3IRY"

    dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
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
    return campaigns


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# SparkContext 생성
sc = SparkContext()
# GlueContext 생성
glueContext = GlueContext(sc)
# SparkSession 생성
spark = glueContext.spark_session
# Job 생성
job = Job(glueContext)
# Job 초기화
job.init(args['JOB_NAME'], args)

print('jhbaek')

campaigns = get_campaigns()
print(campaigns)

spark_sql = 'select client.client_id as client_id, count(*) \
    as cnt from global_temp.pinpoint where attributes.campaign_id in (%s) \
    and event_type = \'_campaign.opened_notification\' group by client.client_id'%campaigns
print(spark_sql)

pinpoint_df = glueContext.create_dynamic_frame.from_catalog(database='pinpoint_campaign', table_name='pinpoint_partitionpinpoint_1127', transformation_ctx='pinpoint_df').toDF()
pinpoint_df.createGlobalTempView("pinpoint")

#p_df = spark.sql("select client.client_id as client_id from pinpoint where attributes.campaign_id = '06bedf455dfe42de99fa8e1df3a0f5fa'")
#p_df = spark.sql("select client.client_id as client_id, count(*) as cnt from global_temp.pinpoint where attributes.campaign_id in ('19fa2cd80e0c4588b6b9f281820f794f', '80610fe9fcbb4db4a45b377da2651416', 'd7330d0c1a7a4153bf9e3a6ded801ed5') and event_type = '_campaign.opened_notification' group by client.client_id having cnt > 1")
p_df = spark.sql(spark_sql)

p_df.write.format('csv').mode('overwrite').save('s3://aws-glue-temporary-332346530942-us-west-2/output')

# Job commit
job.commit()
