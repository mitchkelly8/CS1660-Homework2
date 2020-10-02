import boto3
import csv

# Create s3 Bucket (Access Keys Removed)
s3 = boto3.resource('s3',aws_access_key_id='', aws_secret_access_key='')

try:
    bucket = s3.create_bucket(
        ACL='public-read-write',
        Bucket='datacont-mitchkelly',
        CreateBucketConfiguration={
            'LocationConstraint': 'us-west-2'
        },
    )
except:
    print("Bucket Already Created")


# Print all s3 buckets
for i in s3.buckets.all():
    print(i.name)


# Upload a file to the s3 bucket
body = open('/Users/MitchKelly/Documents/Business/Mr. Party/Mr Party.png', 'rb')
o = s3.Object('datacont-mitchkelly', 'test').put(Body=body)
s3.Object('datacont-mitchkelly', 'test').Acl().put(ACL='public-read')


# Create DynamoDB Table (Access Keys Removed)
dyndb = boto3.resource(
    'dynamodb',
    region_name='us-west-2',
    aws_access_key_id='',
    aws_secret_access_key=''
)


try:
    table = dyndb.create_table(
        TableName='datatable-mitchkelly',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    # If there is an exception, the ntable may already exist. if so....
    table = dyndb.Table("datatable-mitchkelly")


# Wait for the table to be created
# table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)


# Reading CSV File, uploading the blobs, and creating the table
with open('/Users/MitchKelly/Documents/Pitt/Senior+/Fall/CS 1660/Assignments/Homework 2/data.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        #print(item)
        body = open('/Users/MitchKelly/Documents/Pitt/Senior+/Fall/CS 1660/Assignments/Homework 2/datafiles/' + item[3] + '.csv', 'rb')
        s3.Object('datacont-mitchkelly', item[3]).put(Body=body)
        md = s3.Object('datacont-mitchkelly', item[3]).Acl().put(ACL='public-read')

        url = "https://s3-us-west-2.amazonaws.com/datacont-mitchkelly/"+item[3]
        metadata_item ={'PartitionKey': item[0], 'RowKey': item[1], 'description': item[4], 'date': item[2], 'url':url}

        try:
            table.put_item(Item=metadata_item)
        except:
            print("Item may already be there or another failure")


# Searching for an item
response = table.get_item(
    Key={
        'PartitionKey': 'experiment 3',
        'RowKey': '3'
    }
)
item = response['Item']
print(item)
