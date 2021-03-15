import boto3, csv

s3 = boto3.resource('s3',
    aws_access_key_id= 'AKIASMM626M3BNWHP65B',
    aws_secret_access_key= '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'
)
try:
    s3.create_bucket(Bucket='my-first-bucket-stoyer', CreateBucketConfiguration={
    'LocationConstraint':'us-west-2'})

except:
    print('Bucket may already exist')

try:
    s3.Object('my-first-bucket-stoyer', 'Boto3_Screenshot.png').put(
        Body =  open('Boto3_Screenshot.png', 'rb'))
except:
    print('Object may already exist')
bucket = s3.Bucket('my-first-bucket-stoyer')
bucket.Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb', 
    region_name = 'us-west-2', 
    aws_access_key_id= 'AKIASMM626M3BNWHP65B',
    aws_secret_access_key= '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'
)
try:
    table = dyndb.create_table(
        TableName='DataTable',
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
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print (item)
        if (item[4] == '"\turl"' or  item[4] == ''):
            continue
        item[4] = item[4][1:len(item[4])-1].strip() 
        body = open(item[4], 'rb')
        s3.Object('my-first-bucket-stoyer', item[4]).put(Body=body )
        md = s3.Object('my-first-bucket-stoyer', item[4]).Acl().put(ACL='public-read')

        url = "https://s3-us-west-2.amazonaws.com/my-first-bucket-stoyer/"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
        'description' : item[3], 'date' : item[2], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print ("item may already be there or another failure")


response = table.get_item(
    Key = {  
        'PartitionKey':'experiment2',
        'RowKey': '4'
    }
)

item = response['Item']
print(item)
