# Required libraries
import json
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
from pandas.io.json import json_normalize
from io import StringIO
from decimal import *

# Function to upload data to DynamoDB
def put_data(Cnt, DeviceID, inference, prob):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('ai_result')
    #print (inference)
    response = table.put_item(
       Item={
            'Count': Cnt,
            'DeviceID': DeviceID,
            'Inference': inference,
            'Prob': Decimal(str(prob))
        }
    )
    return response
    
# Function to get the data
def query_data(Cnt, DeviceID, window_size):
    
    # Required instances
    dynamodb_client = boto3.client('dynamodb')
    
    # Getting all the elements else last window size elements
    Cnt = Cnt-window_size
    if Cnt <= 0 :
        Cnt = 1
    #print (Cnt)
    # Fetching data
    response = dynamodb_client.query(
        TableName="AthleTechData",
        KeyConditionExpression='DeviceID= :DeviceID AND #Count >= :Count',
        ExpressionAttributeValues={
            ":DeviceID": {'S': DeviceID},
            ":Count": {'N': str(Cnt)}
        },
        ExpressionAttributeNames={
            "#Count": "Count"
        }
    )
    
    # Converting to DataFrame
    columns = list(response['Items'][0].keys())
    response = response['Items']
    df = pd.DataFrame(response)
    df.columns = columns
    df = df[['g_y']]
    
    # Parsing the JSON columns
    #df['DeviceID'] = json_normalize(df['DeviceID']).astype(int)
    #df['Count'] = json_normalize(df['Count']).astype(int)
    df['g_y'] = json_normalize(df['g_y']).astype(float)
    
    # Normalising
    #df['g_y'] = (df['g_y']-df['g_y'].min())/(df['g_y'].max()-df['g_y'].min())
    #df['a_x'] = (df['a_x']-df['a_x'].min())/(df['a_x'].max()-df['a_x'].min())
    
    # Returing the DataFrame
    return df


def lambda_handler(event, context):
    # TODO implement
    
    # Reading the count and device_id from input
    #print(event)
    Cnt = int(event['Records'][0]['dynamodb']['Keys']['Count']['N'])
    DeviceID = event['Records'][0]['dynamodb']['Keys']['DeviceID']['S']
    """For Testing"""
    #Cnt = int(event['Count'])
    #DeviceID = event['DeviceID']
    print("Data from " + str(Cnt))
    
    # Running it got every 50 points
    if Cnt % 50 != 0 or Cnt % 100 == 0:
    
        # Returning success    
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
        
    # Window size
    window_size = 100
    
    # Getting data
    data_df = query_data(Cnt, DeviceID, window_size)
    #print (data_df.head())
    
    # Getting teh templates from S3
    client = boto3.client('s3')
    bucket_name = 'jogwalktemplate'
    object_keys = ['template_fast.csv', 'template_jog.csv', 'template_slow_walk.csv']
    template_list = []
    for obj in object_keys:
        
        csv_obj = client.get_object(Bucket=bucket_name, Key=obj)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
    
        template_list.append(pd.read_csv(StringIO(csv_string)))
    
    # Calculating the correlation    
    corr_list = []
    for i in range(0, len(template_list)):
        result = template_list[i].rolling(window_size, min_periods = 1).corr(data_df['g_y']).mean().mean()
        if result > 1:
            result = 1
        elif result < 0:
            result = 0
        corr_list.append(result)
    print (corr_list)
    
    # Calculating inference and prob
    prob = max(corr_list)
    inference = object_keys[corr_list.index(prob)].split('.')[0]

    # Uploading data
    if prob < 0.5:
        inference = 'No Recognized Activity'
    put_data(Cnt, DeviceID, inference, prob)
    #print ("Result added to the table")
    
    # Returning success    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }