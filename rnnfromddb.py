# Required libraries
import json
from keras.models import load_model
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize
from io import StringIO
from decimal import *

# Function to upload data to DynamoDB
def put_data(Cnt, DeviceID, inference, prob):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('ai_result')
    print (inference)
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
        print ("Not enough data")
        return []
        
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
    df = (df - df.min()) / (df.max() - df.min()) 
    
    # Converting to numpy
    loaded_np = df[['g_y']].to_numpy()
    loaded_np = loaded_np / np.linalg.norm(loaded_np)
    
    # Returing the nuumpy
    return loaded_np


def lambda_handler(event, context):
    # TODO implement
    
    # Reading the count and device_id from input
    print(event)
    #Cnt = int(event['Records'][0]['dynamodb']['Keys']['Count']['N'])
    #DeviceID = event['Records'][0]['dynamodb']['Keys']['DeviceID']['S']
    Cnt = int(event['Count'])
    DeviceID = str(event['DeviceID'])
    print("Data from " + str(Cnt))
    
    # Window size
    window_size = 500
    
    # Getting data
    data = query_data(Cnt, DeviceID, window_size)
    
    if len(data) == 0:
        # Returning success    
        return {
            'statusCode': 200,
            'body': json.dumps('Successful!')
        }
        
    print ("Data Found")
    
    # Getting the model from S3
    client = boto3.client('s3')
    result = client.download_file("jogwalk",'jog_walk.h5', "/tmp/model.h5")
    model = load_model("/tmp/model.h5")
    
    # Prediting
    y_pred = model.predict(data)
    y_pred_rounded = (y_pred > 0.5).astype(int)
    
    
    # Class definition
    class_list = ['walk', 'jog']
    
    # Getting the inference
    inference = class_list[y_pred_rounded]
    prediction = y_pred

    # Uploading data
    put_data(Cnt, DeviceID, inference, prob)
    print ("Result added to the table")
    
    # Returning success    
    return {
        'statusCode': 200,
        'body': json.dumps('Successful!')
    }