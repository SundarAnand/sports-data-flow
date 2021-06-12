exports.handler = (event, context, callback) => {
    // TODO implement
    // Libraries importing
    console.log("Starting...");

    var AWS = require('aws-sdk');
    console.log("AWS SDK Onboarded");
    
    var docClient = new AWS.DynamoDB.DocumentClient();
    console.log("Going to connecet DynamoDB");
    var params = {
        TableName:"ai_result",
        KeyConditionExpression: "DeviceID = :DeviceID",
        ScanIndexForward: false,
        ExpressionAttributeValues: {
        ':DeviceID': "19951234"
        },
        Limit:1
    };
     
     console.log("Just outside scan function");
    //Scaning the entire connected table   
    docClient.query(params, function(err, data) {
        if (err) {
            console.log("Error found - No data found");
            callback(err, null);
        } else {
            callback(null,data["Items"][0]["Inference"] + " with probability of " + Number((data["Items"][0]["Prob"]).toFixed(2)));
        }
    });
};