exports.handler = (event, context, callback) => {
    // TODO implement
    // Libraries importing
    console.log(JSON.parse(JSON.stringify(event)));
    var AWS = require('aws-sdk');
    var dynamo = new AWS.DynamoDB.DocumentClient();
    var table = "AthleTechData";
    var obj = JSON.parse(JSON.stringify(event));
    var params = {
    TableName:table,
    Item:{
        "Count": obj.Count,
        "DeviceID": obj.DeviceID.toString(),
        "a_x": obj.acc_x,
        "a_y": obj.acc_y,
        "a_z": obj.acc_z,
        "g_x": obj.gyr_x,
        "g_y": obj.gyr_y,
        "g_z": obj.gyr_z,
        "m_x": obj.mag_x,
        "m_y": obj.mag_y,
        "m_z": obj.mag_z
        }
    };

    dynamo.put(params, function(err, data) {
        if (err) {
            console.error("Unable to add device. Error JSON:", JSON.stringify(err, null, 2));
            context.fail();
        } else {
            console.log("Successful");
            context.succeed();
        }
    });
};