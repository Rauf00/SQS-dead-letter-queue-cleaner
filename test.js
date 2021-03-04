const createCsvWriter = require('csv-writer').createObjectCsvWriter;
let AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-2'});
const cloudwatchlogs = new AWS.CloudWatchLogs();
let sqs = new AWS.SQS({apiVersion: '2012-11-05'});
let queueURL = "https://sqs.us-east-2.amazonaws.com/955577125609/dev-OH-due-schedules-dead-queue";
const NUM_OF_MSG_IN_SQS = 10;
const csvWriter = createCsvWriter({
    path: 'out.csv',
    header: [
      {id: 'messagePayload', title: 'Message Payload in SQS'},
      {id: 'log', title: 'Log Message in CloudWatch'},
      {id: 'queryStartTime', title: 'Query Start Time'},
      {id: 'queryEndTime', title: 'Query End Time'}
    ]
});

const processedSQSMessages = [];
let deletedSQSMessagescounter = 0;
let logsNotFoundCounter = 0;
const queryStartTimes = [];
const queryEndTimes = []

const recieveSQSmessages = () => {
    return new Promise((resolve, reject) => {
            let params = {
                AttributeNames: [
                    "All"
                ],
                MaxNumberOfMessages: 10,
                QueueUrl: queueURL,
            };
            sqs.receiveMessage(params, async function(err, data) {
                if (err) {
                    console.log(err, err.stack); // an error occurred
                } else {
                    if(data)
                        resolve(data)
                    reject(data)
                }
            });
    });
}

const deleteSQSMessage = (receiptHandle) => {
    return new Promise((resolve, reject) => {
        let params = {
            QueueUrl: queueURL, /* required */
            ReceiptHandle: receiptHandle /* required */
        };
        sqs.deleteMessage(params, function(err, data) {
            if (err){
                console.log(err, err.stack); // an error occurred
            } else {     
                if(data)
                    resolve(data)
                reject(data)
            }
        });
    });
}

const createQuery = async(params) => {
    let returnStatement = new Promise((resolve, reject) => {
        cloudwatchlogs.startQuery(params, function (err, data) {
            if (err) {
                reject(err); // an error occurred
            } else {
                resolve(data)
            }
        });
    });
    return returnStatement
}

const runQuery = async(params) => {
    let returnStatement = new Promise((resolve, reject) => {
        cloudwatchlogs.getQueryResults(params, function (err, data) {
            if (err)
                reject(err); // an error occurred
            else
                resolve(data);
        });
    });
    return returnStatement;
};

const getLogStreamName = async function(startTime, endTime, message){
    console.log('Searching for log stream name in time range: ', startTime, endTime);
    let messagBodyObj = JSON.parse(message.Body)
    let payload = '"clientId":"' + messagBodyObj.clientId + '","cid":"' + messagBodyObj.cid + '","bid":"' + messagBodyObj.bid + '","type":"' + messagBodyObj.type + '"'
    let params = {
        startTime: startTime,
        endTime: endTime,
        queryString: "fields @timestamp, @logStream | sort @timestamp asc | filter @message like /" + payload + "/",
        logGroupNames: [
            'dev-OH-TaskConsumer'
        ]
    }
    console.log('QueryString: ', "fields @timestamp, @logStream | sort @timestamp asc | filter @message like /" + payload + "/")
    let queryID = await createQuery(params);
    let logStream = await runQuery(queryID);
    while(logStream.status !== 'Complete'){
        logStream = await runQuery(queryID)
    }
    if(logStream.results.length === 0){
        console.log("WARNING: Log stream name IS NOT FOUND\n");
        logsNotFoundCounter++;
        return '';
    }
    console.log("Log stream name is retrieved")
    return logStream.results[0][1].value
}

const getLogsFromStream = function(startTime, endTime, logStreamName){
    var params = {
        logGroupName: 'dev-OH-TaskConsumer', /* required */
        logStreamName: logStreamName, /* required */
        endTime: endTime,
        limit: 10,
        startFromHead: true,
        startTime: startTime
      };
      return new Promise((resolve, reject) => {
        cloudwatchlogs.getLogEvents(params, function(err, data) {
            if (err) 
                reject(err) // an error occurred
            else  
                resolve(data.events)           // successful response
          });
      })
      
}

const getErrorLogForSQSmessages = async function(SQSmessages) {
    for (const message of SQSmessages){
        // skip message and delete it
        if (processedSQSMessages.includes(message.Body)){
            console.log('Message has already been processed. Skipping...')
            // await deleteSQSMessage(message.ReceiptHandle).then(async (data) => {
            //         deletedSQSMessagescounter++;
            //         console.log('Skipped message is deleted.');
            //     });
            continue;
        }
        processedSQSMessages.push(message.Body);

        // get @logStream name in the Log Group using the message payload
        let startTime = message.Attributes.ApproximateFirstReceiveTimestamp;
        let endTimeInt = parseInt(startTime) + 60000
        let endTime = endTimeInt.toString();
        let logStreamName = await getLogStreamName(startTime, endTime, message)
        if(logStreamName === ''){
            resultLogs.push('Log stream name is not found')
        } else{
            // get logs from the Log Stream 
            let logsFromStream = await getLogsFromStream(startTime, endTime, logStreamName);
            for(let i = 0; i < logsFromStream.length; i++){
                if(logsFromStream[i].message.includes("INFO com.celayix.consumer.AppServerProxy  - main- requestPayload :" + message.Body.toString())){
                    for(let j = i; j < logsFromStream.length; j++){
                        if(logsFromStream[j].message.includes("ERROR com.celayix.consumer.AppServerProxy  - AppServer call returned error:")){
                            console.log("SUCCESS: ", logsFromStream[j].message)
                            resultLogs.push(logsFromStream[j].message)
                            break;
                        }
                    }
                    break;
                }
            }
            messagePayloads.push(message.Body);
            queryStartTimes.push(startTime);
            queryEndTimes.push(endTime);
            console.log();
        }
    }
}

const writeToCSV = (messagePayloads, resultLogs) => {
    let outputCSV = [];
        for (let i = 0; i < resultLogs.length; i++){
            let output = {};
            output["messagePayload"] = messagePayloads[i];
            output["log"] = resultLogs[i];
            output["queryStartTime"] = queryStartTimes[i];
            output["queryEndTime"] = queryEndTimes[i];
            outputCSV.push(output)
        }
        csvWriter
            .writeRecords(outputCSV)
            .then(()=> console.log('\nThe CSV file was written successfully'));    
}

let messagePayloads = [];
const resultLogs = [];
let msgCounter = 0;
const getAllResults = async function(){
    while (msgCounter < NUM_OF_MSG_IN_SQS){
        console.log('\nStarted retrieving next 10 SQS messages...\n')
        await recieveSQSmessages().then(async (data) => {
            await getErrorLogForSQSmessages(data.Messages);
        })
        
        msgCounter += 10;
    }
    writeToCSV(messagePayloads, resultLogs);
    console.log();
    console.log('Not found logs for ' + logsNotFoundCounter + ' SQS Messages')
    console.log(NUM_OF_MSG_IN_SQS + ' SQS Messages are processed');
    console.log(deletedSQSMessagescounter + ' SQS Messages are deleted');
}

getAllResults();



