const createCsvWriter = require('csv-writer').createObjectCsvWriter;
let AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-2'});
const cloudwatchlogs = new AWS.CloudWatchLogs();
let sqs = new AWS.SQS({apiVersion: '2012-11-05'});
let queueURL = "https://sqs.us-east-2.amazonaws.com/955577125609/dev-OH-due-schedules-dead-queue";
const NUM_OF_MSG_IN_SQS = 2;
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
                MaxNumberOfMessages: 2,
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

const getLogStream = async function(startTime, endTime, message){
    let messagBodyObj = JSON.parse(message.Body)
    let payload = '"clientId":"' + messagBodyObj.clientId + '","cid":' + messagBodyObj.cid + ',"bid":' + messagBodyObj.bid + ',"type":"' + messagBodyObj.type + '"'
    let params = {
        startTime: startTime,
        endTime: endTime,
        queryString: "fields @timestamp, @logStream | sort @timestamp asc | filter @message like /" + payload + "/",
        logGroupNames: [
            'dev-OH-TaskConsumer'
        ]
    }
    let queryID = await createQuery(params);
    let logStream = await runQuery(queryID);
    while(logStream.status !== 'Complete'){
        logStream = await runQuery(queryID)
    }
    return logStream.results[0][1].value
}

const queryLogs = async function(SQSmessages) {
    let queryIDs = [];
    for (const message of SQSmessages){
        // skip and delete
        if (processedSQSMessages.includes(message.Body)){
            console.log('Message has already been processed. Skipping...')
            // await deleteSQSMessage(message.ReceiptHandle).then(async (data) => {
            //         deletedSQSMessagescounter++;
            //         console.log('Skipped message is deleted.');
            //     });
            continue;
        }
        processedSQSMessages.push(message.Body);
        
        let endTimeInt = parseInt(message.Attributes.ApproximateFirstReceiveTimestamp) + 60000
        let endTime = endTimeInt.toString();
        let logStream = await getLogStream(message.Attributes.ApproximateFirstReceiveTimestamp, endTime, message)
        console.log("loStream", logStream)
        let params = {
            startTime: message.Attributes.ApproximateFirstReceiveTimestamp,
            endTime: endTime,
            queryString: "fields @timestamp, @message | sort @timestamp asc | filter @message like /ERROR/",
            logGroupNames: [
                'dev-OH-TaskConsumer'
            ]
        }
        let queryID = await createQuery(params);
        queryStartTimes.push(message.Attributes.ApproximateFirstReceiveTimestamp);
        queryEndTimes.push(endTime);
        console.log('Creating query for message with payload ' + message.Body + ' between ' + message.Attributes.ApproximateFirstReceiveTimestamp + ' and ' + endTime + ' Epoch time')
        queryIDs.push(queryID)
        messagePayloads.push(message.Body)
    }
    return queryIDs;
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

const receiveLogs = async function(queryIDs, messages) {
    for(let i = 0; i < queryIDs.length; i++){
        console.log('\nRetrieving query data...')
        queryResult = await runQuery(queryIDs[i]);
        if(typeof(queryResult.results[0]) === "undefined"){
            logsNotFoundCounter++;
            console.log('No log is found. Message is not deleted\n');
            continue;
        } else {
            queryResults.push([queryResult.results[0][0].value, queryResult.results[0][1].value]);
            console.log('Log is retrieved');
            // await deleteSQSMessage(messages[i].ReceiptHandle).then(async (data) => {
            //         deletedSQSMessagescounter++;
            //         console.log('Message is deleted');
            //     });
        }
    }
}

const writeToCSV = (messagePayloads, queryResults) => {
    let outputCSV = [];
        for (let i = 0; i < queryResults.length; i++){
            let output = {};
            output["messagePayload"] = messagePayloads[i];
            output["log"] = queryResults[i][1];
            output["queryStartTime"] = queryStartTimes[i];
            output["queryEndTime"] = queryEndTimes[i];
            outputCSV.push(output)
        }
        csvWriter
            .writeRecords(outputCSV)
            .then(()=> console.log('\nThe CSV file was written successfully'));    
}

let messagePayloads = [];
let queryResults = [];
let msgCounter = 0;
const getAllResults = async function(){
    while (msgCounter < NUM_OF_MSG_IN_SQS){
        console.log('\nStarted retrieving next 10 SQS messages...\n')
        await recieveSQSmessages().then(async (data) => {
            let queryIDs = await queryLogs(data.Messages);
            await receiveLogs(queryIDs, data.Messages)
        })
        
        msgCounter += 10;
    }
    writeToCSV(messagePayloads, queryResults);
    console.log();
    console.log('Not found logs for ' + logsNotFoundCounter + ' SQS Messages')
    console.log(NUM_OF_MSG_IN_SQS + ' SQS Messages are processed');
    console.log(deletedSQSMessagescounter + ' SQS Messages are deleted');
}

getAllResults();



