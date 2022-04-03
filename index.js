const { setTimeout } = require('timers/promises');
const { Kafka } = require('kafkajs');
const asyncLIB = require('async');

const TOPIC = process.env.TOPIC || 'demo_topic';
const GROUP_ID = process.env.GROUP_ID || 'demo_group_1'
const PARALLEL_WORKERS = 100;
const MAX_PENDING_MESSAGES = 500;
const PENDING_WAITING_TIME_MILLIS = 500;

const kafka = new Kafka({
    clientId: 'demo-app',
    brokers: ['127.0.0.1:9092'],
});

let producer, consumer;
let producerInterval;
let onShutdown = false;

async function createProducer() {
    producer = kafka.producer();
    await producer.connect();
}

async function createConsumer() {
    consumer = kafka.consumer({ groupId: GROUP_ID });
    await consumer.subscribe({ topic: TOPIC });
}
 
function createAsyncQueue() {
    console.log(`creating kafka consumer async queue with ${PARALLEL_WORKERS} parallel workers and ${MAX_PENDING_MESSAGES} max pending messages`);
    asyncQueue = asyncLIB.queue(handleMessage, PARALLEL_WORKERS);
  
    // assign a callback
    asyncQueue.drain(function() {
      console.log('internal queue is empty. all messages has been processed');
    });
}

function setShutdownEvents() {
    ['SIGTERM', 'SIGINT'].forEach((event) => {
        process.on(event, async function() {
            if (onShutdown) {
                return;
            }

            onShutdown = true;
            console.log('shuting down..');

            try {
                clearInterval(producerInterval);
                await producer.disconnect();
                while (asyncQueue.length() > 0 || asyncQueue.running() > 0) {
                    await setTimeout(1000);
                }

                await consumer.pause();
                await consumer.disconnect();
            } catch (err) {
                console.log(err);
            } finally {
                process.exit();
            }
        });
    });
}

async function init() {
    setShutdownEvents();
    createAsyncQueue();
    await createProducer();
    await createConsumer();
}

function parseCommandLineArguments() {
    const argv = process.argv;

    const scenarioNumber = isNaN(argv[2]) ? 1 : Number(argv[2]);
    const producerRate = isNaN(argv[3]) ? 1 : Number(argv[3]);
    const workTimeParams = argv[4].split(":");

    let consumerFixTime = '100', pickInternal = 0, pickTime = '100';

    if (workTimeParams[0] === "fix") {
        consumerFixTime = workTimeParams[1];
    } else if (workTimeParams[0] === 'pick') {
        const pickArguments = workTimeParams[1].split(',');
        consumerFixTime = pickArguments[0];
        pickInternal = Number(pickArguments[1].split('-')[0]);
        pickTime = pickArguments[1].split('-')[1];
    }

    console.log(`running scenario number ${scenarioNumber}`);
    console.log(`producer rate in seconds: ${producerRate}`);
    console.log(`consumer fixed work time: ${Number(consumerFixTime)}`);
    console.log(`consumer pick interval: ${pickInternal}`);
    console.log(`consumer pick time: ${Number(pickTime)}`);
    
    return {
        scenario_number: scenarioNumber,
        producer_rate_in_second: producerRate,
        consumer_fix_time: Number(consumerFixTime),
        consumer_pick_interval: pickInternal,
        consumer_pick_time: Number(pickTime)
    };
}

async function runScenario() {
    const scenario = parseCommandLineArguments();

    switch (scenario.scenario_number) {
        case 1:
            await runEachMessageConsumer(1);
            break;
        case 2:
            await runBatchConsumer(naiveBatchHandling);
            break;
        case 3:
            await runBatchConsumer(asyncBatchHandling);
            break;
        case 4: 
            await runEachMessageConsumer(3);
            break;
    }

    runProducer(scenario.producer_rate_in_second, scenario.consumer_fix_time, scenario.consumer_pick_interval, scenario.consumer_pick_time);
}

function runProducer(messagesPerSecond, fixedWorkTime, pickInternal, pickWorkTime) {
    const interval = 1000 / messagesPerSecond;
    let counter = 0;

    producerInterval = setInterval(async function() {
        counter++;

        if (pickInternal > 0 && counter % pickInternal === 0) {
            workTime = pickWorkTime.toString();
        } else {
            workTime = fixedWorkTime.toString();
        }

        await producer.send({
            topic: TOPIC,
            messages: [ { value: workTime } ],
        });
    }, interval);
}

async function runEachMessageConsumer(partitionsConcurrently) {
    await consumer.run({
        partitionsConsumedConcurrently: partitionsConcurrently,
        eachMessage: async ({ topic, partition, message }) => {
            await handleMessage({message, partition});
        },
    });
}

async function runBatchConsumer(eachBatchHandler) {
    await consumer.run({
        autoCommit: true,
        eachBatchAutoResolve: false,
        eachBatch: eachBatchHandler
    });
}

async function naiveBatchHandling({batch, resolveOffset, heartbeat}) {
    await heartbeat();

    await Promise.all(batch.messages.map(message => handleMessage({message, partition: batch.partition})));

    const lastOffset = Math.max(...batch.messages.map(x => x.offset));
    resolveOffset(lastOffset);
}

async function asyncBatchHandling({batch, resolveOffset, heartbeat}) {
    if (asyncQueue.length() > MAX_PENDING_MESSAGES) {
        console.log(`internal message queue reached the maximum pending message (${MAX_PENDING_MESSAGES}). goes to sleep for ${PENDING_WAITING_TIME_MILLIS} milliseconds..`);
        await heartbeat();
        await setTimeout(PENDING_WAITING_TIME_MILLIS);
        return;
    }

    for (let message of batch.messages) {
        resolveOffset(message.offset);
        const queueMessage = {
            message: message,
            partition: batch.partition
        };

        asyncQueue.push(queueMessage);
    }

    await heartbeat();
}

async function handleMessage({ message, partition }) {
    const workTime = message.value.toString();

    console.log({
        partition: partition,
        work_time: workTime
    });

    await doWork(workTime);
}

async function doWork(ms) {
    console.log(`working for ${ms} milliseconds..`);
    await setTimeout(ms);
}

init()
    .then(runScenario)
    .catch((err) => {
        console.log(err);
        process.exitCode = 1;
    });