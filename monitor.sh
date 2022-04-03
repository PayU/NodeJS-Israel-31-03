#!/bin/bash

consumerGroup=$1
echo "running stats for consumer group: $consumerGroup"

while :
do
	info=($(/Users/shaimoria/Desktop/kafka_2.12-2.2.0/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group $consumerGroup))
    clear

    if [[ "$consumerGroup" == "demo_group_1" ]]; then
        echo "Current partition offset: ${info[10]}"
        echo "Current consumer group offset: ${info[11]}"
        echo "Consumer group lag: ${info[12]}"
    elif [[ "$consumerGroup" == "demo_group_3" ]]; then
        echo "partition 1 lag: ${info[12]}"
        echo "partition 2 lag: ${info[20]}"
        echo "partition 3 lag: ${info[28]}"
    else
        echo "consumer group was not found"
    fi

done