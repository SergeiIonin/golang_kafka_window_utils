#!/bin/bash

# This script clears the topics in the kafka cluster
/Users/sergey.ionin/kafka_2.13-3.1.0/bin/kafka-topics.sh --delete --bootstrap-server localhost:29092 --topic timewindows
/Users/sergey.ionin/kafka_2.13-3.1.0/bin/kafka-topics.sh --delete --bootstrap-server localhost:29092 --topic timewindows_aggregated

# /Users/sergey.ionin/kafka_2.13-3.1.0/bin/kafka-topics.sh --list --bootstrap-server localhost:29092
