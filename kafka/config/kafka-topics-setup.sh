#!/bin/bash

# Create Kafka topics
kafka-topics.sh --create --topic views --partitions 3 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
kafka-topics.sh --create --topic clicks --partitions 3 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
