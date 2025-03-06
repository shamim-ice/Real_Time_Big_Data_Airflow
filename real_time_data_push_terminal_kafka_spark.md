# Install & Set Up Apache Kafka on Ubuntu
### Download Kafka
```sh
wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.12-3.9.0.tgz
```
### Extract & Move kafka

```sh
tar -xvzf kafka_2.12-3.9.0.tgz
sudo mv kafka_2.12-3.9.0 /usr/local/kafka
cd /usr/local/kafka
```

# Start Zookeeper & Kafka

### Start Zookeeper
```sh
cd /usr/local/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```
### Start Kafka
Open a new terminal, go to kafka's directory then start kafka
```sh
cd /usr/local/kafka
bin/kafka-server-start.sh config/server.properties
```
now kafka is running!

# Create & Test ```sensor_data``` Topic
### Create Topic
Open a new terminal, go to kafka's directory then create topic
```sh
bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Produce kafka test data
```sh
bin/kakfa-console-producer.sh --topic sensor_data --bootstrap-server localhost:9092
```
Type:
```json
{"id": 1, "temperature": 24.5, "humidity": 60}
```
Press ENTER.

### Consume kafka test data

Open a new terminal and run:
```sh
cd /usr/local/kafka
bin/kafka-console-consumer.sh --topic sensor_data --bootstrap-server localhost:9092
```
You shouls SEE:

```json
{"id": 1, "temperature": 24.5, "humidity": 60}
```
