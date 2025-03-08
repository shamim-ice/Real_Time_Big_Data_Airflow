# Install & Set Up Apache Kafka on Ubuntu

## Install JAVA
```sh
sudo apt install openjdk-11-jdk -y
```
### Set JAVA_HOME
```sh
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```
## Install Hadoop Dependencies
```sh
sudo apt install wget curl -y
```
## Download  & Install Apache Spark
```sh
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xvzf spark-3.5.0-bin-hadoop3.tgz
```

### Make sure that SPARK_HOME and PYSPARK_PYTHON are correctly set.
```sh
echo 'export SPARK_HOME=~/spark' >> ~/.bashrc
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
echo 'export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH' >> ~/.bashrc
echo 'export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH' >> ~/.bashrc
source ~/.bashrc
```

## Download Kafka
```sh
wget https://dlcdn.apache.org/kafka/3.9.0/kafka_2.12-3.9.0.tgz
```
## Extract & Move kafka

```sh
tar -xvzf kafka_2.12-3.9.0.tgz
sudo mv kafka_2.12-3.9.0 /usr/local/kafka
cd /usr/local/kafka
```

## Start Zookeeper & Kafka

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

## Download kafka conneector
for spark 3.5.0
```sh
wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.0/spark-sql-kafka-0-10_2.13-3.5.0.jar
mv spark-sql-kafka-0-10_2.13-3.4.0.jar $SPARK_HOME/jars/
```
## Ensure the Correct Scala Version is Installed
for spark 3.5.0 install scala-2.13.8
```sh
wget https://downloads.lightbend.com/scala/2.13.8/scala-2.13.8.tgz
tar -xvf scala-2.13.8.tgz
sudo mv scala-2.13.8 /usr/local/scala
echo 'export PATH=/usr/local/scala/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

## Create & Test ```sensor_data``` Topic
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
{"id": 1, "date": "2024/01/18", "temperature": 24.5, "humidity": 60}
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
{"id": 1, "date": "2024/01/18", "temperature": 24.5, "humidity": 60}
```

# Add PostgreSQL JDBC Driver to Spark
```sh
wget https://jdbc.postgresql.org/download/postgresql-42.3.8.jar -P /home/shamim/spark/jars/
```